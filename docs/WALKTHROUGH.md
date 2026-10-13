# 代码导读（中文）

这份文档按"数据从哪里来、经过哪些类、最后落到哪里"的顺序，把每个 Java 类讲一遍。目标是：面试时能用自己的话把整条链路讲清楚，并回答"为什么这样设计"。

## 0. 一句话版本

把 NASA 的电池放电实验数据当成 BMS（电池管理系统）实时上报的遥测，用 **Replayer** 按事件时间顺序写进 **Kafka（Redpanda）**；**Flink** 作业按电芯分组，做数据质量检查、120 秒滚动窗口和"每个放电循环一个会话窗口"的计算，产生告警；原始数据、循环汇总和告警写进 **ClickHouse**；**Spring Boot** 提供只读 REST 接口。

```
CSV ──Replayer──▶ Kafka topic bms.telemetry (4 分区, key = 电芯ID)
                     │
                     ▼
         Flink 作业（同一个 JVM 里的 MiniCluster）
   水位线 → 丢心跳 → 数据质量闸门 ─┬─▶ 120s 滚动窗口 → 过温 / 电压骤降
                                   ├─▶ 每循环会话窗口 → 循环汇总 + 缺数告警 → 容量衰减
                                   └─▶ 通过的原始样本
                     │
                     ▼
     ClickHouse: telemetry / cycles / alerts（ReplacingMergeTree）
                     │
                     ▼
          Spring Boot 只读 API（/api/v1/...）
```

## 1. 数据准备（Python，不在 Java 里）

- `scripts/convert_nasa.py`：读 B0005/6/7/18 四个 `.mat` 文件，只取放电（discharge）循环，每个采样点输出一行：`event_time_ms, cell_id, cycle, seq, voltage_v, current_a, temperature_c`。
  - **事件时间** = 循环开始时间（文件里的 MATLAB 日期向量）+ 循环内的 `Time` 秒数。原数据没写时区，我们统一当 UTC，保证可复现。
  - 同时输出 `reference_capacity.csv`：NASA 自己算好的每循环容量，用来**核对**流式计算出的容量对不对。
  - 原始数据和转换结果都放在仓库外（`D:\data\bsa\`），不提交：NASA 目录页没有写许可证。
- `scripts/make_synthetic_fixture.py`：生成测试用的小合成数据（2 个电芯 × 8 个循环 × 45 个点），每种告警都是**故意设计出来**的，所以测试能断言精确数量。

## 2. bsa-core：纯 Java 的模型和规则（不依赖 Flink）

为什么单独一个模块：规则逻辑和"流处理框架"分开，规则就能用普通 JUnit 测、用 PIT 做变异测试；Flink 那层只是很薄的包装。

### 模型（`bsa.core.model`）
| 类 | 作用 |
|---|---|
| `Telemetry` | 一条遥测。公有字段 + 无参构造器 = Flink 认得的 POJO，序列化快（不走 Kryo）。`kind=heartbeat` 是心跳，不带测量值，只用来推动事件时间。`sampleKey()` = `cycle:seq`，判断重复用。 |
| `Alert` | 一条告警。**`alertId` 是确定性的**：`UUID.nameUUIDFromBytes(电芯|类型|区分符)`，区分符是窗口起点 / 循环号 / 样本键。同样的输入重算一遍，id 不变——这是幂等写入的基础。 |
| `AlertType` | 8 种告警：`OVER_TEMPERATURE`、`VOLTAGE_SAG`、`CAPACITY_DROP`、`CAPACITY_EOL`，以及数据质量类 `DQ_GAP`、`DQ_OUT_OF_RANGE`、`DQ_DUPLICATE`、`DQ_LATE`。 |
| `CycleSummary` | 一个放电循环结束后的汇总：样本数、缺失数、积分容量、最低电压、最高温度、SOH 等。 |

### 规则（`bsa.core.rules`）
| 类 | 做什么 | 面试时可以讲的点 |
|---|---|---|
| `Thresholds` | 所有阈值集中在一处，可用环境变量覆盖；`validate()` 检查自相矛盾的配置（比如会话间隔必须大于最大采样间隔，否则一个缺口就会把循环切成两半）。 | 阈值怎么来的：物理范围是宽松的"传感器坏了"判据；40°C 过温是为 24°C 实验室数据设的演示值（按电芯规格书 60°C 的话这批数据一个都不会触发），README 里写明了。 |
| `DataQualityGate` | 对每个样本判定：越界？迟到？重复？顺序固定：先越界，再迟到，最后重复。 | "迟到"的定义来自 Flink 的水位线约定：水位线 W 表示"时间戳 ≤ W 的数据不会再来"，所以 `eventTime <= W` 就是迟到。 |
| `WindowStats` | 窗口的增量累加器：不存样本，只存计数和最小二乘所需的几个和（Σx, Σy, Σx², Σxy），结束时算 dV/dt（伏/分钟）。只有"带载"（电流 < -0.5A）的点进回归。 | 为什么增量聚合：每个窗口只保留几个 double，状态小；`merge()` 满足 Flink AggregateFunction 合约。 |
| `WindowRules` | 窗口关闭后：最高温 > 阈值 → 过温；带载点 ≥5 且斜率 < -0.3 V/min → 电压骤降。 | 一个窗口一种告警最多一条，天然去抖。 |
| `CycleAnalyzer` | 一个循环的全部样本到齐后：按 seq 排序、梯形法积分放电电流得到容量（Ah）、检测缺数（seq 跳号或时间间隔 > 60s）。 | 排序后再算，所以和到达顺序无关；尾部缺失检测不到（不知道本来有几个点），README 写了。 |
| `FadeTracker` | 每个电芯一份历史：前 3 个循环的平均值 = 初始容量；前 5 个循环的滚动平均 = 基线。低于基线 3% → `CAPACITY_DROP`；SOH 首次 < 0.8 → `CAPACITY_EOL`（只报一次）。 | 太短的循环（<20 点）不参与，防止一个残缺循环污染基线。 |

### 编解码（`bsa.core.codec`）
- `TelemetryCodec`：Kafka 上的 JSON 格式（snake_case），生产者和消费者共用一份，字段名不会对不上。解析失败返回 null，由作业计数后丢弃。
- `TelemetryCsv`：读转换后的 CSV。

## 3. bsa-replayer：把文件"播放"成实时流

| 类 | 作用 |
|---|---|
| `ReplayPlan` | **纯函数**，不碰 Kafka 也不碰时钟：给每个样本一个"投递时间 = 事件时间 + 延迟"，按投递时间稳定排序。可选的故障注入（用固定随机种子，同种子同结果）：重复（1 秒后再发一份）、乱序（晚 5 秒，在 20 秒容忍范围内，**不应**告警）、迟到（晚 30 分钟）、越界（电压改成 9.99V）、丢弃。每个（电芯，循环）最多一个故障，且避开循环首尾，这样每个故障都对应一个可检测的结果。`expectedDqAlerts()` 算出作业**应该**报多少条，用来和实际结果对账。还会每 60 秒（事件时间）插一条心跳，结尾再发一条"一小时后"的心跳把所有窗口关掉。 |
| `Replayer` | 读 CSV → 建计划 → 建 topic（4 分区）→ 用 Kafka 生产者发送。key = 电芯 ID，所以同一电芯的数据进同一分区、保持顺序；心跳显式发到每个分区。开启**幂等生产者**（`enable.idempotence=true`）：网络重试不会在 Kafka 里产生重复。`Pacer` 负责控速：`--speedup 5000` 表示 1 秒墙钟 = 5000 秒事件时间，循环之间几小时的空闲被截断为最多 20ms。发送时写入 `ingest_ms`（墙钟），后面算延迟用。 |

为什么需要心跳：Flink 的水位线是"所有输入分区的最小值"。如果某个分区一直没数据（比如 B0018 的实验比另外三个晚了三个月），水位线就卡住，窗口永远不触发。心跳让每个分区的事件时间都往前走。

## 4. bsa-flink：流处理作业

| 类 | 作用 |
|---|---|
| `StreamingJob` | 入口。从环境变量读配置；开启 checkpoint（5 秒一次，hashmap 状态后端，保存到 `/checkpoints` 卷，取消时保留）；固定延迟重启策略；建 KafkaSource（checkpoint 时提交 offset）；调用 `AlertPipeline.build`；接三个 ClickHouse sink。**在同一个 JVM 里用本地 MiniCluster 运行**，省掉单独的 JobManager/TaskManager 容器。启动时如果 `/checkpoints` 下有完整的 checkpoint，就从它恢复（见 `CheckpointLocator`）。 |
| `TelemetryWatermarks` | 水位线 = 已见最大事件时间 − 20 秒 − 1ms。每条记录都可能推进水位线（punctuated），这样行为只取决于记录顺序，测试可复现。Kafka 源额外开启**水位线对齐**（watermark alignment）：某个分区跑得比最慢分区快超过 600 秒（事件时间），就先暂停它，避免分区间消费速度差太多导致"迟到"判定随调度抖动。注入的"迟到"是 30 分钟，远大于 600 秒漂移 + 20 秒容忍 + 60 秒心跳间隔，所以判定稳定。第一次跑 demo 时漂移设 60 秒、更新间隔 200 毫秒，作业被对齐机制卡到每秒约 130 条（回放每秒发 2200 条）——这是对齐的代价：每个更新周期事件时间最多前进一个"最大漂移"。现在是 600 秒 / 50 毫秒。 |
| `TelemetryDeserializer` | Kafka 字节 → `Telemetry`，坏消息计数（`malformedRecords` 指标）后跳过。 |
| `AlertPipeline` | 整个拓扑，和数据源/输出解耦（生产用 Kafka/ClickHouse，测试用内存列表）。步骤：去掉心跳 → 按电芯 keyBy 进 `DataQualityFunction` → 两路：① 按电芯 120s 滚动窗口（增量聚合 + 窗口函数）→ 过温/骤降；② 按"电芯#循环"会话窗口（5 分钟无数据即关闭）→ 循环汇总 + 缺数告警 → 按电芯 keyBy 进容量衰减。所有告警 union 成一条流。窗口的 `sideOutputLateData` 用来兜底统计"闸门放过但窗口仍嫌迟"的样本（理论上应为 0，实测也是 0）。 |
| `DataQualityFunction` | 按电芯的 `KeyedProcessFunction`。用 `MapState<样本键, 事件时间>` 记住见过的样本，判断重复；通过 `DataQualityGate` 判定，拒绝的样本变成 DQ 告警走侧输出。清理：按分钟分桶注册事件时间定时器，水位线越过"样本时间 + 10 分钟"就忘掉它（此后同一样本再来必然先被判"迟到"）。这份状态在 checkpoint 里，故障恢复后去重仍然有效（有测试）。 |
| `WindowFunctions.StatsAggregate` / `WindowAlerts` | 滚动窗口的增量聚合 + 窗口关闭时调用 `WindowRules`。 |
| `WindowFunctions.CycleWindow` | 会话窗口关闭时调用 `CycleAnalyzer`，循环汇总走主输出，缺数告警走侧输出。 |
| `WindowFunctions.CapacityFade` | **一个真实踩过的坑**：不同并行子任务发来的循环汇总，在网络上**不保证按事件时间有序**（第 6 循环可能比第 5 循环先到），只有水位线是跨通道协调的。第一版直接处理，MiniCluster 测试就失败了（基线算错、EOL 报在错误的循环）。修法：先把汇总存进 `MapState<时间戳, 列表>`，注册该时间戳的事件时间定时器；水位线越过它时，所有更早的汇总一定都到了，再按时间顺序交给 `FadeTracker`。 |
| `CheckpointLocator` | 在 checkpoint 目录里找最新的 `chk-N/_metadata`。原因：MiniCluster 内部的任务失败，Flink 自己会从内存里的最新 checkpoint 恢复；但整个容器被杀掉后，内存没了，只剩磁盘上的保留 checkpoint，启动时把它作为恢复路径传进去（真实集群里这件事由 JobManager 高可用来做）。 |
| `sink.ClickHouseSink` / `ClickHouseWriter` | 自己写的 Sink V2：攒批，满 5000 行、或 1 秒、或 **Flink 做 checkpoint 前调用 `flush()`** 时，用 HTTP `INSERT ... FORMAT JSONEachRow` 写入。5xx/网络错误指数退避重试，4xx（权限、SQL 错）不重试；重试用完就抛异常让任务失败 → Flink 从上个 checkpoint 重来。 |
| `sink.Rows` | 对象 → ClickHouse 列名映射；NaN 转成 NULL（JSON 没有 NaN）。 |

### 交付语义怎么讲（面试高频）
- **Flink 内部状态**：checkpoint 用 EXACTLY_ONCE 屏障对齐，状态是精确一次的。
- **写 ClickHouse**：**至少一次**。checkpoint 前会 flush，所以 checkpoint 之前的数据一定落库；失败后从 checkpoint 重放，checkpoint 之后已经写过的行会再写一遍。
- **为什么结果仍然是"效果上精确一次"**：每行都有确定性主键（样本 = 电芯+循环+seq；告警 = 确定性 alertId），表引擎是 `ReplacingMergeTree`，相同排序键的行在后台合并时去重；查询加 `FINAL` 在读的时候就去重。`demo.sh crash`（中途 kill -9 作业容器）和 `demo.sh reprocess`（整个 topic 重算一遍、每行写两次）两个实验验证了这一点，数字在 README。
- 为什么不做两阶段提交的真·精确一次：ClickHouse 没有跨批次事务；幂等键 + 去重引擎更简单，代价是读时要 `FINAL`（数据量大时改用预聚合或 `argMax`）。

## 5. ClickHouse 表（`clickhouse/init/01-schema.sh`）

- `telemetry`：`ORDER BY (cell_id, event_time, cycle, seq)`，按月分区。按电芯 + 时间范围查最快。
- `alerts`：`ORDER BY (cell_id, alert_type, event_time, alert_id)`，按月分区。
- `cycles`：`ORDER BY (cell_id, cycle)`，表很小不分区。
- 三张表都是 `ReplacingMergeTree(inserted_at)`：同键保留最新一行。
- 两个最小权限用户：`bsa_writer` 只能 INSERT（给 Flink），`bsa_reader` 只能 SELECT 且 readonly（给 API）。密码来自环境变量，SQL 文件里没有密码。冒烟脚本验证了 reader 写入会被拒绝。

## 6. bsa-api：Spring Boot 只读接口

| 类 | 作用 |
|---|---|
| `ApiApplication` | 启动类 + OpenAPI 元信息（文档在 `/v3/api-docs`，界面在 `/swagger-ui.html`）。 |
| `BatteryController` | `/api/v1/cells`、`/cells/{id}`、`/cells/{id}/alerts`、`/cells/{id}/cycles`、`/alerts?since=`、`/alerts/summary`。**先校验再查库**：电芯 ID 白名单正则 `^[A-Za-z0-9_-]{1,32}$`，告警类型必须是枚举值，`limit` 1–1000，时间必须是 ISO-8601。健康检查用 actuator `/actuator/health`（含数据库探活）。 |
| `BatteryRepository` | 所有 SQL 在这里；用户输入**全部是命名绑定参数**（`:cellId`、`:sinceMs`），不拼接字符串。查询带 `FINAL`（原因见上）。 |
| `ApiExceptionHandler` | 错误统一成 RFC 9457 `application/problem+json`；数据库异常返回 503 + 固定文字，不泄露 SQL、主机名和堆栈。 |
| `Dtos` | 返回结构（Java record）。 |

## 7. 测试分层

| 层 | 文件 | 测什么 |
|---|---|---|
| 规则单元测试 | `bsa-core/src/test/...` | 每条规则的边界（等于阈值不报、超过才报）、梯形积分、乱序输入、环形缓冲。PIT 变异测试只针对这一层：第一次 87.6%，按幸存变异补了 `MutationGapTest` 后 94.0%，剩下 14 个逐条说明在 `results/pit-summary.md`。 |
| 算子测试 | `DataQualityFunctionTest` | 用 Flink 测试 harness 手动推水位线：迟到边界、定时器清理状态、**状态快照后恢复，去重仍有效**。 |
| 顺序测试 | `CapacityFadeOrderTest` | 6 个循环汇总乱序到达、一个水位线同时越过它们：输出必须按循环顺序，基线才对。 |
| 拓扑测试 | `PipelineMiniClusterTest` | 真 MiniCluster、并行度 2：① 干净数据的流式结果 == 同一套规则的批处理结果（"oracle"）；② 注入 3 重复 + 3 界内乱序 + 2 迟到 + 2 越界 + 2 心跳，DQ 告警数量精确吻合，其余告警等于"去掉被丢样本后"的批处理结果；③ 同输入跑两次 alertId 完全一致。 |
| Sink 测试 | `ClickHouseWriterTest` | 用 JDK 自带 HttpServer 假扮 ClickHouse：按批发送、checkpoint flush、5xx 重试、4xx 不重试、失败时保留缓冲。 |
| API 切片测试 | `BatteryControllerTest`、`OpenApiDocTest` | 校验、404/400/503、参数透传、OpenAPI 文档内容。 |
| 端到端 | `scripts/demo.sh` + `scripts/api-smoke.sh` | 真 Redpanda + ClickHouse + 作业 + API，真实 NASA 回放；数字写进 `results/`，README 里的数字由 `ReadmeNumbersTest` 钉住。 |
| 手工变异 | `scripts/hand-mutations.sh` | 20 处手工"弄坏"（包括 Flink 接线、sink、API 校验），第一轮抓到 18 个。漏掉的两个：回放器不重排序是真漏洞，已补测试；容量衰减"倒序释放"是等价变异——Flink 按时间戳顺序触发事件时间定时器，每次 `onTimer` 只会有一个到期时间戳。另外第一次跑这个脚本时踩了个坑：`mv` 还原文件会保留旧的修改时间，Maven 增量编译就不重新编译，后面所有结果都被前一个变异污染；现在还原后会 `touch`。 |

## 8. 可能被问到的问题

- **为什么用 Flink 不用 Kafka Streams / Spark？** 需要事件时间、会话窗口、按键状态 + 定时器、checkpoint 恢复，这些是 Flink 的强项；Kafka Streams 也能做，但会话窗口 + 跨通道重排序的写法没这么直接；Spark Structured Streaming 是微批，延迟和逐条状态控制不如 Flink。
- **水位线设多少？** 20 秒。NASA 采样间隔约 9.4–20 秒，注入的界内乱序是 5 秒。越大越能容忍乱序，但窗口结果出得越晚。
- **为什么不用 allowedLateness？** 迟到数据在闸门就被判定并告警，不进窗口；窗口结果一旦写库就不再修改，下游逻辑简单。代价是迟到超过 20 秒的样本会丢（并报 `DQ_LATE` + 留下 `DQ_GAP`）。
- **单机 MiniCluster 和真集群差在哪？** API、算子、状态、checkpoint 一样；差在没有独立的 JobManager 高可用、TaskManager 不能横向扩、Web UI 没开。换成集群只需要把同一个 jar 提交上去（Kafka/ClickHouse 依赖打在 jar 里）。
- **局限**：单节点；回放的是实验室数据，不是真 BMS；阈值是演示值；`FINAL` 在大表上慢；尾部缺数检测不到；容量是积分全部放电电流，和 NASA 的口径有小差异（见 README 的对账数字）。
