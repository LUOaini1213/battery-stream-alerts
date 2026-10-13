# Builds everything from source: `docker compose up --build` needs no local JDK or Maven.
# Works with the classic builder and BuildKit (no BuildKit-only syntax).

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml ./
COPY bsa-core ./bsa-core
COPY bsa-replayer ./bsa-replayer
COPY bsa-flink ./bsa-flink
COPY bsa-api ./bsa-api
RUN mvn -B -q -DskipTests package \
 && mkdir -p /out \
 && cp bsa-flink/target/bsa-flink-job.jar bsa-replayer/target/bsa-replayer.jar bsa-api/target/bsa-api.jar /out/

FROM eclipse-temurin:17-jre AS runtime-base
RUN useradd --system --uid 10001 --create-home app \
 && mkdir -p /checkpoints /results \
 && chown app /checkpoints /results
WORKDIR /app

FROM runtime-base AS flink-job
COPY --from=build /out/bsa-flink-job.jar /app/job.jar
USER app
# --add-opens: Flink's serializers need reflective access on JDK 17.
ENV JAVA_OPTS="-Xmx448m -XX:MaxMetaspaceSize=160m -XX:MaxDirectMemorySize=160m"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED -jar /app/job.jar"]

FROM runtime-base AS replayer
COPY --from=build /out/bsa-replayer.jar /app/replayer.jar
USER app
ENV JAVA_OPTS="-Xmx224m"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/replayer.jar \"$@\"", "replayer"]

FROM runtime-base AS api
COPY --from=build /out/bsa-api.jar /app/api.jar
USER app
ENV JAVA_OPTS="-Xmx224m -XX:MaxMetaspaceSize=128m"
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=30 CMD curl -fsS http://localhost:8080/actuator/health || exit 1
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/api.jar"]
