# Thin wrappers. Every target works without a local JDK: Maven runs in the official maven image.
MVN := bash scripts/mvn-docker.sh

.PHONY: test package pit mutations up demo demo-faults demo-crash demo-reprocess down

test:            ## unit + MiniCluster + Spring slice tests
	$(MVN) -B verify

package:         ## build the three runnable jars (target/)
	$(MVN) -B -DskipTests package

pit:             ## PIT mutation testing of the alert rules (bsa-core)
	$(MVN) -B -pl bsa-core org.pitest:pitest-maven:mutationCoverage

mutations:       ## the 20 hand-made mutations (results/hand-mutations.md)
	docker run --rm --name bsa-mutations --memory 2g -v bsa-m2:/root/.m2 -v "$(CURDIR)":/src -w /src \
	  maven:3.9-eclipse-temurin-17 bash scripts/hand-mutations.sh

up:              ## broker + ClickHouse + Flink job + API
	docker compose up -d --build --wait redpanda clickhouse flink-job api

demo:            ## full replay of the NASA data, prints alert counts
	bash scripts/demo.sh clean

demo-faults:
	bash scripts/demo.sh faults

demo-crash:
	bash scripts/demo.sh crash

demo-reprocess:
	bash scripts/demo.sh reprocess

down:            ## stop and delete containers, volumes and the network
	docker compose --profile replay down -v --remove-orphans
