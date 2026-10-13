#!/usr/bin/env bash
# Run Maven inside the official maven:3.9-eclipse-temurin-17 image, so no local JDK/Maven is needed.
# Usage: scripts/mvn-docker.sh -B verify
#        scripts/mvn-docker.sh -B -pl bsa-core org.pitest:pitest-maven:mutationCoverage
# The local repository is cached in the Docker volume bsa-m2.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
exec docker run --rm --name "bsa-mvn-$$" --memory 2g --cpus 4 \
  -v bsa-m2:/root/.m2 -v "$REPO":/src -w /src \
  maven:3.9-eclipse-temurin-17 mvn "$@"
