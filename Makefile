# Thin wrapper around Maven. The tests start their own Redis servers in Docker with Testcontainers (see
# src/test/scala/com/redislabs/provider/redis/env/RedisContainers.scala), so no local redis-server is needed.
#
#   make test                                  # every suite, against the pom's redis.image
#   make test REDIS_IMAGE=redis:7.4            # another Redis version
#   make test SUITES=com.example.FooSuite      # one suite, or a comma-separated list
#   make test MVN_ARGS="-Dspark.version=3.3.3 -Dscala.complete.version=2.12.15"   # another Spark version
#   make benchmark                             # the benchmark suites (long)
#
# Requirements: Docker, and JAVA_HOME pointing at a JDK 8.

MVN ?= mvn
REDIS_IMAGE ?=
SUITES ?=
MVN_ARGS ?=

MAVEN_ARGS = -B $(if $(REDIS_IMAGE),-Dredis.image=$(REDIS_IMAGE)) $(if $(SUITES),-Dsuites=$(SUITES)) $(MVN_ARGS)

.PHONY: test benchmark help

# 'clean' so that classes compiled for another Scala version (dev/change-scala-version.sh) are not reused.
test:
	$(MVN) $(MAVEN_ARGS) clean verify

benchmark:
	$(MVN) $(MAVEN_ARGS) -Pbenchmark clean verify

help:
	@sed -n '1,10p' $(firstword $(MAKEFILE_LIST))
