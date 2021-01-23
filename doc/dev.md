### Development Environment

Spark-Redis is built using [Apache Maven](https://maven.apache.org/) and a helper [GNU Make](https://www.gnu.org/software/make/) file.
Maven is used to build a jar file and run tests. Makefile is used to start and stop redis instances required for integration tests.

The `Makefile` expects that Redis binaries (`redis-server` and`redis-cli`) are in your `PATH` environment variable.

To build Spark-Redis and run tests, run:

```
make package
```

To run tests:

```
make test
```

If you would like to run tests from your IDE, you have to start Redis test instances with `make start` before that. To stop test
instances, run `make stop`.

To build Spark-Redis skipping tests, run:

```
mvn clean package -DskipTests
```

