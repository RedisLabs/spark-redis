### Development Environment

Spark-Redis is built using [Apache Maven](https://maven.apache.org/) and a helper [GNU Make](https://www.gnu.org/software/make/) file.
Maven is used to build a jar file and run tests. Makefile is used to start and stop redis instances required for integration tests.

Build with JDK 8, the version the CI uses. Building with JDK 11 or newer is known to fail with the Scala and plugin versions
the project uses (for example with `NoClassDefFoundError: javax/tools/ToolProvider`), so if you have several JDKs installed,
point `JAVA_HOME` to a JDK 8 and check the Java version reported by `mvn -version`.

The `Makefile` expects that Redis binaries (`redis-server` and`redis-cli`) are in your `PATH` environment variable.
The tests also connect over TLS, so Redis must be built with TLS support (`make BUILD_TLS=yes`), as in the
[CI workflow](../.github/workflows/integration.yml).

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

