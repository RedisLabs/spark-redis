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

### Scala and Spark versions

The build defaults to Scala 2.12 and the latest Spark 3.x (see `pom.xml`). To build and test for Scala 2.13, switch the
build with the script below. It changes the artifact name (`spark-redis_2.13`) and the Scala version in `pom.xml`, and
`dev/change-scala-version.sh 2.12` switches it back:

```
dev/change-scala-version.sh 2.13
make test
```

To test against another Spark version, pass it to Maven together with the Scala version that Spark release is built with
(listed in the [CI workflow](../.github/workflows/integration.yml)):

```
make test MVN_ARGS="-Dspark.version=3.3.3 -Dscala.complete.version=2.12.15"
```

Code that is shared by both Scala versions must compile with both: use `scala.collection.JavaConverters._` with explicit
`.asScala` / `.asJava` conversions (`scala.collection.JavaConversions` doesn't exist in Scala 2.13), and convert Java
collections with `.asScala.toSeq` where a `Seq` is expected.

