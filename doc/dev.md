### Development Environment

Spark-Redis is built with [Apache Maven](https://maven.apache.org/). A small [GNU Make](https://www.gnu.org/software/make/)
file wraps the common commands.

Build with JDK 8, the version the CI uses. Building with JDK 11 or newer is known to fail with the Scala and plugin versions
the project uses (for example with `NoClassDefFoundError: javax/tools/ToolProvider`), so if you have several JDKs installed,
point `JAVA_HOME` to a JDK 8 and check the Java version reported by `mvn -version`.

You need:

- **A JDK 8** for this 3.x build, with `JAVA_HOME` pointing at it.
- **Docker.** The tests start their own Redis servers in containers through Testcontainers, on free ports chosen at
  run time, so no local `redis-server` is needed, and several test runs can happen at once.

To run every test against the Redis version set by `redis.image` in `pom.xml`:

```
make test
```

To pick another Redis version, or to run only some suites:

```
make test REDIS_IMAGE=redis:8.6
make test SUITES=com.redislabs.provider.redis.df.standalone.HashDataframeStandaloneSuite
```

Running tests from your IDE works the same way: the first suite that needs Redis starts it. To use Redis servers you
started yourself instead, on the old fixed ports (6379 standalone, 6380 TLS, 7379-7381 cluster), pass
`-Dredis.containers=false`.

To build Spark-Redis without running tests:

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

