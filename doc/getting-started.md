## Getting the library

The artifact name ends with the Scala version, and it must match the Scala version of your Spark distribution. Like Spark
3.x, Spark-Redis is built for two Scala versions:

| Your Spark distribution | Artifact |
|---|---|
| Scala 2.12, the default Spark 3.x download ("Pre-built for Apache Hadoop 3.3 and later") | `spark-redis_2.12` |
| Scala 2.13 ("Pre-built for Apache Hadoop 3.3 and later (Scala 2.13)") | `spark-redis_2.13` |

`spark-shell` prints the Scala version of your Spark at startup ("Using Scala version 2.12.18"). A mismatch fails at
runtime with errors such as `java.lang.NoSuchMethodError: scala.Predef$.refArrayOps`. Release 3.1.0 is available for
Scala 2.12 only; `spark-redis_2.13` starts with 3.1.1. See the
[version compatibility table](../README.md#version-compatibility-and-branching) for the Spark version each release is
built for.

The examples below use `spark-redis_2.12`; replace it with `spark-redis_2.13` for a Scala 2.13 Spark. With SBT, `%%` picks
the artifact that matches your project's Scala version.

### Maven

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>spark-redis_2.12</artifactId>
      <version>3.1.0</version>
    </dependency>
  </dependencies>
```

### SBT

```scala
libraryDependencies += "com.redislabs" %% "spark-redis" % "3.1.0"
```

### GitHub releases

From 3.1.1 on, each [GitHub release](https://github.com/RedisLabs/spark-redis/releases) has the jars attached, for Scala
2.12 and 2.13: the library, its sources and javadoc, and a `jar-with-dependencies` that also contains the library's
dependencies. `SHA256SUMS` lists their checksums. 3.1.1 is published only there, not on Maven Central, so the Maven,
SBT and `--packages` examples on this page use 3.1.0. To use 3.1.1, download its `jar-with-dependencies` and pass it
with `--jars`, as shown below.

### Build from source
You can download the library's source and build it. The build requires JDK 8 and Maven:
```
git clone https://github.com/RedisLabs/spark-redis.git
cd spark-redis
mvn clean package -DskipTests
```

The build produces `target/spark-redis_2.13-<version>.jar` and `target/spark-redis_2.13-<version>-jar-with-dependencies.jar`.
To build for Scala 2.12 instead, for the default Spark 3.x download, switch the build first:

```
dev/change-scala-version.sh 2.12
mvn clean package -DskipTests
```

See [Dev environment](dev.md) to run the tests.

### Using the library with spark shell
Add Spark-Redis to Spark with the `--packages` command line option, which downloads the library and its dependencies from Maven Central:

```bash
$ bin/spark-shell --packages com.redislabs:spark-redis_2.12:3.1.0
```

or with the `--jars` option and a JAR file that includes the dependencies, from a [GitHub release](#github-releases) or
from your own build:

```bash
$ bin/spark-shell --jars <path-to>/spark-redis_2.12-<version>-jar-with-dependencies.jar
```

By default it connects to `localhost:6379` without any password, you can change the connection settings in the following manner:

```bash
$ bin/spark-shell --jars <path-to>/spark-redis_2.12-3.1.1-jar-with-dependencies.jar --conf "spark.redis.host=localhost" --conf "spark.redis.port=6379" --conf "spark.redis.password=passwd"
```

Spark-Redis 3.1.0 and earlier don't read `spark.redis.password`. They read the password from `spark.redis.auth`, which
Spark shows in clear text in the Spark UI and event logs; see
[Keeping the password out of the Spark UI and logs](configuration.md#keeping-the-password-out-of-the-spark-ui-and-logs).

On Databricks, install the library on the cluster: 3.1.0 from Maven with the coordinates
`com.redislabs:spark-redis_2.12:3.1.0`, or 3.1.1 by uploading its `jar-with-dependencies`. Then set the `spark.redis.*`
parameters in the cluster's Spark config.


### Configuring connection to Redis in a self-contained application

An example configuration of SparkContext with Redis configuration:

```scala
import com.redislabs.provider.redis._

...

val sc = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("myApp")
      // initial redis host - can be any node in cluster mode
      .set("spark.redis.host", "localhost")
      // initial redis port
      .set("spark.redis.port", "6379")
      // optional redis AUTH password
      .set("spark.redis.password", "passwd")
  )
```

The SparkSession can be configured in a similar manner:

```scala
val spark = SparkSession
  .builder()
  .appName("myApp")
  .master("local[*]")
  .config("spark.redis.host", "localhost")
  .config("spark.redis.port", "6379")
  .config("spark.redis.password", "passwd")
  .getOrCreate()
  
val sc = spark.sparkContext  
```

The `spark.redis.*` parameters are read when the `SparkContext` is created. Setting them later with `spark.conf.set(...)`,
or with `SparkSession.builder().config(...)` when a `SparkContext` already exists, has no effect. See
[Configuration](configuration.md#when-the-configuration-is-read) for all the parameters, TLS and authentication.

### Create RDD

```scala
import com.redislabs.provider.redis._

val keysRDD = sc.fromRedisKeyPattern("foo*", 5)
val keysRDD = sc.fromRedisKeys(Array("foo", "bar"), 5)
```

### Write Dataframe

```scala
df.write
  .format("org.apache.spark.sql.redis")
  .option("table", "foo")
  .save()
```

### Create Stream

```scala
import com.redislabs.provider.redis.streaming._

val ssc = new StreamingContext(sc, Seconds(1))
val redisStream = ssc.createRedisStream(Array("foo", "bar"),
    storageLevel = StorageLevel.MEMORY_AND_DISK_2)
```
