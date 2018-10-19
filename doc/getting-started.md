## Getting the library

### Maven

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>spark-redis</artifactId>
      <version>2.3.0</version>
    </dependency>
  </dependencies>
```

### Build form source
You can download the library's source and build it:
```
git clone https://github.com/RedisLabs/spark-redis.git
cd spark-redis
mvn clean package -DskipTests
```

## Using the library
Add Spark-Redis to Spark with the `--jars` command line option. For example, use it from spark-shell, include it in the following manner:

```
$ bin/spark-shell --jars <path-to>/spark-redis-<version>-jar-with-dependencies.jar

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_101)
```

The following sections contain code snippets that demonstrate the use of Spark-Redis. To use the sample code, you'll need to replace `your.redis.server` and `6379` with your Redis database's IP address or hostname and port, respectively.

### Configuring Connections to Redis using SparkConf

Below is an example configuration of SparkContext with redis configuration:

```scala
import com.redislabs.provider.redis._

...

sc = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("myApp")

      // initial redis host - can be any node in cluster mode
      .set("spark.redis.host", "localhost")

      // initial redis port
      .set("spark.redis.port", "6379")

      // optional redis AUTH password
      .set("spark.redis.auth", "")
  )
```

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
  .option("table", "person")
  .save()
```

### Create Stream

```scala
val ssc = new StreamingContext(sc, Seconds(1))
val redisStream = ssc.createRedisStream(Array("foo", "bar"),
    storageLevel = StorageLevel.MEMORY_AND_DISK_2)
```
