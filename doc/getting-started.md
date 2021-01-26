## Getting the library

### Maven

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>spark-redis_2.11</artifactId>
      <version>2.4.2</version>
    </dependency>
  </dependencies>
```

Or

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>spark-redis_2.12</artifactId>
      <version>2.4.2</version>
    </dependency>
  </dependencies>
```

### SBT

```scala
libraryDependencies += "com.redislabs" %% "spark-redis" % "2.4.2"
```

### Build form source
You can download the library's source and build it:
```
git clone https://github.com/RedisLabs/spark-redis.git
cd spark-redis
mvn clean package -DskipTests
```

### Using the library with spark shell
Add Spark-Redis to Spark with the `--jars` command line option. 

```bash
$ bin/spark-shell --jars <path-to>/spark-redis-<version>-jar-with-dependencies.jar
```

By default it connects to `localhost:6379` without any password, you can change the connection settings in the following manner:

```bash
$ bin/spark-shell --jars <path-to>/spark-redis-<version>-jar-with-dependencies.jar --conf "spark.redis.host=localhost" --conf "spark.redis.port=6379" --conf "spark.redis.auth=passwd"
```


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
      .set("spark.redis.auth", "passwd")
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
  .config("spark.redis.auth", "passwd")
  .getOrCreate()
  
val sc = spark.sparkContext  
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
