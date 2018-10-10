[![Build Status](https://travis-ci.org/RedisLabs/spark-redis.svg)](https://travis-ci.org/RedisLabs/spark-redis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/spark-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/spark-redis)

# Spark-Redis
A library for reading and writing data from and to [Redis](http://redis.io) with [Apache Spark](http://spark.apache.org/)

Spark-Redis provides access to all of Redis' data structures - String, Hash, List, Set and Sorted Set - from Spark as RDDs. The library can be used both with Redis stand-alone as well as clustered databases. When used with Redis cluster, Spark-Redis is aware of its partitioning scheme and adjusts in response to resharding and node failure events.

Spark-Redis also provides Spark-Streaming support.

## Version compatibility and branching

The library has several branches, each corresponds to a different supported Spark version. For example, 'branch-2.3' works with any Spark 2.3.x version.
The master branch contains the recent development for the next release.

| Spark-Redis | Spark         | Redis            | Supported Scala Versions | 
| ----------- | ------------- | ---------------- | ------------------------ |
| 2.3         | 2.3           | >=2.9.0          | 2.11                     | 
| 1.4         | 1.4           |                  | 2.10                     | 


## Known limitations

* Java, Python and R API bindings are not provided at this time

## Additional considerations
This library is work in progress so the API may change before the official release.

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
      .set("redis.host", "localhost")

      // initial redis port
      .set("redis.port", "6379")

      // optional redis AUTH password
      .set("redis.auth", "")
  )
```

The supported configuration keys are:

* `redis.host` - host or IP of the initial node we connect to. The connector will read the cluster
topology from the initial node, so there is no need to provide the rest of the cluster nodes.
* `redis.port` - the inital node's TCP redis port.
* `redis.auth` - the initial node's AUTH password
* `redis.db` - optional DB number. Avoid using this, especially in cluster mode.

### The keys RDD
Since data access in Redis is based on keys, to use Spark-Redis you'll first need a keys RDD.  The following example shows how to read key names from Redis into an RDD:
```
import com.redislabs.provider.redis._

val keysRDD = sc.fromRedisKeyPattern("foo*", 5)
val keysRDD = sc.fromRedisKeys(Array("foo", "bar"), 5)
```

The above example populates the keys RDD by retrieving the key names from Redis that match the given pattern (`foo*`) or the keys can be listed by an Array. Furthermore, it overrides the default setting of 3 partitions in the RDD with a new value of 5 - each partition consists of a set of Redis cluster hashslots contain the matched key names.


### Reading data

Each of Redis' data types can be read to an RDD. The following snippet demonstrates reading Redis Strings.

#### Strings

```
import com.redislabs.provider.redis._
val stringRDD = sc.fromRedisKV("keyPattern*")
val stringRDD = sc.fromRedisKV(Array("foo", "bar"))
```

Once run, `stringRDD: RDD[(String, String)]` will contain the string values of all keys whose names are provided by keyPattern or Array[String].

#### Hashes
```
val hashRDD = sc.fromRedisHash("keyPattern*")
val hashRDD = sc.fromRedisHash(Array("foo", "bar"))
```

This will populate `hashRDD: RDD[(String, String)]` with the fields and values of the Redis Hashes, the hashes' names are provided by keyPattern or Array[String]

#### Lists
```
val listRDD = sc.fromRedisList("keyPattern*")
val listRDD = sc.fromRedisList(Array("foo", "bar"))
```
The contents (members) of the Redis Lists in whose names are provided by keyPattern or Array[String] will be stored in `listRDD: RDD[String]`

#### Sets
```
val setRDD = sc.fromRedisSet("keyPattern*")
val setRDD = sc.fromRedisSet(Array("foo", "bar"))
```

The Redis Sets' members will be written to `setRDD: RDD[String]`.

#### Sorted Sets
```
val zsetRDD = sc.fromRedisZSetWithScore("keyPattern*")
val zsetRDD = sc.fromRedisZSetWithScore(Array("foo", "bar"))
```

Using `fromRedisZSetWithScore` will store in `zsetRDD: RDD[(String, Double)]`, an RDD that consists of members and their scores, from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```
val zsetRDD = sc.fromRedisZSet("keyPattern*")
val zsetRDD = sc.fromRedisZSet(Array("foo", "bar"))
```

Using `fromRedisZSet` will store in `zsetRDD: RDD[String]`, an RDD that consists of members, from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```
val startPos: Int = _
val endPos: Int = _
val zsetRDD = sc.fromRedisZRangeWithScore("keyPattern*", startPos, endPos)
val zsetRDD = sc.fromRedisZRangeWithScore(Array("foo", "bar"), startPos, endPos)
```

Using `fromRedisZRangeWithScore` will store in `zsetRDD: RDD[(String, Double)]`, an RDD that consists of members and the members' ranges are within [startPos, endPos] of its own Sorted Set, from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```
val startPos: Int = _
val endPos: Int = _
val zsetRDD = sc.fromRedisZRange("keyPattern*", startPos, endPos)
val zsetRDD = sc.fromRedisZRange(Array("foo", "bar"), startPos, endPos)
```

Using `fromRedisZSet` will store in `zsetRDD: RDD[String]`, an RDD that consists of members and the members' ranges are within [startPos, endPos] of its own Sorted Set, from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```
val min: Double = _
val max: Double = _
val zsetRDD = sc.fromRedisZRangeByScoreWithScore("keyPattern*", min, max)
val zsetRDD = sc.fromRedisZRangeByScoreWithScore(Array("foo", "bar"), min, max)
```

Using `fromRedisZRangeByScoreWithScore` will store in `zsetRDD: RDD[(String, Double)]`, an RDD that consists of members and the members' scores are within [min, max], from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```
val min: Double = _
val max: Double = _
val zsetRDD = sc.fromRedisZRangeByScore("keyPattern*", min, max)
val zsetRDD = sc.fromRedisZRangeByScore(Array("foo", "bar"), min, max)
```

Using `fromRedisZSet` will store in `zsetRDD: RDD[String]`, an RDD that consists of members and the members' scores are within [min, max], from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

### Writing data
To write data from Spark to Redis, you'll need to prepare the appropriate RDD depending on the data type you want to use for storing the data in it.

#### Strings
For String values, your RDD should consist of the key-value pairs that are to be written. Assuming that the strings RDD is called `stringRDD`, use the following snippet for writing it to Redis:

```
...
sc.toRedisKV(stringRDD)
```

#### Hashes
To store a Redis Hash, the RDD should consist of its field-value pairs. If the RDD is called `hashRDD`, the following should be used for storing it in the key name specified by `hashName`:

```
...
sc.toRedisHASH(hashRDD, hashName)
```

#### Lists
Use the following to store an RDD in a Redis List:

```
...
sc.toRedisLIST(listRDD, listName)
```

Use the following to store an RDD in a fixed-size Redis List:

```
...
sc.toRedisFixedLIST(listRDD, listName, listSize)
```

The `listRDD` is an RDD that contains all of the list's string elements in order, and `listName` is the list's key name.
`listSize` is an integer which specifies the size of the redis list; it is optional, and will default to an unlimited size.


#### Sets
For storing data in a Redis Set, use `toRedisSET` as follows:

```
...
sc.toRedisSET(setRDD, setName)
```

Where `setRDD` is an RDD with the set's string elements and `setName` is the name of the key for that set.

#### Sorted Sets
```
...
sc.toRedisZSET(zsetRDD, zsetName)
```

The above example demonstrates storing data in Redis in a Sorted Set. The `zsetRDD` in the example should contain pairs of members and their scores, whereas `zsetName` is the name for that key.

### Streaming
Spark-Redis support streaming data from Redis instance/cluster, currently streaming data are fetched from Redis' List by the `blpop` command. Users are required to provide an array which stores all the List names they are interested in. The [storageLevel](http://spark.apache.org/docs/latest/streaming-programming-guide.html#data-serialization) is `MEMORY_AND_DISK_SER_2` by default, you can change it on your demand.
`createRedisStream` will create a `(listName, value)` stream, but if you don't care about which list feeds the value, you can use `createRedisStreamWithoutListname` to get the only `value` stream.

Use the following to get a `(listName, value)` stream from `foo` and `bar` list

```
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import com.redislabs.provider.redis._
val ssc = new StreamingContext(sc, Seconds(1))
val redisStream = ssc.createRedisStream(Array("foo", "bar"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
redisStream.print()
ssc.start()
ssc.awaitTermination()
```


Use the following to get a `value` stream from `foo` and `bar` list

```
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import com.redislabs.provider.redis._
val ssc = new StreamingContext(sc, Seconds(1))
val redisStream = ssc.createRedisStreamWithoutListname(Array("foo", "bar"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
redisStream.print()
ssc.start()
ssc.awaitTermination()
```


### Connecting to Multiple Redis Clusters/Instances

```
def twoEndpointExample ( sc: SparkContext) = {
  val redisConfig1 = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379, "passwd"))
  val redisConfig2 = new RedisConfig(new RedisEndpoint("127.0.0.1", 7379))
  val rddFromEndpoint1 = {
    //endpoint("127.0.0.1", 6379) as the default connection in this block
    implicit val c = redisConfig1
    sc.fromRedisKV("*")
  }
  val rddFromEndpoint2 = {
    //endpoint("127.0.0.1", 7379) as the default connection in this block
    implicit val c = redisConfig2
    sc.fromRedisKV("*")
  }
}
```
If you want to use multiple redis clusters/instances, an implicit RedisConfig can be used in a code block to specify the target cluster/instance in that block.

## Contributing

You're encouraged to contribute to the open source Spark-Redis project. There are two ways you can do so.

### Issues

If you encounter an issue while using the Spark-Redis library, please report it at the project's [issues tracker](https://github.com/RedisLabs/spark-redis/issues).

### Pull request

Code contributions to the Spark-Redis project can be made using [pull requests](https://github.com/RedisLabs/spark-redis/pulls). To submit a pull request:

 1. Fork this project.
 2. Make and commit your changes.
 3. Submit your changes as a pull request.
