
# RDD

  - [The keys RDD](#the-keys-rdd)
  - [Reading data](#reading-data)
  - [Writing data](#writing-data)
  - [Running custom commands](#running-custom-commands)
  - [Read and write configuration options](#read-and-write-configuration-options)

### The keys RDD
Since data access in Redis is based on keys, to use Spark-Redis you'll first need a keys RDD.  The following example shows how to read key names from Redis into an RDD:
```scala
import com.redislabs.provider.redis._

val keysRDD = sc.fromRedisKeyPattern("foo*", 5)
val keysRDD = sc.fromRedisKeys(Array("foo", "bar"), 5)
```

The above example populates the keys RDD by retrieving the key names from Redis that match the given pattern (`foo*`) or the keys can be listed by an Array. Furthermore, it overrides the default setting of 3 partitions in the RDD with a new value of 5 - each partition consists of a set of Redis cluster hashslots contain the matched key names.

A key pattern is resolved with `SCAN` on every master node, see `spark.redis.scan.count` in [Tuning reads and writes](configuration.md#tuning-reads-and-writes).
Partitions are split by key, so all the values of one key are read by one task: reading a single large list or hash with
`partitionNum = 8` still produces one non-empty partition. Use `repartition` after reading to spread the data.

The functions below that read Hashes, Lists, Sets and Sorted Sets return the values only, without the name of the key they
come from. To keep the key name, see [Running custom commands](#running-custom-commands).

### Reading data

Each of Redis' data types can be read into an RDD. The following snippet demonstrates reading from Redis Strings.

#### Strings

```scala
import com.redislabs.provider.redis._
val stringRDD = sc.fromRedisKV("keyPattern*")
val stringRDD = sc.fromRedisKV(Array("foo", "bar"))
```

Once run, `stringRDD: RDD[(String, String)]` will contain the string values of all keys whose names are provided by keyPattern or `Array[String]`.

#### Hashes
```scala
val hashRDD = sc.fromRedisHash("keyPattern*")
val hashRDD = sc.fromRedisHash(Array("foo", "bar"))
```

This will populate `hashRDD: RDD[(String, String)]` with the fields and values of the Redis Hashes, the hashes' names are provided by keyPattern or `Array[String]`.

#### Lists
```scala
val listRDD = sc.fromRedisList("keyPattern*")
val listRDD = sc.fromRedisList(Array("foo", "bar"))
```
The contents (members) of the Redis Lists in whose names are provided by keyPattern or `Array[String]` will be stored in `listRDD: RDD[String]`.

#### Sets
```scala
val setRDD = sc.fromRedisSet("keyPattern*")
val setRDD = sc.fromRedisSet(Array("foo", "bar"))
```

The Redis Sets' members will be written to `setRDD: RDD[String]`.

#### Sorted Sets
```scala
val zsetRDD = sc.fromRedisZSetWithScore("keyPattern*")
val zsetRDD = sc.fromRedisZSetWithScore(Array("foo", "bar"))
```

Using `fromRedisZSetWithScore` will store in `zsetRDD: RDD[(String, Double)]` an RDD that consists of members and their scores, from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```scala
val zsetRDD = sc.fromRedisZSet("keyPattern*")
val zsetRDD = sc.fromRedisZSet(Array("foo", "bar"))
```

Using `fromRedisZSet` will store in `zsetRDD: RDD[String]`, an RDD that consists of members from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```scala
val startPos: Int = _
val endPos: Int = _
val zsetRDD = sc.fromRedisZRangeWithScore("keyPattern*", startPos, endPos)
val zsetRDD = sc.fromRedisZRangeWithScore(Array("foo", "bar"), startPos, endPos)
```

Using `fromRedisZRangeWithScore` will store in `zsetRDD: RDD[(String, Double)]`, an RDD that consists of members and the members' ranges are within [startPos, endPos] of its own Sorted Set from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```scala
val startPos: Int = _
val endPos: Int = _
val zsetRDD = sc.fromRedisZRange("keyPattern*", startPos, endPos)
val zsetRDD = sc.fromRedisZRange(Array("foo", "bar"), startPos, endPos)
```

Using `fromRedisZRange` will store in `zsetRDD: RDD[String]`, an RDD that consists of members and the members' ranges are within [startPos, endPos] of its own Sorted Set from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```scala
val min: Double = _
val max: Double = _
val zsetRDD = sc.fromRedisZRangeByScoreWithScore("keyPattern*", min, max)
val zsetRDD = sc.fromRedisZRangeByScoreWithScore(Array("foo", "bar"), min, max)
```

Using `fromRedisZRangeByScoreWithScore` will store in `zsetRDD: RDD[(String, Double)]`, an RDD that consists of members and the members' scores are within [min, max] from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

```scala
val min: Double = _
val max: Double = _
val zsetRDD = sc.fromRedisZRangeByScore("keyPattern*", min, max)
val zsetRDD = sc.fromRedisZRangeByScore(Array("foo", "bar"), min, max)
```

Using `fromRedisZRangeByScore` will store in `zsetRDD: RDD[String]`, an RDD that consists of members and the members' scores are within [min, max] from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

### Writing data
To write data to Redis from Spark, you'll need to prepare the appropriate RDD depending on the type of data you want to write.

The write functions add to the data that already exists in Redis: Strings are overwritten (`SET`), Hashes are updated field
by field (`HSET`), Lists are appended to (`RPUSH`), and Sets and Sorted Sets get new members (`SADD`/`ZADD`).
Existing fields, elements and members are not removed, so writing the same RDD twice into a List duplicates its elements.
To replace the content of a key, delete it first, for example as shown in [Deleting a table](dataframe.md#deleting-a-table).

#### Strings
For String values, your RDD should consist of the key-value pairs that are to be written. Assuming that the strings RDD is called `stringRDD`, use the following snippet for writing it to Redis:

```scala
sc.toRedisKV(stringRDD)
```

In order to set an expiry on the key, we can pass in the `ttl` (in seconds) as an additional argument:

```scala
sc.toRedisKV(stringRDD, ttl)
```

By default, Strings won't have any expiry set.

Use the following to store an RDD of binary key-value pairs as Redis Strings:

```scala
sc.toRedisByteKV(byteKVRDD)
```

The `byteKVRDD` is an RDD of tuples (`key`, `value`) represented as byte arrays, e.g. serialized protobufs. Expiry can be set the same way:

```scala
sc.toRedisByteKV(byteKVRDD, ttl)
```

#### Hashes
To store a Redis Hash, the RDD should consist of its field-value pairs. If the RDD is called `hashRDD`, the following should be used for storing it in the key name specified by `hashName`:

```scala
sc.toRedisHASH(hashRDD, hashName)
```
In order to set an expiry on the key, we can pass in the `ttl` (in seconds) as an additional argument:

```scala
sc.toRedisHASH(hashRDD, hashName, ttl)
```

By default, Hashes won't have any expiry set.

Use the following to store an RDD into multiple hashs:

```scala
sc.toRedisHASHes(hashRDD, ttl)
```

The `hashRDD` is a rdd of tuples (`hashname`, `map[field name, field value]`)

```scala
sc.toRedisByteHASHes(hashRDD, ttl)
```

The `hashRDD` is a rdd of tuples (`hashname`, `map[field name, field value]`) represented as byte arrays.

#### Lists
Use the following to store an RDD in a Redis List:

```scala
sc.toRedisLIST(listRDD, listName)
```

Use the following to store an RDD in a fixed-size Redis List:

```scala
sc.toRedisFixedLIST(listRDD, listName, listSize)
```

The `listRDD` is an RDD that contains all of the list's string elements in order, and `listName` is the list's key name.
`listSize` is an integer which specifies the size of the Redis list; it is optional, and will default to an unlimited size.
`toRedisFixedLIST` pushes the elements to the head of the list (`LPUSH`) and then trims the list to its first `listSize` elements.

The partitions of the RDD are written in parallel, so the order of the elements in the list is only preserved within a
partition. Use `coalesce(1)` before writing if the order matters.

Use the following to store an RDD in multiple Redis Lists:

```scala
sc.toRedisLISTs(rdd)
```

The `rdd` is an RDD of tuples (`list name`, `list values`)

Use the following to store an RDD of binary values in multiple Redis Lists:

```scala
sc.toRedisByteLISTs(byteListRDD)
```

The `byteListRDD` is an RDD of tuples (`list name`, `list values`) represented as byte arrays. 

Expiry can be set on Lists by passing in an additional argument called `ttl` (in seconds) to the above methods except `toRedisFixedLIST`:
```scala
sc.toRedisLIST(listRDD, listName, ttl)
sc.toRedisLISTs(rdd, ttl)
sc.toRedisByteLISTs(byteListRDD, ttl)
```

By default, Lists won't have any expiry set.

#### Sets
For storing data in a Redis Set, use `toRedisSET` as follows:

```scala
sc.toRedisSET(setRDD, setName)
```

Where `setRDD` is an RDD with the set's string elements and `setName` is the name of the key for that set.

In order to set an expiry on the key, we can pass in the `ttl` (in seconds) as an additional argument:

```scala
sc.toRedisSET(setRDD, setName, ttl)
```

By default, Sets won't have any expiry set.

#### Sorted Sets
```scala
sc.toRedisZSET(zsetRDD, zsetName)
```

The above example demonstrates storing data in Redis in a Sorted Set. The `zsetRDD: RDD[(String, String)]` in the example should contain pairs of members and their scores (the score is a string that is parsed as a `Double`), whereas `zsetName` is the name for that key.

In order to set an expiry on the key, we can pass in the `ttl` (in seconds) as an additional argument:

```scala
sc.toRedisZSET(zsetRDD, zsetName, ttl)
```

By default, Sorted Sets won't have any expiry set.

### Running custom commands

The functions above cover the common read and write patterns. For other commands, for example `SETNX`, `HINCRBY`, `SADD`
to many Sets, `LSET`, `GEOADD`, `PFADD`, `XADD`, a Lua script for conditional updates, or to read values together with
their key names, open connections yourself in `foreachPartition` or `mapPartitions` and use the
[Jedis](https://github.com/redis/jedis) API. `RedisConfig` gives you a connection to the node that owns a key, which also
works with a Redis Cluster, and `foreachWithPipeline` batches the commands in pipelines of `spark.redis.max.pipeline.size`
commands.

The following example adds members to many Sets with `SADD`:

```scala
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.PipelineUtils.foreachWithPipeline

val redisConfig = RedisConfig.fromSparkConf(sc.getConf)
val readWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)

// (set name, member) pairs
val members = sc.parallelize(Seq(("tags:1", "a"), ("tags:1", "b"), ("tags:2", "c")))

members.foreachPartition { partition =>
  implicit val rwConf: ReadWriteConfig = readWriteConfig
  partition.grouped(readWriteConfig.rddWriteIteratorGroupingSize).foreach { batch =>
    // send each key to the node that owns it
    batch.groupBy { case (key, _) => redisConfig.getHost(key) }.foreach { case (node, items) =>
      withConnection(node.endpoint.connect()) { conn =>
        foreachWithPipeline(conn, items) { case (pipeline, (key, member)) =>
          pipeline.sadd(key, member)
        }
      }
    }
  }
}
```

The following example reads Hashes together with their key names. The keys can come from any RDD, or from
`sc.fromRedisKeyPattern(...)`:

```scala
import scala.collection.JavaConverters._

val keys = sc.parallelize(Seq("person:1", "person:2"))

val hashes = keys.mapPartitions { partition =>
  partition.map { key =>
    withConnection(redisConfig.connectionForKey(key)) { conn =>
      (key, conn.hgetAll(key).asScala.toMap)
    }
  }
}
```

### Read and write configuration options

Some [configuration options](configuration.md) can be overridden for a particular RDD:

```scala
val readWriteConf = ReadWriteConfig(scanCount = 1000, maxPipelineSize = 1000)
val rdd = sc.fromRedisKeyPattern(keyPattern)(readWriteConfig = readWriteConf) 
```

or with an implicit parameter:

```scala
implicit val readWriteConf = ReadWriteConfig(scanCount = 1000, maxPipelineSize = 1000)
val rdd = sc.fromRedisKeyPattern(keyPattern)
```
