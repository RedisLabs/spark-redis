# RDD

  - [The keys RDD](#the-keys-rdd)
  - [Reading data](#reading-data)
  - [Writing data](#writing-data)
  - [Read and write configuration options](#read-and-write-configuration-options)

### The keys RDD
Since data access in Redis is based on keys, to use Spark-Redis you'll first need a keys RDD.  The following example shows how to read key names from Redis into an RDD:
```scala
import com.redislabs.provider.redis._

val keysRDD = sc.fromRedisKeyPattern("foo*", 5)
val keysRDD = sc.fromRedisKeys(Array("foo", "bar"), 5)
```

The above example populates the keys RDD by retrieving the key names from Redis that match the given pattern (`foo*`) or the keys can be listed by an Array. Furthermore, it overrides the default setting of 3 partitions in the RDD with a new value of 5 - each partition consists of a set of Redis cluster hashslots contain the matched key names.

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

Using `fromRedisZSet` will store in `zsetRDD: RDD[String]`, an RDD that consists of members and the members' ranges are within [startPos, endPos] of its own Sorted Set from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

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

Using `fromRedisZSet` will store in `zsetRDD: RDD[String]`, an RDD that consists of members and the members' scores are within [min, max] from the Redis Sorted Sets whose keys are provided by keyPattern or Array[String].

### Writing data
To write data to Redis from Spark, you'll need to prepare the appropriate RDD depending on the type of data you want to write.

#### Strings
For String values, your RDD should consist of the key-value pairs that are to be written. Assuming that the strings RDD is called `stringRDD`, use the following snippet for writing it to Redis:

```scala
sc.toRedisKV(stringRDD)
```

#### Hashes
To store a Redis Hash, the RDD should consist of its field-value pairs. If the RDD is called `hashRDD`, the following should be used for storing it in the key name specified by `hashName`:

```scala
sc.toRedisHASH(hashRDD, hashName)
```

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

#### Sets
For storing data in a Redis Set, use `toRedisSET` as follows:

```scala
sc.toRedisSET(setRDD, setName)
```

Where `setRDD` is an RDD with the set's string elements and `setName` is the name of the key for that set.

#### Sorted Sets
```scala
sc.toRedisZSET(zsetRDD, zsetName)
```

The above example demonstrates storing data in Redis in a Sorted Set. The `zsetRDD` in the example should contain pairs of members and their scores, whereas `zsetName` is the name for that key.

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
