[![Build Status](https://travis-ci.org/RedisLabs/spark-redis.svg)](https://travis-ci.org/RedisLabs/spark-redis)

# Spark-Redis
A library for reading and writing data from and to [Redis](http://redis.io) with [Apache Spark](http://spark.apache.org/), for Spark SQL and DataFrames.

Spark-Redis provides access to all of Redis' data structures - String, Hash, List, Set and Sorted Set - from Spark as RDDs. The library can be used both with Redis stand-alone as well as clustered databases. When used with Redis cluster, Spark-Redis is aware of its partitioning scheme and adjusts in response to resharding and node failure events.

## Minimal requirements
You'll need the the following to use Spark-Redis:

 - Apache Spark v1.4.0
 - Scala v2.10.4
 - Jedis v2.7
 - Redis v2.8.12 or v3.0.3

## Known limitations

* Java, Python and R API bindings are not provided at this time 
* The package was only tested with the following stack:
 - Apache Spark v1.4.0
 - Scala v2.10.4
 - Jedis v2.7 and v2.8 pre-release (see [below](#jedis-and-read-only-redis-cluster-slave-nodes) for details)
 - Redis v2.8.12 and v3.0.3

## Additional considerations
This library is work in progress so the API may change before the official release.

## Getting the library
You can download the library's source and build it:
```
git clone https://github.com/RedisLabs/spark-redis.git
cd spark-redis
mvn clean package -DskipTests
```

### Jedis and read-only Redis cluster slave nodes
Jedis' current version - v2.7 - does not support reading from Redis cluster's slave nodes. This functionality will only be included in its upcoming version, v2.8.

To use Spark-Redis with Redis cluster's slave nodes, the library's source includes a pre-release of Jedis v2.8 under the `with-slaves` branch. Switch to that branch by entering the following before running `mvn clean install`:
```
git checkout with-slaves
```

## Using the library
Add Spark-Redis to Spark with the `--jars` command line option. For example, use it from spark-shell, include it in the following manner:

```
$ bin/spark-shell --jars <path-to>/spark-redis-<version>.jar,<path-to>/jedis-<version>.jar

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.4.0
      /_/

Using Scala version 2.10.4 (OpenJDK 64-Bit Server VM, Java 1.7.0_79)
...
```

The following sections contain code snippets that demonstrate the use of Spark-Redis. To use the sample code, you'll need to replace `your.redis.server` and `6379` with your Redis database's IP address or hostname and port, respectively.

### The keys RDD
Since data access in Redis is based on keys, to use Spark-Redis you'll first need a keys RDD.  The following example shows how to read key names from Redis into an RDD:
```
import com.redislabs.provider.redis._
val keysRDD = sc.fromRedisKeyPattern(("your.redis.server", 6379), "foo*", 5)
```

The above example populates the keys RDD by retrieving the key names from Redis that match the given pattern (`foo*`). Furthermore, it overrides the default setting of 3 partitions in the RDD with a new value of 5 - each partition consists of a set of Redis cluster hashslots contain the matched key names.


### Reading data

Each of Redis' data types can be read to an RDD. The following snippet demonstrates reading Redis Strings.

#### Strings

```
import com.redislabs.provider.redis._
val keysRDD = sc.fromRedisKeyPattern(("your.redis.server", 6379), "keyPattern", 5)

val stringRDD = keysRDD.getKV
```

Once run, `stringRDD` will contain the string values of all keys whose names are in provided in `keysRDD`. To read other data types, replace the last line in the example above with one of the following lines according to the actual type that's used.

#### Hashes
```
val hashRDD = keysRDD.getHash
```

This will populate `hashRDD` with the fields and values of the Redis Hashes given by `keysRDD`.

#### Lists
```
val listRDD = keysRDD.getList
```
The contents (members) of the Redis Lists in `keysRDD` will be stored in `listRDD`

#### Sets
```
val setRDD = keysRDD.getSet
```

The Redis Sets' members will be written to `setRDD`.

#### Sorted Sets
```
val zsetRDD = keysRDD.getZSet
```

Using `getZSet` will store in `zsetRDD`, an RDD that consists of members and their scores, from the Redis Sorted Sets in `keysRDD`.

### Writing data
To write data from Spark to Redis, you'll need to prepare the appropriate RDD depending on the data type you want to use for storing the data in it.

#### Strings
For String values, your RDD should consist of the key-value pairs that are to be written. Assuming that the strings RDD is called `stringRDD`, use the following snippet for writing it to Redis:

```
...
sc.toRedisKV(stringRDD, ("your.redis.server", 6379))
```

#### Hashes
To store a Redis Hash, the RDD should consist of its field-value pairs. If the RDD is called `hashRDD`, the following should be used for storing it in the key name specified by `hashName`:

```
...
sc.toRedisHASH(hashRDD, hashName, ("your.redis.server", 6379))
```

#### Lists
Use the following to store an RDD in a Redis List:

```
...
sc.toRedisLIST(listRDD, listName, ("your.redis.server", 6379))
```

Use the following to store an RDD in a fixed-size Redis List:

```
...
sc.toRedisFixedLIST(listRDD, listName, ("your.redis.server", 6379), listSize)
```

The `listRDD` is an RDD that contains all of the list's string elements in order, and `listName` is the list's key name.
`listSize` is an integer which specifies the size of the redis list; it is optional, and will default to an unlimited size.


#### Sets
For storing data in a Redis Set, use `toRedisSET` as follows:

```
...
sc.toRedisSET(setRDD, setName, ("your.redis.server", 6379))
```

Where `setRDD` is an RDD with the set's string elements and `setName` is the name of the key for that set.

#### Sorted Sets
```
...
sc.toRedisZSET(zsetRDD, zsetName, ("your.redis.server", 6379))
```

The above example demonstrates storing data in Redis in a Sorted Set. The `zsetRDD` in the example should contain pairs of members and their scores, whereas `zsetName` is the name for that key.

## Contributing

You're encouraged to contribute to the open source Spark-Redis project. There are two ways you can do so.

### Issues

If you encounter an issue while using the Spark-Redis library, please report it at the project's [issues tracker](https://github.com/RedisLabs/spark-redis/issues).

### Pull request

Code contributions to the Spark-Redis project can be made using [pull requests](https://github.com/RedisLabs/spark-redis/pulls). To submit a pull request:

 1. Fork this project.
 2. Make and commit your changes.
 3. Submit your changes as a pull request.
