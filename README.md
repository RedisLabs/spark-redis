# Spark-Redis

Spark-Redis is a connector for reading/writing from Redis cluster directly via Spark. It supports all the
types of Redis structures: Plain Key/Value, Hash, ZSet, Set, List
In Spark, the data from Redis is represented as an RDD with the tolerance of reshard and down of nodes.

Integrating Redis and Spark gives us a system that combines the best of both worlds.

## Requirements

This library requires Apache Spark 1.4+, Scala 2.10.4+, Jedis 2.7+, Redis 3.0+

## Current Limitations
* No Java or Python API bindings
* Only tested with the following configurations:
    - Redis 3.0
    - Scala 2.10
    - Spark 1.4.0
    - Jedis 2.7

## Enable Slaves For Reading
As jedis-2.7 doesn't support `readonly` command. We must wait for the release of jedis-2.8.
The pre-build jedis-2.8.0 is included in `with-slaves` branch. We can enable slaves for reading by

`git checkout with-slaves`

after the `git clone` in **Using the library** field

## Warnings
* The APIs will probably change several times before an official release

## Using the library
```
git clone https://github.com/RedisLabs/spark-redis.git
mvn clean install
```
In order to add the Spark-Redis jar file to Spark, you can use the --jars command line option.
For example, to include it when starting the spark-shell:

```
$ bin/spark-shell --jars <path-to>/spark-redis-<version>.jar,<path-to>/jedis-<version>.jar

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.4.0
      /_/

Using Scala version 2.10.4 (OpenJDK 64-Bit Server VM, Java 1.7.0_79)
Type in expressions to have them evaluated.
Type :help for more information.
...
```
To read data from Redis Cluster, you can use the library by loading the implicits from `com.redislab.provider.redis._` .

In the example we can see how to read from Redis Cluster.
```
scala> import com.redislab.provider.redis._
scala> val keysRDD = sc.fromRedisKeyPattern(("127.0.0.1", 7000), "*", 5)
scala> val kvRDD = keysRDD.getKV
scala> val hashRDD = keysRDD.getHash
scala> val zsetRDD = keysRDD.getZSet
scala> val listRDD = keysRDD.getList
scala> val setRDD = keysRDD.getSet
```

In the example we can see how to write to Redis Cluster.
```
scala> import import com.redislab.provider.redis._
scala> val keysRDD = sc.fromRedisKeyPattern(("127.0.0.1", 7000), "*", 5)
scala> val kvRDD = keysRDD.getKV
scala> sc.toRedisHASH(kvRDD, "saved_hash", ("127.0.0.1", 7000))
```

