# Spark-Redis

Spark-Redis is a connector for reading/writing from Redis cluster directly via Spark. It supports all the
types of Redis structures: Plain Key/Value, Hash, ZSet, Set, List
In Spark, the data from Redis is represented as an RDD with the tolerance of reshard and down of nodes.

Integrating Redis and Spark gives us a system that combines the best of both worlds.

## Requirements

This library requires Apache Spark 1.4+, Scala 2.10.4+, Jedis 2.7+, Redis 2.8+

## Current Limitations
* No Java or Python API bindings
* Only tested with the following configurations:
    - Redis 2.8+
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
There are two ways of using Spark-Redis library:

You can use it as a maven dependency:
```
<repositories>
    <repository>
        <id>spark-redis-mvn-repo</id>
        <url>https://raw.github.com/RedisLabs/spark-redis/mvn-repo/</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.redislabs</groupId>
        <artifactId>spark-redis</artifactId>
        <version>0.5.1</version>
    </dependency>
</dependencies>
```

There also exists the possibility of downloading the project by doing:
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
To read data from Redis Server, you can use the library by loading the implicits from `com.redislabs.provider.redis._` .

In the example we can see how to read from Redis Server.
```
import com.redislabs.provider.redis._
val keysRDD = sc.fromRedisKeyPattern(("127.0.0.1", 7000), "keyPattern", 5)
```
keyPattern should be a plain string or a RedisRegex.
keysRDD is a RDD holds all the keys of keyPattern of the redis server.
keysRDD is divided into 5(default 3) partitions by hash slots.

Using Redis' Key/Values
```
import com.redislabs.provider.redis._
val keysRDD = sc.fromRedisKeyPattern(("127.0.0.1", 7000), "keyPattern", 5)
val kvRDD = keysRDD.getKV
```
kvRDD is a RDD holds all the k/v pairs whose k's pattern is keyPattern, and k must be of 'string' type in redis-server.

Using Redis' Hash
```
import com.redislabs.provider.redis._
val keysRDD = sc.fromRedisKeyPattern(("127.0.0.1", 7000), "keyPattern", 5)
val hashRDD = keysRDD.getHash
```
hashRDD is a RDD holds all the dicts' contents, and the dicts' names must be of keyPattern and exists in the redis-server.

Using Redis' ZSet
```
import com.redislabs.provider.redis._
val keysRDD = sc.fromRedisKeyPattern(("127.0.0.1", 7000), "keyPattern", 5)
val zsetRDD = keysRDD.getZSet
```
zsetRDD is a RDD holds all the zsets' contents(key, score), and the zsets' names must be of keyPattern and exists in the redis-server.

Using Redis' List
```
import com.redislabs.provider.redis._
val keysRDD = sc.fromRedisKeyPattern(("127.0.0.1", 7000), "keyPattern", 5)
val listRDD = keysRDD.getList
```
listRDD is a RDD holds all the lists' contents, and the lists' names must be of keyPattern and exists in the redis-server.

Using Redis' Set
```
import com.redislabs.provider.redis._
val keysRDD = sc.fromRedisKeyPattern(("127.0.0.1", 7000), "keyPattern", 5)
val setRDD = keysRDD.getSet
```
setRDD is a RDD holds all the sets' contents, score), and the sets' names must be of keyPattern and exists in the redis-server.

*****

To write data to Redis Server, you can use the library by loading the implicits from `com.redislabs.provider.redis._` .

In the example we can see how to write to Redis Server.

Saving as Redis' Key/Values
```
import com.redislabs.provider.redis._
val kvRDD = ...
sc.toRedisKV(kvRDD, ("127.0.0.1", 7000))
```
kvRDD is a RDD holds k/v pairs, we will store all the k/v pairs of kvRDD to the redis-server

Saving as Redis' Hash
```
import com.redislabs.provider.redis._
val hashRDD = ...
sc.toRedisHASH(hashRDD, hashName, ("127.0.0.1", 7000))
```
hashRDD is a RDD holds k/v pairs, we will store all the k/v pairs of hashRDD to a dict named hashName to the redis-server

Saving as Redis' ZSet
```
import com.redislabs.provider.redis._
val zsetRDD = ...
sc.toRedisZSET(zsetRDD, zsetName, ("127.0.0.1", 7000))
```
zsetRDD is a RDD holds k/v pairs, we will store all the k/v pairs of zsetRDD to a zset named zsetName to the redis-server

Saving as Redis' List
```
import com.redislabs.provider.redis._
val listRDD = ...
sc.toRedisLIST(listRDD, listName, ("127.0.0.1", 7000))
```
listRDD is a RDD holds strings, we will store all the strings of listRDD to a list named listName to the redis-server

Saving as Redis' Set
```
import com.redislabs.provider.redis._
val setRDD = ...
sc.toRedisSET(setRDD, setName, ("127.0.0.1", 7000))
```
setRDD is a RDD holds strings, we will store all the unique strings of setRDD to a set named setName to the redis-server
