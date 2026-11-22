# Troubleshooting

  - [JedisConnectionException: Could not get a resource from the pool](#jedisconnectionexception-could-not-get-a-resource-from-the-pool)
  - [Failed connecting to host localhost:6379](#failed-connecting-to-host-localhost6379)
  - [WRONGPASS or NOAUTH errors](#wrongpass-or-noauth-errors)
  - [ERR unknown command or ERR ... don't process admin command](#err-unknown-command-or-err--dont-process-admin-command)
  - [Read timed out](#read-timed-out)
  - [NullPointerException in Utils.redact](#nullpointerexception-in-utilsredact)
  - [NoSuchMethodError: scala.Predef$...](#nosuchmethoderror-scalapredef)
  - [NotSerializableException: SparkContext or StreamingContext](#notserializableexception-sparkcontext-or-streamingcontext)
  - [The job succeeds but data is missing in Redis](#the-job-succeeds-but-data-is-missing-in-redis)

## JedisConnectionException: Could not get a resource from the pool

This is a generic Jedis error: a connection to Redis could not be opened. The real reason is in the `Caused by:` part of the
stack trace. The most common causes are:

1. **The connection settings were not applied**, and Spark-Redis connected to the default `localhost:6379`. Look for
   `Failed connecting to host localhost:6379` in the stack trace, and see [below](#failed-connecting-to-host-localhost6379).
2. **The option names are wrong.** DataFrame and stream options don't have the `spark.redis.` prefix: use
   `.option("host", ...)`, `.option("auth", ...)`, `.option("ssl", "true")`, not `.option("spark.redis.host", ...)`. See
   [Configuration](configuration.md#overriding-the-connection-for-a-dataframe-stream-or-rdd).
3. **TLS is required** by the server (for example Amazon MemoryDB, Amazon ElastiCache with in-transit encryption, or any
   server with TLS enabled) but `ssl` isn't set to `true`. See [TLS](configuration.md#tls).
4. **Authentication fails**, see [WRONGPASS or NOAUTH errors](#wrongpass-or-noauth-errors).
5. **The executors can't reach Redis.** The driver and every executor connect to Redis directly. Check security groups,
   firewalls, VPC peering and private endpoints from the executor hosts, not only from the machine you run the
   notebook or `redis-cli` on. In cluster mode the executors connect to the node addresses that `CLUSTER SLOTS`
   returns, not to `spark.redis.host`. See [Cluster](cluster.md#node-addresses).

## Failed connecting to host localhost:6379

Spark-Redis didn't see your `spark.redis.*` settings and used the defaults. The parameters are read from the `SparkConf`
when the `SparkContext` is created, so settings made later are ignored:

* `spark.conf.set("spark.redis.host", ...)` doesn't work.
* `SparkSession.builder().config("spark.redis.host", ...).getOrCreate()` doesn't work if a `SparkContext` already exists,
  which is always the case in `spark-shell`, `pyspark` and notebooks. On Databricks, set the parameters in the cluster's
  **Spark config**. On Zeppelin, set them in the Spark interpreter settings.

Alternatively, pass the connection settings as DataFrame options (`host`, `port`, `auth`, ...). See
[When the configuration is read](configuration.md#when-the-configuration-is-read).

## WRONGPASS or NOAUTH errors

* `NOAUTH Authentication required`: the server requires a password, but none was sent. Set `spark.redis.auth` (or the `auth`
  option). Check that the setting was applied, see [above](#failed-connecting-to-host-localhost6379).
* `WRONGPASS invalid username-password pair or user is disabled`: the user or password is wrong. If the server has no
  password or no ACL user, don't pass empty strings such as `.option("user", "")` or `.option("auth", "")`. Leave the
  options unset, because an empty value is still sent with `AUTH`. If you only have a password, set `auth` and leave `user`
  unset.

## ERR unknown command or ERR ... don't process admin command

Spark-Redis needs `INFO`, `SCAN` and, in cluster mode, `CLUSTER SLOTS`. Proxies and managed services that block or don't
implement these commands are not supported. See [Redis server requirements](configuration.md#redis-server-requirements).

## Read timed out

`JedisConnectionException: java.net.SocketTimeoutException: Read timed out` means a command took longer than
`spark.redis.timeout` (2000 ms by default). This setting is both the connection timeout and the socket read timeout.
It typically happens when reading a single very large key (a hash, list or set with many elements is read with one
`HGETALL`, `LRANGE` or `SMEMBERS`), or when Redis is busy. Raise the timeout, for example `--conf spark.redis.timeout=60000`
or `.option("timeout", 60000)`.

## NullPointerException in Utils.redact

A stack trace like the following when saving or loading a DataFrame means that one of the options has a `null` value, for
example `.option("auth", None)` in Python or a variable that is `null` in Scala/Java:

```
java.lang.NullPointerException
  at java.util.regex.Matcher.getTextLength(...)
  ...
  at org.apache.spark.util.Utils$.redact(...)
  at org.apache.spark.sql.internal.SQLConf.redactOptions(...)
```

Don't pass options without a value.

## NoSuchMethodError: scala.Predef$...

Errors such as `java.lang.NoSuchMethodError: scala.Predef$.refArrayOps(...)` mean that the Spark-Redis artifact was built
for a different Scala version than your Spark distribution. Spark 3.x is built with Scala 2.12, so use the `spark-redis_2.12`
artifact. See [Getting the library](getting-started.md#getting-the-library).

## NotSerializableException: SparkContext or StreamingContext

When you write to Redis from `foreachRDD` in a Spark Streaming job with checkpointing enabled, don't use the outer `sc` or
`ssc` inside the closure. Use the RDD's own context:

```scala
import com.redislabs.provider.redis._

stream.foreachRDD { rdd =>
  rdd.sparkContext.toRedisKV(rdd)
}
```

See [Writing a DStream to Redis](streaming.md#writing-a-dstream-to-redis).

## The job succeeds but data is missing in Redis

* **Duplicate keys**: with the DataFrame API, rows that have the same `key.column` value are written to the same Redis key,
  and the last one wins. Check the number of distinct key values in your DataFrame.
* **Null values** are not written to Redis hashes, see [Null values](dataframe.md#null-values).
* **`ttl`**: if you set the `ttl` option or argument, the keys expire after that many seconds.
* **Errors returned by Redis inside a pipeline are not reported.** Spark-Redis writes in pipelines and doesn't check the
  individual replies, so a command that Redis rejects, for example with `OOM command not allowed when used memory > 'maxmemory'`,
  doesn't fail the Spark job. Monitor the Redis memory usage, or count the written keys after the job.
