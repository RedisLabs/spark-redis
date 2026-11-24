# Configuration

  - [Spark configuration parameters](#spark-configuration-parameters)
  - [When the configuration is read](#when-the-configuration-is-read)
  - [Overriding the connection for a DataFrame, stream or RDD](#overriding-the-connection-for-a-dataframe-stream-or-rdd)
  - [Authentication](#authentication)
    - [Keeping the password out of the Spark UI and logs](#keeping-the-password-out-of-the-spark-ui-and-logs)
  - [TLS](#tls)
  - [Connection pool](#connection-pool)
  - [Redis server requirements](#redis-server-requirements)
  - [Tuning reads and writes](#tuning-reads-and-writes)

## Spark configuration parameters

| Parameter                                      | Description                                                                                                                                                                 | Default     |
|------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `spark.redis.host`                             | host or IP of the initial node. In cluster mode the connector reads the cluster topology from this node, so there is no need to list the other nodes.                     | `localhost` |
| `spark.redis.port`                             | the initial node's TCP port                                                                                                                                                 | `6379`      |
| `spark.redis.user`                             | ACL user name sent with `AUTH` (Redis 6+). Leave it unset to authenticate with a password only.                                                                             | -           |
| `spark.redis.password`                         | password sent with `AUTH`. Leave it unset if the server has no password. Spark hides its value in the Spark UI and event logs, see [Keeping the password out of the Spark UI and logs](#keeping-the-password-out-of-the-spark-ui-and-logs) | -           |
| `spark.redis.auth`                             | **deprecated**, use `spark.redis.password`. Read only when `spark.redis.password` is unset. Spark doesn't hide its value                                                    | -           |
| `spark.redis.db`                               | DB number. Avoid using this, especially in cluster mode.                                                                                                                    | `0`         |
| `spark.redis.timeout`                          | connection timeout **and** socket read timeout, in milliseconds                                                                                                             | `2000`      |
| `spark.redis.ssl`                              | set to `true` to connect with TLS, see [TLS](#tls)                                                                                                                          | `false`     |
| `spark.redis.max.pipeline.size`                | the maximum number of commands sent in one pipeline, see [Tuning](#tuning-reads-and-writes)                                                                                 | `100`       |
| `spark.redis.scan.count`                       | the `COUNT` hint passed to `SCAN` when iterating over keys, see [Tuning](#tuning-reads-and-writes)                                                                          | `100`       |
| `spark.redis.rdd.write.iterator.grouping.size` | the number of items `toRedisKV`, `toRedisByteKV` and `toRedisByteHASHes` take from a partition at a time before grouping them by Redis node, see [Tuning](#tuning-reads-and-writes) | `1000`      |
| `spark.redis.client.name`                      | optional client name. Every connection the connector opens, on the driver, on the executors and to every cluster node, sets it with `CLIENT SETNAME`, so `CLIENT LIST` shows it as `name=...`. Printable ASCII without spaces only; any other value fails on the driver before a connection opens. Unset or empty (the default) leaves connections unnamed |

## When the configuration is read

Spark-Redis reads the `spark.redis.*` parameters from the `SparkConf` of the `SparkContext`. They must be set **before the
`SparkContext` is created**, for example:

* with `--conf` on `spark-submit`, `spark-shell` or `pyspark`,
* with `SparkConf.set(...)` before creating the `SparkContext`,
* with `SparkSession.builder().config(...)` when the builder creates a new session.

Settings applied later are not seen by Spark-Redis, and the connector silently falls back to `localhost:6379`:

* `spark.conf.set("spark.redis.host", ...)` changes the session's runtime SQL configuration, not the `SparkConf`.
* If a `SparkContext` already exists, `SparkSession.builder().config(...).getOrCreate()` returns a session on top of it and
  does not change its `SparkConf`. This is always the case in `spark-shell`, `pyspark` and notebooks (Databricks, Zeppelin,
  Jupyter, EMR notebooks). In these environments, put the parameters in the cluster or interpreter Spark configuration, or
  pass the connection settings as [DataFrame or stream options](#overriding-the-connection-for-a-dataframe-stream-or-rdd).

## Overriding the connection for a DataFrame, stream or RDD

The DataFrame API (`spark.read`, `df.write`, Spark SQL `OPTIONS`) and Structured Streaming (`spark.readStream`) accept the
following options. Note that the names don't have the `spark.redis.` prefix: an option such as `.option("spark.redis.password", ...)`
is ignored.

| Option    | Overrides             | Type      |
|-----------|-----------------------|-----------|
| `host`    | `spark.redis.host`    | `String`  |
| `port`    | `spark.redis.port`    | `Int`     |
| `user`    | `spark.redis.user`    | `String`  |
| `password` | `spark.redis.password` | `String` |
| `auth`    | **deprecated**, use `password`. Read only when `password` is unset | `String` |
| `dbNum`   | `spark.redis.db`      | `Int`     |
| `timeout` | `spark.redis.timeout` | `Int`     |
| `ssl`     | `spark.redis.ssl`     | `Boolean` |
| `client.name` | `spark.redis.client.name` | `String` |

Each option overrides only its own parameter. Options that aren't set fall back to the `spark.redis.*` value from the
`SparkConf`, and then to the default. An empty `client.name` leaves that source's connections unnamed.

```scala
val df = spark.read
  .format("org.apache.spark.sql.redis")
  .option("host", "redis.example.com")
  .option("port", "6380")
  .option("password", "passwd")
  .option("ssl", "true")
  .option("table", "person")
  .load()
```

The RDD and DStream functions take an implicit `RedisConfig`, see [Connecting to Multiple Redis Clusters/Instances](cluster.md).
A `RedisEndpoint` can also be created from a Redis URI:

```scala
// redis://[user:password@]host:port[/db], use the rediss:// scheme for TLS
implicit val redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint("rediss://user:passwd@redis.example.com:6380/0"))
```

## Authentication

* Password only (`requirepass`, or the `default` user): set `spark.redis.password` (or the `password` option).
* ACL user (Redis 6+): set both `spark.redis.user` and `spark.redis.password` (or the `user` and `password` options).
* No authentication: leave both unset. Don't set them to an empty string. An empty value is still sent with `AUTH`, and
  the connection fails with `WRONGPASS invalid username-password pair or user is disabled` (or a similar `AUTH` error).

### Keeping the password out of the Spark UI and logs

Spark hides the value of a setting whose name matches `spark.redaction.regex` in the Spark UI's Environment tab, the
History Server and event logs. It also hides a DataFrame or stream option whose name matches it where query plans show
options, for example in the SQL tab. The default pattern contains `password`, so `spark.redis.password` and the
`password` option are hidden.

Spark-Redis 3.1.0 and earlier read the password only from `spark.redis.auth` (`redis.auth` before 2.3.1) and the `auth`
option. These names don't match the default pattern, so the password shows in clear text wherever Spark shows the
configuration or the query plan
([GHSA-wmm3-vmjq-7mmc](https://github.com/RedisLabs/spark-redis/security/advisories/GHSA-wmm3-vmjq-7mmc)). Spark-Redis
still reads them when the new names are unset, and logs a warning.

* Rename `spark.redis.auth` to `spark.redis.password`, and the `auth` option to `password`. Remove the old name: while
  it's set, its value still shows, even if the new name is set too.
* With Spark-Redis 3.1.0 or earlier, add the old names to the pattern instead. Start from your Spark version's default,
  for example with Spark 3.5:

  ```bash
  --conf 'spark.redaction.regex=(?i)secret|password|token|access[.]?key|credential|redis[.]auth|^auth$'
  ```

* If the password was set with the old names, rotate it. Also delete the event logs that contain it.
* Authenticate as an [ACL user](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/)
  (`spark.redis.user`) that has only the permissions the job needs, so a leaked password grants as little as possible.

Spark also hides a value that matches the pattern on its own, whatever its key. A test password such as `my-secret` is
hidden even under `spark.redis.auth`, so check with a value like `hunter2`.

## TLS

Set `spark.redis.ssl` (or the `ssl` option) to `true`. Spark-Redis then uses the JVM's default `SSLSocketFactory`, which
you configure with the standard `javax.net.ssl.*` system properties:

* If the server certificate isn't signed by a CA in the JVM's default truststore, for example a self-signed or private CA,
  provide a truststore with `javax.net.ssl.trustStore`, `javax.net.ssl.trustStorePassword` and `javax.net.ssl.trustStoreType`.
* For mutual TLS (the server requires a client certificate), also provide a keystore with `javax.net.ssl.keyStore`,
  `javax.net.ssl.keyStorePassword` and `javax.net.ssl.keyStoreType`.

The properties must be set on the driver **and** on the executors, because both open connections to Redis:

```bash
spark-submit \
  --conf "spark.redis.host=redis.example.com" \
  --conf "spark.redis.port=6380" \
  --conf "spark.redis.ssl=true" \
  --files /path/to/truststore.jks,/path/to/keystore.jks \
  --conf "spark.driver.extraJavaOptions=-Djavax.net.ssl.trustStore=/path/to/truststore.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.keyStore=/path/to/keystore.jks -Djavax.net.ssl.keyStorePassword=changeit" \
  --conf "spark.executor.extraJavaOptions=-Djavax.net.ssl.trustStore=truststore.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.keyStore=keystore.jks -Djavax.net.ssl.keyStorePassword=changeit" \
  ...
```

On YARN, the files passed with `--files` are placed in each executor's working directory, so the executor options can use
a relative path. On other cluster managers, make the files available at the same path on every node.

A custom `SSLSocketFactory`, `SSLParameters` or `HostnameVerifier` can't be passed to Spark-Redis.

## Connection pool

Each Spark JVM (the driver and every executor) keeps one Jedis connection pool per Redis endpoint, with at most 250
connections and 32 idle connections. The pool size is not configurable. A task borrows a connection, runs its commands
and returns it to the pool. If the server rejects a new connection with `ERR max number of clients reached`, Spark-Redis
waits and retries, for up to 60 seconds, then fails the task with an error that names the server. Make sure the server's
`maxclients` covers the number of tasks that run concurrently.

## Redis server requirements

Besides the commands that read and write your data, Spark-Redis uses the following commands:

| Command                     | When                                                                                                                                                                 |
|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `INFO`, `INFO replication`  | on the driver, every time a connection configuration is created, to detect cluster mode and replicas                                                                 |
| `CLUSTER SLOTS`             | in cluster mode, to read the topology                                                                                                                                |
| `SCAN`                      | to find the keys that match a pattern: RDD reads with a key pattern, DataFrame reads, and DataFrame writes with `SaveMode.Overwrite`, `ErrorIfExists` (the default) and `Ignore` |
| `TYPE`                      | DataFrame reads with the `filter.keys.by.type` option                                                                                                               |
| `CLIENT SETNAME`            | on every new connection, when `spark.redis.client.name` or the `client.name` option is set |
| `ASKING`                    | before a write to a slot that is being migrated, after the server answered `ASK` |
| `MULTI`, `UNLINK`, `EXEC`   | DataFrame writes with `write.mode = replace` |
| `COMMAND INFO`, `HELLO`     | on the driver, once per write of the `binary` model with a `ttl`, to find out whether every primary has `MSETEX` and whether the server is a cluster. A refused command counts as no: the write then uses `SET ... EX` and groups the keys by hash slot |

Proxies and managed services that block or don't implement these commands are not supported. Typical errors are
`ERR unknown command 'CLUSTER'`, `ERR data connection don't process admin command` (blocked `INFO`) and
`Invalid node response` (no `SCAN`).

Each new connection also sends `CLIENT SETINFO` with the library's name and version (Redis 7.2 and later). A server
that refuses it is still supported: the refusal is ignored.

[Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/) is not supported: Spark-Redis
can't ask Sentinel for the current master. As a workaround, point `spark.redis.host` and `spark.redis.port` at the current
master, for example resolved with `SENTINEL get-master-addr-by-name <master-name>` before you start the job. A failover while
the job runs isn't handled. If `spark.redis.host` points at a replica, Spark-Redis follows `master_host` and `master_port`
from `INFO replication` and sends all reads and writes to the master.

Spark-Redis requires Redis 7.4 or later, and CI tests 7.4 and 8.6.

## Tuning reads and writes

* **`max.pipeline.size`** (`spark.redis.max.pipeline.size`, default `100`): Spark-Redis sends commands in
  [pipelines](https://redis.io/docs/latest/develop/use/pipelining/), and this is the number of commands sent before waiting
  for the replies. Larger pipelines mean fewer network round trips, which helps most when Redis is far from the executors,
  but each pipeline's commands and replies are buffered in memory. Values in the hundreds or low thousands are typical.
* **`scan.count`** (`spark.redis.scan.count`, default `100`): the `COUNT` hint of
  [`SCAN`](https://redis.io/docs/latest/commands/scan/), the approximate number of keys Redis examines per call. To find the
  keys that match a pattern, Spark-Redis scans the whole keyspace of each node, so with millions of keys a small value
  means many round trips. Raising it to 1000-10000 usually makes the key scan much faster. Each call blocks Redis a bit
  longer.
* **`iterator.grouping.size`** (DataFrame option, default `1000`): DataFrame reads and writes process a partition in
  groups of this many rows. Each group's keys are grouped by Redis node and sent in pipelines. It bounds how many rows
  are held in memory at once.
* **`partitions.number`** (DataFrame option, default `3`) and the `partitionNum` argument of the RDD functions set the
  number of read partitions. Partitions split the cluster hash slots, so they add parallelism only when the data is spread
  across many keys and nodes. All the values of a single key (for example one large hash or list) are read by one task
  with a single command (`HGETALL`, `LRANGE`, `SMEMBERS`, ...). If such a read hits `Read timed out`, raise
  `spark.redis.timeout`.
* **Write parallelism** comes from the number of partitions of the RDD or DataFrame you write. Each partition is written
  by its own task over its own connections, so `repartition` the data to write with more parallel tasks.
* **Save modes**: `SaveMode.Overwrite`, `ErrorIfExists` (the default) and `Ignore` scan for the table's existing keys
  before writing, which can take a long time on a large database. `SaveMode.Append` skips the scan. See
  [Save Modes](dataframe.md#save-modes).
