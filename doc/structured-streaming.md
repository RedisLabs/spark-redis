### Structured Streaming

Spark-Redis supports [Redis Stream](https://redis.io/topics/streams-intro) data structure as a source for [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).
Redis Streams require Redis 5.0 or later.

The following example reads data from a Redis Stream `sensors` that has two fields `sensor-id` and `temperature`: 

```scala
val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()

val sensors = spark
      .readStream
      .format("redis")                          // read from Redis
      .option("stream.keys", "sensors")         // stream key
      .schema(StructType(Array(                 // stream fields 
        StructField("sensor-id", StringType),
        StructField("temperature", FloatType)
      )))
      .load()

val query = sensors
  .writeStream
  .format("console")
  .start()

query.awaitTermination()

```

You can write the following items to the stream to test it:

```
xadd sensors * sensor-id 1 temperature 28.1
xadd sensors * sensor-id 2 temperature 30.5
xadd sensors * sensor-id 1 temperature 28.3
```

### Output to Redis

There is no Redis Sink available, but you can leverage [`foreachBatch`](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch) and [DataFrame](dataframe.md) write command to output 
stream into Redis. Please note, `foreachBatch` is only available starting from Spark 2.4.0.

```scala
val query = sensors
  .writeStream
  .outputMode("update")
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    batchDF
      .write
      .format("org.apache.spark.sql.redis")
      .option("table", "output")
      .mode(SaveMode.Append)
      .save()
  }
  .start()

query.awaitTermination()
``` 

After writing the following to the Redis Stream:
```
xadd sensors * sensor-id 1 temperature 28.1
xadd sensors * sensor-id 2 temperature 30.5
xadd sensors * sensor-id 1 temperature 28.3
```

there will be the output `keys output:*`:
```
1) "output:b1682af092b9467cb13cfdcf7fcc9835"
2) "output:04c80769320f4edeadcce8381a6f834d"
3) "output:4f04070a2fd548fdbea441b694c8673b"
```

`hgetall output:b1682af092b9467cb13cfdcf7fcc9835`:

```
1) "sensor-id"
2) "2"
3) "temperature"
4) "30.5"
```

Please refer to [DataFrame docs](dataframe.md) for different options (such as specifying key name) available for writing.

### Stream Offset

Spark-Redis keeps track of a query's position in a consumer group (`spark-source` by default, see `stream.group.name`
below). When the group doesn't exist yet, Spark-Redis creates it at the latest entry of the stream, so the query
reads only the entries added after it starts. If the group already exists, for example from a previous run of the
query, the query continues from the group's position.

If you need to start from the specific position in the stream, specify the `stream.offsets` parameter as a JSON string.
The group is then set to that position every time the query starts.
In the following example we set offset id to be `1548083485360-0`. The group name `spark-source` is the default consumer group that Spark-Redis automatically creates to read stream
(use your own name if you set `stream.group.name`).

```scala
val offsets = """{"offsets":{"sensors":{"groupName":"spark-source","offset":"1548083485360-0"}}}"""

...

  .option("stream.offsets", offsets)
```

If you want to process the stream from the beginning, set offset id to `0-0`. 

### Entry id column

You can access stream entry id by adding a column `_id` to the stream schema:

```scala
val sensors = spark
      .readStream
      .format("redis")                          
      .option("stream.keys", "sensors")         
      .schema(StructType(Array(                 
        StructField("_id", StringType),         // entry id
        StructField("sensor-id", StringType),
        StructField("temperature", FloatType)
      )))
      .load()
```

The stream schema:


```
+---------------+---------+-----------+
|            _id|sensor-id|temperature|
+---------------+---------+-----------+
|1548083485360-0|        1|       28.1|
|1548083486248-0|        2|       30.5|
|1548083486944-0|        1|       28.3|
+---------------+---------+-----------+

```

### Level of Parallelism

By default each Redis stream key is read by a single Spark partition. There are two options available for increasing the level of parallelism.

The first approach is to create stream from multiple Redis keys. You can specify multiple keys separated by comma, e.g. 
`.option("stream.keys", "sensors-eu,sensors-us")`. In this case data from each key will be mapped to a Spark partition.
Please note, item ordering will be preserved only within a particular Redis key (Spark partition), there is no ordering guarantees for items across different keys.

With the second approach you can read data from a single Redis key with several partitions in parallel, e.g.
`option("stream.parallelism", 4)`. The entries of each micro-batch are split into that many consecutive ranges by the
time part of their IDs (the milliseconds before the `-`), and each range is read by its own Spark partition. Entries
whose IDs have the same time part always end up in the same partition, so streams with explicit IDs such as `0-1`, `0-2`
don't benefit from this option. Ordering is preserved within a partition only.

### Connection options

Similarly to Dataframe API, we can override connection options on the individual stream level, using following options passed to `spark.readStream`:

| Name       | Description                                                  | Type       | Default     |
| -----------| -------------------------------------------------------------| ---------- | ----------- |
| host       | overrides `spark.redis.host` configured in SparkSession      | `String`   | `localhost` |
| port       | overrides `spark.redis.port` configured in SparkSession      | `Int`      | `6379`      |
| user       | overrides `spark.redis.user` configured in SparkSession      | `String`   | -           |
| password   | overrides `spark.redis.password` configured in SparkSession  | `String`   | -           |
| auth       | **deprecated**, use `password`                               | `String`   | -           |
| ssl        | overrides `spark.redis.ssl` configured in SparkSession       | `Boolean`  | `false`     |
| dbNum      | overrides `spark.redis.db` configured in SparkSession        | `Int`      | `0`         |
| timeout    | overrides `spark.redis.timeout` configured in SparkSession   | `Int`      | `2000`      |

Each option overrides only its own parameter, see [Configuration](configuration.md#overriding-the-connection-for-a-dataframe-stream-or-rdd).


### Other configuration

Spark-Redis automatically creates a consumer group with name `spark-source` if it doesn't exist. You can customize the consumer group name with
`.option("stream.group.name", "my-group")`. The group records the progress of the query: after each micro-batch,
Spark-Redis sets the group's last delivered ID to the end of the batch, so `XINFO GROUPS` shows how far the query got,
and a query started without a checkpoint continues from there. The entries themselves are read with `XRANGE`, so no
consumers appear in the group and entries are not added to its pending entries list. Give each query its own group
name, otherwise the queries overwrite each other's progress.

Another option you can configure is `stream.read.batch.size`, the maximum number of entries read by one `XRANGE` call.
The default value is 100 items.

```scala
  .option("stream.read.batch.size", 200)   // items
```

Note that `stream.read.batch.size` is the `COUNT` of each `XRANGE` call, not a limit on the size of a micro-batch. At
each trigger, Spark-Redis takes the last entry ID of the stream as the end offset and reads all the entries up to it, with
as many `XRANGE` calls as needed. A micro-batch therefore contains everything that was added to the stream since the
previous one. There is currently no option to cap the number of entries per micro-batch.

The `stream.read.block` and `stream.consumer.prefix` options of earlier versions are no longer used.

### Data types

Stream entry fields are strings, and they are converted according to the schema. The following types are supported:
`String`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Boolean`, `Date` (`yyyy-mm-dd`) and `Timestamp`.

A `Timestamp` field accepts `yyyy-mm-dd hh:mm:ss[.fffffffff]`, an ISO-8601 local date-time such as
`2022-10-14T13:33:15.012345`, or an ISO-8601 date-time with an offset such as `2022-10-14T13:33:15Z` or
`2022-10-14T13:33:15+02:00`. Values without an offset are interpreted in the JVM default time zone, not in
`spark.sql.session.timeZone`.

For other formats, read the field as `StringType` and convert it with `to_date()` or `to_timestamp()`:

```scala
import org.apache.spark.sql.functions.{col, to_timestamp}

val events = spark
  .readStream
  .format("redis")
  .option("stream.keys", "events")
  .schema(StructType(Array(
    StructField("time", StringType),
    StructField("event", StringType)
  )))
  .load()
  .withColumn("time", to_timestamp(col("time"), "dd/MM/yyyy HH:mm:ss"))
```

### Fault Tolerance Semantics

Spark-Redis provides a replayable source, so by enabling [checkpointing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing) and using 
idempotent sinks, one can ensure end-to-end exactly-once semantics under any failure. If checkpointing is not enabled, it is possible that you will lose messages.

Each micro-batch reads a fixed range of entry IDs, so it returns the same entries every time it is computed: when a
task is retried, when a `foreachBatch` function uses the batch DataFrame more than once, and when the query restarts
from its checkpoint and runs a batch again. The start of the very first batch is saved in the checkpoint too. Entries
that are deleted or trimmed from the stream (`XDEL`, `XTRIM`, `MAXLEN`) before a batch is read again can't be returned. 
