### Structured Streaming

Spark-Redis supports [Redis Stream](https://redis.io/topics/streams-intro) data structure as a source for [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html):

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

Please refer to [DataFrame docs](dataframe.md) for different options(such as specifying key name) available for writing .

### Stream Offset

By default it pulls messages starting from the latest message in the stream. If you need to start from the specific position in the stream, specify the `stream.offsets` parameter as a JSON string. 
In the following example we set offset id to be `1548083485360-0`. The group name `redis-source` is a default consumer group that Spark-Redis automatically creates to read stream.

```scala
val offsets = """{"offsets":{"sensors":{"groupName":"redis-source","offset":"1548083485360-0"}}}"""

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

By default Spark-Redis creates a consumer group with a single consumer. There are two options available for increasing the level of parallelism.

The first approach is to create stream from multiple Redis keys. You can specify multiple keys separated by comma, e.g. 
`.option("stream.keys", "sensors-eu,sensors-us")`. In this case data from each key will be mapped to a Spark partition.
Please note, item ordering will be preserved only within a particular Redis key (Spark partition), there is no ordering guarantees for items across different keys.

With the second approach you can read data from a single Redis key with multiple consumers in parallel, e.g. `option("stream.parallelism", 4)`. Each consumer will be mapped to a Spark partition. There are no ordering guarantees in this case.

### Other configuration

Spark-Redis automatically creates a consumer group with name `spark-source` if it doesn't exist. You can customize the consumer group name with
`.option("stream.group.name", "my-group")`. Also you can customize the name of consumers in consumer group with `.option("stream.consumer.prefix", "my-consumer")`.

Other options you can configure are `stream.read.batch.size` and `stream.read.block`. They define the maximum number of pulled items and time in milliseconds to wait in a `XREADGROUP` call. 
The default values are 100 items and 500 ms, respectively.

```scala
  .option("stream.read.batch.size", 200)   // items
  .option("stream.read.block", 1000)       // in milliseconds
```

### Fault Tolerance Semantics

Spark-Redis provides a replayable source, so by enabling [checkpointing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing) and using 
idempotent sinks, one can ensure end-to-end exactly-once semantics under any failure. If checkpointing is not enabled, it is possible that you will lose messages. 
