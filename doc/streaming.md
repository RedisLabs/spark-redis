### Streaming

Spark-Redis supports streaming data from Stream and List data structures:

  - [Redis Stream](#redis-stream)
  - [Redis List](#redis-list)


## Redis Stream

To stream data from [Redis Stream](https://redis.io/topics/streams-intro) use `createRedisXStream` method:

```scala
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.streaming.{ConsumerConfig, StreamItem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

val spark = SparkSession.builder.appName("Redis Stream Example")
  .master("local[*]")
  .config("spark.redis.host", "localhost")
  .config("spark.redis.port", "6379")
  .getOrCreate()

val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

val stream = ssc.createRedisXStream(Seq(ConsumerConfig("my-stream", "my-consumer-group", "my-consumer-1")))
stream.print()

ssc.start()
ssc.awaitTermination()

```

It will automatically create a consumer group if it doesn't exist and will start listening for the messages in the stream. 

By default it pulls messages starting from the latest message. If you need to start from the earliest message or any specific position in the stream, specify the `offset` parameter:

```scala
ConsumerConfig("my-stream", "my-consumer-group", "my-consumer-1", offset = Earliest) // start from '0-0'
ConsumerConfig("my-stream", "my-consumer-group", "my-consumer-1", IdOffset(42, 0))   // start from '42-0'
```

Please note, spark-redis will attempt to create a consumer group with the specified offset, but if the consumer group already exists, 
it will use the existing offset. It means, for example, if you decide to re-process all the messages from the beginning, 
just changing the offset to `Earliest` may not be enough. You may need to either manually delete the consumer 
group with `XGROUP DESTROY` or modify the offset with `XGROUP SETID`.

The DStream is implemented with a [Reliable Receiver](https://spark.apache.org/docs/latest/streaming-custom-receivers.html#receiver-reliability) that acknowledges 
after the data has been stored in Spark. As with any other Receiver to achieve strong fault-tolerance guarantees and ensure zero data loss, you have to enable [write-ahead logs](https://spark.apache.org/docs/latest/streaming-programming-guide.html#deploying-applications). 

The received data is stored with `StorageLevel.MEMORY_AND_DISK_2` by default. 
Storage level can be configured with `storageLevel` parameter, e.g.:
```scala
ssc.createRedisXStream(conf, storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
```


### Level of Parallelism

The `createRedisXStream()` takes a sequence of consumer configs, each consumer is started in a separate thread. This allows you, for example, to
create a stream from multiple Redis Stream keys:

```scala
ssc.createRedisXStream(Seq(
  ConsumerConfig("my-stream-1", "my-consumer-group-1", "my-consumer-1"),
  ConsumerConfig("my-stream-2", "my-consumer-group-2", "my-consumer-1")
))
```

In this example we created an input DStream that corresponds to a single receiver running in a Spark executor. The receiver will create two threads pulling 
data from the streams in parallel. However if the data receiving becomes a bottleneck, you may want to start multiple receivers in different executors (worker machines).
This can be achieved by creating multiple input DStreams and `union` them together. You can read more about about it [here](https://spark.apache.org/docs/latest/streaming-programming-guide.html#level-of-parallelism-in-data-receiving).

For example, the following will create two receivers pulling the data from `my-stream` and balancing the load:  

```scala
val streams = Seq(
  ssc.createRedisXStream(Seq(ConsumerConfig("my-stream", "my-consumer-group", "my-consumer-1"))),
  ssc.createRedisXStream(Seq(ConsumerConfig("my-stream", "my-consumer-group", "my-consumer-2")))
)

val stream = ssc.union(streams)
stream.print()
```


## Redis List

The stream can be also created from Redis' List, the data is fetched with the `blpop` command. Users are required to provide an array which stores all the List names they are interested in. The [storageLevel](http://spark.apache.org/docs/latest/streaming-programming-guide.html#data-serialization) is `MEMORY_AND_DISK_SER_2` by default, you can change it on your demand.

The method `createRedisStream` will create a `(listName, value)` stream, but if you don't care about which list feeds the value, you can use `createRedisStreamWithoutListname` to get the only `value` stream.

Use the following to get a `(listName, value)` stream from `foo` and `bar` list

```scala
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import com.redislabs.provider.redis._
val ssc = new StreamingContext(sc, Seconds(1))
val redisStream = ssc.createRedisStream(Array("foo", "bar"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
redisStream.print()
ssc.awaitTermination()
```


Use the following to get a `value` stream from `foo` and `bar` list

```scala
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import com.redislabs.provider.redis._
val ssc = new StreamingContext(sc, Seconds(1))
val redisStream = ssc.createRedisStreamWithoutListname(Array("foo", "bar"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
redisStream.print()
ssc.awaitTermination()
```
