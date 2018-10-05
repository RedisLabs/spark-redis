### Streaming
Spark-Redis support streaming data from Redis instance/cluster, currently streaming data are fetched from Redis' List by the `blpop` command. Users are required to provide an array which stores all the List names they are interested in. The [storageLevel](http://spark.apache.org/docs/latest/streaming-programming-guide.html#data-serialization) is `MEMORY_AND_DISK_SER_2` by default, you can change it on your demand.
`createRedisStream` will create a `(listName, value)` stream, but if you don't care about which list feeds the value, you can use `createRedisStreamWithoutListname` to get the only `value` stream.

Use the following to get a `(listName, value)` stream from `foo` and `bar` list

```
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import com.redislabs.provider.redis._
val ssc = new StreamingContext(sc, Seconds(1))
val redisStream = ssc.createRedisStream(Array("foo", "bar"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
redisStream.print
ssc.awaitTermination()
```


Use the following to get a `value` stream from `foo` and `bar` list

```
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import com.redislabs.provider.redis._
val ssc = new StreamingContext(sc, Seconds(1))
val redisStream = ssc.createRedisStreamWithoutListname(Array("foo", "bar"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
redisStream.print
ssc.awaitTermination()
```
