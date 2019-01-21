package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.redis.stream.RedisSourceTypes.StreamEntry
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * RDD of EntryID -> StreamEntry.fields
  *
  * @author The Viet Nguyen
  */
class RedisSourceRdd(sc: SparkContext, redisConfig: RedisConfig,
                     offsetRanges: Seq[RedisSourceOffsetRange], autoAck: Boolean = true)
  extends RDD[StreamEntry](sc, Nil) {

  private val streamReader = new RedisStreamReader()

  override def compute(split: Partition, context: TaskContext): Iterator[StreamEntry] = {
    val partition = split.asInstanceOf[RedisSourceRddPartition]
    val offsetRange = partition.offsetRange
    val consumerConfig = offsetRange.config
    val streamKey = consumerConfig.streamKey
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      // note, we cannot easily use Iterators with jedis connection pool because there might be concurrency issues.
      // So we read a materialized List and convert to Iterator below
      if (offsetRange.start.isDefined) {
        // offset is defined, read by offset
        streamReader.streamEntriesByOffset(conn, offsetRange).iterator
      } else {
        // offset is no defined, happens for the first batch or after spark restart
        // read starting from where the point the consumer group ended
        streamReader.unreadStreamEntries(conn, offsetRange).iterator
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (e, i) => RedisSourceRddPartition(i, e) }
      .toArray
  }
}

case class RedisSourceRddPartition(index: Int, offsetRange: RedisSourceOffsetRange)
  extends Partition
