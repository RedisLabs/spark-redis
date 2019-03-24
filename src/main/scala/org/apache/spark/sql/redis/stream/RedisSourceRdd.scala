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

  override def compute(split: Partition, context: TaskContext): Iterator[StreamEntry] = {
    val partition = split.asInstanceOf[RedisSourceRddPartition]
    val offsetRange = partition.offsetRange
    val streamReader = new RedisStreamReader(redisConfig)
    streamReader.unreadStreamEntries(offsetRange)
  }

  override protected def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (e, i) => RedisSourceRddPartition(i, e) }
      .toArray
  }
}

case class RedisSourceRddPartition(index: Int, offsetRange: RedisSourceOffsetRange)
  extends Partition
