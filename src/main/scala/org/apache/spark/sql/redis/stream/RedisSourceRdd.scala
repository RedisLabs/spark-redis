package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.StreamUtils.{EntryIdEarliest, createConsumerGroupIfNotExist}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.redis.stream.RedisSourceTypes.EntryIdWithFields
import org.apache.spark.sql.redis.stream.RedisStreamReader._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import redis.clients.jedis.EntryID

/**
  * RDD of EntryID -> StreamEntry.fields
  *
  * @author The Viet Nguyen
  */
class RedisSourceRdd(sc: SparkContext, redisConfig: RedisConfig,
                     offsetRanges: Seq[RedisSourceOffsetRange])
  extends RDD[EntryIdWithFields](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[EntryIdWithFields] = {
    val partition = split.asInstanceOf[RedisSourceRddPartition]
    val offsetRange = partition.offsetRange
    val streamKey = offsetRange.streamKey
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      val start = offsetRange.start.map(new EntryID(_)).getOrElse(EntryIdEarliest)
      createConsumerGroupIfNotExist(conn, streamKey, offsetRange.groupName, start)
      pendingMessages(conn, offsetRange) ++ unreadMessages(conn, offsetRange)
    }
  }

  override protected def getPartitions: Array[Partition] =
    offsetRanges.zipWithIndex.map { case (e, i) => RedisSourceRddPartition(i, e) }
      .toArray
}

case class RedisSourceRddPartition(index: Int, offsetRange: RedisSourceOffsetRange)
  extends Partition
