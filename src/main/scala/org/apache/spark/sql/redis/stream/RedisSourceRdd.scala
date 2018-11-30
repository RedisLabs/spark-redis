package org.apache.spark.sql.redis.stream

import java.util.{Map => JMap}

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import redis.clients.jedis.EntryID

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class RedisSourceRdd(sc: SparkContext, redisConfig: RedisConfig,
                     offsetRange: RedisSourceOffsetRange)
  extends RDD[(String, JMap[String, String])](sc, Nil) {

  override def compute(split: Partition, context: TaskContext):
  Iterator[(String, JMap[String, String])] = {
    val streamKey = offsetRange.streamKey
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      conn.xrange(streamKey, new EntryID(0, 0), new EntryID(offsetRange.end), Int.MaxValue).asScala
        .map { entry =>
          val id = entry.getID
          id.toString -> entry.getFields
        }.iterator
    }
  }

  override protected def getPartitions: Array[Partition] =
    Array(RedisSourceRddPartition(0))
}
