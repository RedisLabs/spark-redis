package org.apache.spark.sql.redis.stream

import java.util.AbstractMap.SimpleEntry
import java.util.{Map => JMap}

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.streaming.ConsumerConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import redis.clients.jedis.EntryID

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class RedisSourceRdd(sc: SparkContext, redisConfig: RedisConfig,
                     consumerConf: ConsumerConfig)
  extends RDD[(String, JMap[String, String])](sc, Nil) {

  override def compute(split: Partition, context: TaskContext):
  Iterator[(String, JMap[String, String])] = {
    withConnection(redisConfig.connectionForKey(consumerConf.streamKey)) { conn =>
      val response = conn.xreadGroup(consumerConf.groupName, consumerConf.consumerName,
        consumerConf.batchSize, consumerConf.block, false,
        new SimpleEntry(consumerConf.streamKey, EntryID.UNRECEIVED_ENTRY))
      response.asScala.flatMap { e =>
        val streamEntries = e.getValue
        streamEntries.asScala.map { entry =>
          val id = entry.getID
          id.toString -> entry.getFields
        }
      }.iterator
    }
  }

  override protected def getPartitions: Array[Partition] =
    Array(RedisSourceRddPartition(0))
}
