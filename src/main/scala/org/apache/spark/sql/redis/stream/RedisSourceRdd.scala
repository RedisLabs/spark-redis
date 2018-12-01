package org.apache.spark.sql.redis.stream

import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry
import java.util.{List => JList, Map => JMap}

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.StreamUtils.createConsumerGroupIfNotExist
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import redis.clients.jedis.{EntryID, StreamEntry}

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
    val streams = new SimpleEntry(streamKey, EntryID.UNRECEIVED_ENTRY)
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      val start = offsetRange.start.orElse(Some("0-0")).map(new EntryID(_)).get
      createConsumerGroupIfNotExist(conn, streamKey, "group55", start)
      conn.xreadGroup("group55", "consumer-123", 1000, 100, false, streams)
        .asScala
        .flatMap {
          flattenRddEntry
        }.iterator
    }
  }

  private def flattenRddEntry(entry: Entry[String, JList[StreamEntry]]):
  Seq[(String, JMap[String, String])] = {
    entry.getValue.asScala
      .map { streamEntry =>
        val id = streamEntry.getID
        id.toString -> streamEntry.getFields
      }
  }

  override protected def getPartitions: Array[Partition] =
    Array(RedisSourceRddPartition(0))
}
