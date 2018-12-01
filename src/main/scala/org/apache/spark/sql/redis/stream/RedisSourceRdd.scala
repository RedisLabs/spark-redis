package org.apache.spark.sql.redis.stream

import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry
import java.util.{List => JList, Map => JMap}

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.StreamUtils
import com.redislabs.provider.redis.util.StreamUtils.createConsumerGroupIfNotExist
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import redis.clients.jedis.{EntryID, Jedis, StreamEntry}

import scala.collection.JavaConverters._

/**
  * RDD of EntryID -> StreamEntry.fields
  *
  * @author The Viet Nguyen
  */
class RedisSourceRdd(sc: SparkContext, redisConfig: RedisConfig,
                     offsetRange: RedisSourceOffsetRange)
  extends RDD[(EntryID, JMap[String, String])](sc, Nil) {

  override def compute(split: Partition, context: TaskContext):
  Iterator[(EntryID, JMap[String, String])] = {
    val streamKey = offsetRange.streamKey
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      val start = offsetRange.start.map(new EntryID(_)).getOrElse(StreamUtils.EntryIdEarliest)
      val end = new EntryID(offsetRange.end)
      createConsumerGroupIfNotExist(conn, streamKey, "group55", start)
      unreadMessages(conn, streamKey, end)
    }
  }

  private def unreadMessages(conn: Jedis, streamKey: String, end: EntryID):
  Iterator[(EntryID, JMap[String, String])] = {
    val unreadEntry = new SimpleEntry(streamKey, EntryID.UNRECEIVED_ENTRY)
    import scala.math.Ordering.Implicits._
    Stream.continually {
      conn.xreadGroup("group55", "consumer-123", 1000, 100, false, unreadEntry)
    }
      .takeWhile { response =>
        !response.isEmpty
      }
      .flatMap {
        _.asScala.iterator
      }
      .flatMap {
        flattenRddEntry
      }
      .takeWhile { entry =>
        entry._1 <= end
      }
      .iterator
  }

  private def flattenRddEntry(entry: Entry[String, JList[StreamEntry]]):
  Iterator[(EntryID, JMap[String, String])] = {
    entry.getValue.asScala
      .map { streamEntry =>
        streamEntry.getID -> streamEntry.getFields
      }
      .iterator
  }

  override protected def getPartitions: Array[Partition] =
    Array(RedisSourceRddPartition(0, offsetRange))
}

case class RedisSourceRddPartition(index: Int, offsetRange: RedisSourceOffsetRange)
  extends Partition
