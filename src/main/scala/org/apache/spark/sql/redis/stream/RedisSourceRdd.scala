package org.apache.spark.sql.redis.stream

import java.util.AbstractMap.SimpleEntry
import java.util.{List => JList, Map => JMap}

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.StreamUtils.{EntryIdEarliest, createConsumerGroupIfNotExist}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.redis.stream.RedisSourceRdd.{EntryK, RddEntry, RddIterator, StreamK}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import redis.clients.jedis.{EntryID, Jedis, StreamEntry}

import scala.collection.JavaConverters._

/**
  * RDD of EntryID -> StreamEntry.fields
  *
  * @author The Viet Nguyen
  */
class RedisSourceRdd(sc: SparkContext, redisConfig: RedisConfig,
                     offsetRanges: Seq[RedisSourceOffsetRange]) extends RDD[RddEntry](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): RddIterator = {
    val partition = split.asInstanceOf[RedisSourceRddPartition]
    val offsetRange = partition.offsetRange
    val streamKey = offsetRange.streamKey
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      val start = offsetRange.start.map(new EntryID(_)).getOrElse(EntryIdEarliest)
      createConsumerGroupIfNotExist(conn, streamKey, offsetRange.groupName, start)
      unreadMessages(conn, offsetRange)
    }
  }

  private def unreadMessages(conn: Jedis, offsetRange: RedisSourceOffsetRange): RddIterator =
    messages(conn, offsetRange) {
      val startEntry = new SimpleEntry(offsetRange.streamKey, EntryID.UNRECEIVED_ENTRY)
      Stream.continually {
        val streamGroup = conn
          .xreadGroup(offsetRange.groupName, "consumer-123", 1000, 100, false, startEntry)
        None -> streamGroup
      }
    }

  private def messages(conn: Jedis, offsetRange: RedisSourceOffsetRange)
                      (streamGroups: => StreamK): RddIterator = {
    val end = new EntryID(offsetRange.end)
    import scala.math.Ordering.Implicits._
    streamGroups
      .takeWhile { case (_, response) =>
        !response.isEmpty
      }
      .flatMap { case (min, response) =>
        response.asScala.iterator.map { entry =>
          min -> entry
        }
      }
      .flatMap { case (min, streamEntry) =>
        flattenRddEntry(streamEntry, min)
      }
      .takeWhile { case (entryId, _) =>
        entryId <= end
      }
      .iterator
  }

  private def flattenRddEntry(entry: EntryK, min: Option[EntryID]): RddIterator = {
    import scala.math.Ordering.Implicits._
    entry.getValue.asScala.iterator
      .filter { streamEntry =>
        min.isEmpty || streamEntry.getID >= min.get
      }
      .map { streamEntry =>
        streamEntry.getID -> streamEntry.getFields
      }
  }

  override protected def getPartitions: Array[Partition] =
    offsetRanges.zipWithIndex.map { case (e, i) => RedisSourceRddPartition(i, e) }
      .toArray
}

object RedisSourceRdd {

  type RddEntry = (EntryID, JMap[String, String])
  type RddIterator = Iterator[RddEntry]
  type EntryK = JMap.Entry[String, JList[StreamEntry]]
  type StreamK = Stream[(Option[EntryID], JList[EntryK])]
}

case class RedisSourceRddPartition(index: Int, offsetRange: RedisSourceOffsetRange)
  extends Partition
