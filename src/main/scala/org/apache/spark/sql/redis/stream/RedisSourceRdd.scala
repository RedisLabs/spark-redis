package org.apache.spark.sql.redis.stream

import java.util
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
      pendingMessages(conn, offsetRange) ++ unreadMessages(conn, offsetRange)
    }
  }

  private def pendingMessages(conn: Jedis, offsetRange: RedisSourceOffsetRange): RddIterator = {
    logInfo("Reading pending stream entries...")
    messages(conn, offsetRange) {
      val initialStart = offsetRange.start.map(id => new EntryID(id))
        .getOrElse(new EntryID(0, 0))
      val initialEntry = new SimpleEntry(offsetRange.streamKey, initialStart)
      Stream.iterate(xreadGroup(conn, offsetRange, initialEntry)) { response =>
        val responseOption = for {
          lastEntries <- response.asScala.lastOption
          lastEntry <- lastEntries.getValue.asScala.lastOption
          lastEntryId = lastEntry.getID
          startEntryId = new EntryID(lastEntryId.getTime, lastEntryId.getSequence + 1)
          startEntryOffset = new SimpleEntry(offsetRange.streamKey, startEntryId)
        } yield {
          xreadGroup(conn, offsetRange, startEntryOffset)
        }
        responseOption.getOrElse(new util.ArrayList)
      }
    }
  }

  private def unreadMessages(conn: Jedis, offsetRange: RedisSourceOffsetRange): RddIterator = {
    logInfo("Reading unread stream entries...")
    messages(conn, offsetRange) {
      val startEntryOffset = new SimpleEntry(offsetRange.streamKey, EntryID.UNRECEIVED_ENTRY)
      Stream.continually {
        xreadGroup(conn, offsetRange, startEntryOffset)
      }
    }
  }

  private def xreadGroup(conn: Jedis, offsetRange: RedisSourceOffsetRange,
                         startEntryOffset: JMap.Entry[String, EntryID]): JList[EntryK] = conn
    .xreadGroup(offsetRange.groupName, "consumer-123", 1000, 100, false, startEntryOffset)

  private def messages(conn: Jedis, offsetRange: RedisSourceOffsetRange)
                      (streamGroups: => StreamK): RddIterator = {
    val end = new EntryID(offsetRange.end)
    import scala.math.Ordering.Implicits._
    streamGroups
      .takeWhile { response =>
        !response.isEmpty
      }
      .flatMap { response =>
        response.asScala.iterator
      }
      .flatMap { streamEntry =>
        flattenRddEntry(streamEntry)
      }
      .takeWhile { case (entryId, _) =>
        entryId <= end
      }
      .iterator
  }

  private def flattenRddEntry(entry: EntryK): RddIterator = {
    entry.getValue.asScala.iterator
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
  type StreamK = Stream[JList[EntryK]]
}

case class RedisSourceRddPartition(index: Int, offsetRange: RedisSourceOffsetRange)
  extends Partition
