package org.apache.spark.sql.redis.stream

import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.{Map => JMap}

import com.redislabs.provider.redis.util.Logging
import org.apache.spark.sql.redis.stream.RedisSourceTypes.{StreamEntry, StreamEntryBatch, StreamEntryBatches}
import redis.clients.jedis.{EntryID, Jedis}

import scala.collection.JavaConverters._
import scala.math.Ordering.Implicits._

/**
  * TODO: iterator
  *
  * @author The Viet Nguyen
  */
class RedisStreamReader extends Logging with Serializable {

  def streamEntriesByOffset(conn: Jedis, offsetRange: RedisSourceOffsetRange): List[StreamEntry] = {
    val config = offsetRange.config

    logInfo(s"Reading stream entries with given offset " +
      s"[${config.streamKey}, ${config.groupName}, ${config.consumerName} ${offsetRange.start}]...")

    filterStreamEntries(conn, offsetRange) {
      val initialStart = offsetRange.start.map(id => new EntryID(id)).getOrElse(throw new RuntimeException("Offset start is not set"))
      val initialEntry = new SimpleEntry(config.streamKey, initialStart)
      Iterator.iterate(readStreamEntryBatches(conn, offsetRange, initialEntry)) { response =>
        val responseOption = for {
          lastEntries <- response.asScala.lastOption
          lastEntry <- lastEntries.getValue.asScala.lastOption
          lastEntryId = lastEntry.getID
          startEntryId = new EntryID(lastEntryId.getTime, lastEntryId.getSequence)
          startEntryOffset = new SimpleEntry(config.streamKey, startEntryId)
        } yield readStreamEntryBatches(conn, offsetRange, startEntryOffset)
        responseOption.getOrElse(new util.ArrayList)
      }
    }
  }

  def unreadStreamEntries(conn: Jedis, offsetRange: RedisSourceOffsetRange): List[StreamEntry] = {
    val config = offsetRange.config

    logInfo(s"Reading unread stream entries " +
      s"[${config.streamKey}, ${config.groupName}, ${config.consumerName}]... ")

    val res = filterStreamEntries(conn, offsetRange) {
      val startEntryOffset = new SimpleEntry(config.streamKey, EntryID.UNRECEIVED_ENTRY)
      Iterator.continually {
        readStreamEntryBatches(conn, offsetRange, startEntryOffset)
      }
    }
    res
  }

  private def readStreamEntryBatches(conn: Jedis, offsetRange: RedisSourceOffsetRange,
                                     startEntryOffset: JMap.Entry[String, EntryID]): StreamEntryBatches = {
    val config = offsetRange.config
    // we don't need acknowledgement, if spark processing fails, it will request the same batch again
    val noAck = true
    val response = conn.xreadGroup(config.groupName, config.consumerName, config.batchSize, config.block, noAck, startEntryOffset)
    logDebug(s"Got entries: $response")
    response
  }

  private def filterStreamEntries(conn: Jedis, offsetRange: RedisSourceOffsetRange)
                                 (streamGroups: => Iterator[StreamEntryBatches]): List[StreamEntry] = {
    val end = new EntryID(offsetRange.end)
    streamGroups
      .takeWhile { response =>
        !response.isEmpty
      }
      .flatMap { response =>
        response.asScala.iterator
      }
      .flatMap { streamEntry =>
        flattenStreamEntries(streamEntry)
      }
      .takeWhile { case (entryId, _) =>
        entryId <= end
      }
      // convert to List to avoid an issue of concurrently using the same redis connection bound to
      // different RDD partitions (iterators)
      .toList
  }

  private def flattenStreamEntries(entry: StreamEntryBatch): Iterator[StreamEntry] = {
    entry.getValue.asScala.iterator
      .map { streamEntry =>
        streamEntry.getID -> streamEntry.getFields
      }
  }
}
