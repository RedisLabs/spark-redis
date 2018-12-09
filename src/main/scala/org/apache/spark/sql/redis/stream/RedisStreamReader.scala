package org.apache.spark.sql.redis.stream

import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.{Map => JMap}

import com.redislabs.provider.redis.util.Logging
import org.apache.spark.sql.redis.stream.RedisSourceTypes.{StreamEntry, StreamEntryBatch, StreamEntryBatches}
import redis.clients.jedis.{EntryID, Jedis}

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
object RedisStreamReader extends Logging {

  def pendingMessages(conn: Jedis, offsetRange: RedisSourceOffsetRange): Iterator[StreamEntry] = {
    logInfo("Reading pending stream entries...")
    messages(conn, offsetRange) {
      val config = offsetRange.config
      val initialStart = offsetRange.start.map(id => new EntryID(id))
        .getOrElse(new EntryID(0, 0))
      val initialEntry = new SimpleEntry(config.streamKey, initialStart)
      Iterator.iterate(xreadGroup(conn, offsetRange, initialEntry)) { response =>
        val responseOption = for {
          lastEntries <- response.asScala.lastOption
          lastEntry <- lastEntries.getValue.asScala.lastOption
          lastEntryId = lastEntry.getID
          startEntryId = new EntryID(lastEntryId.getTime, lastEntryId.getSequence + 1)
          startEntryOffset = new SimpleEntry(config.streamKey, startEntryId)
        } yield xreadGroup(conn, offsetRange, startEntryOffset)
        responseOption.getOrElse(new util.ArrayList)
      }
    }
  }

  def unreadMessages(conn: Jedis, offsetRange: RedisSourceOffsetRange): Iterator[StreamEntry] = {
    logInfo("Reading unread stream entries...")
    messages(conn, offsetRange) {
      val config = offsetRange.config
      val startEntryOffset = new SimpleEntry(config.streamKey, EntryID.UNRECEIVED_ENTRY)
      Iterator.continually {
        xreadGroup(conn, offsetRange, startEntryOffset)
      }
    }
  }

  private def xreadGroup(conn: Jedis, offsetRange: RedisSourceOffsetRange,
                         startEntryOffset: JMap.Entry[String, EntryID]): StreamEntryBatches = {
    val config = offsetRange.config
    conn.xreadGroup(config.groupName, config.consumerName, config.batchSize, config.block,
      false, startEntryOffset)
  }

  private def messages(conn: Jedis, offsetRange: RedisSourceOffsetRange)
                      (streamGroups: => Iterator[StreamEntryBatches]): Iterator[StreamEntry] = {
    import scala.math.Ordering.Implicits._
    val end = new EntryID(offsetRange.end)
    streamGroups
      .takeWhile { response =>
        !response.isEmpty
      }
      .flatMap { response =>
        val responseScala = response.asScala
        logDebug(s"Got entries: $responseScala")
        responseScala.iterator
      }
      .flatMap { streamEntry =>
        flattenRddEntry(streamEntry)
      }
      .takeWhile { case (entryId, _) =>
        entryId <= end
      }
  }

  private def flattenRddEntry(entry: StreamEntryBatch): Iterator[StreamEntry] = {
    entry.getValue.asScala.iterator
      .map { streamEntry =>
        streamEntry.getID -> streamEntry.getFields
      }
  }
}
