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
  * @author The Viet Nguyen
  */
class RedisStreamReader(autoAck: Boolean) extends Logging with Serializable {

  def pendingStreamEntries(conn: Jedis, offsetRange: RedisSourceOffsetRange):
  Iterator[StreamEntry] = {
    logInfo("Reading pending stream entries...")
    filterStreamEntries(conn, offsetRange) {
      val config = offsetRange.config
      val initialStart = offsetRange.start.map(id => new EntryID(id))
        .getOrElse(new EntryID(0, 0))
      val initialEntry = new SimpleEntry(config.streamKey, initialStart)
      Iterator.iterate(readStreamEntryBatches(conn, offsetRange, initialEntry)) { response =>
        val responseOption = for {
          lastEntries <- response.asScala.lastOption
          lastEntry <- lastEntries.getValue.asScala.lastOption
          lastEntryId = lastEntry.getID
          startEntryId = new EntryID(lastEntryId.getTime, lastEntryId.getSequence + 1)
          startEntryOffset = new SimpleEntry(config.streamKey, startEntryId)
        } yield readStreamEntryBatches(conn, offsetRange, startEntryOffset)
        responseOption.getOrElse(new util.ArrayList)
      }
    }
  }

  def unreadStreamEntries(conn: Jedis, offsetRange: RedisSourceOffsetRange):
  Iterator[StreamEntry] = {
    logInfo("Reading unread stream entries...")
    filterStreamEntries(conn, offsetRange) {
      val config = offsetRange.config
      val startEntryOffset = new SimpleEntry(config.streamKey, EntryID.UNRECEIVED_ENTRY)
      Iterator.continually {
        readStreamEntryBatches(conn, offsetRange, startEntryOffset)
      }
    }
  }

  private def readStreamEntryBatches(conn: Jedis, offsetRange: RedisSourceOffsetRange,
                                     startEntryOffset: JMap.Entry[String, EntryID]):
  StreamEntryBatches = {
    val config = offsetRange.config
    val response = conn.xreadGroup(config.groupName, config.consumerName, config.batchSize,
      config.block, false, startEntryOffset)
    if (autoAck) {
      logInfo(s"Auto acknowledging: $response")
      val end = new EntryID(offsetRange.end)
      val responseScala = response.asScala
      responseScala.foreach { batch =>
        val entryIds = batch.getValue.asScala.map(_.getID).filter(_ <= end)
        if (entryIds.nonEmpty) {
          conn.xack(batch.getKey, config.groupName, entryIds: _*)
        }
      }
    }
    response
  }

  private def filterStreamEntries(conn: Jedis, offsetRange: RedisSourceOffsetRange)
                                 (streamGroups: => Iterator[StreamEntryBatches]):
  Iterator[StreamEntry] = {
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
        flattenStreamEntries(streamEntry)
      }
      .takeWhile { case (entryId, _) =>
        entryId <= end
      }
  }

  private def flattenStreamEntries(entry: StreamEntryBatch): Iterator[StreamEntry] = {
    entry.getValue.asScala.iterator
      .map { streamEntry =>
        streamEntry.getID -> streamEntry.getFields
      }
  }
}
