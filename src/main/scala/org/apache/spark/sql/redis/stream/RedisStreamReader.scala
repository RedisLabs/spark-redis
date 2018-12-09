package org.apache.spark.sql.redis.stream

import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.{List => JList, Map => JMap}

import com.redislabs.provider.redis.util.Logging
import org.apache.spark.sql.redis.stream.RedisSourceRdd.{EntryIdWithFieldsIterator, StreamK, StreamKeyWithEntries}
import redis.clients.jedis.{EntryID, Jedis}

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
object RedisStreamReader extends Logging {

  def pendingMessages(conn: Jedis, offsetRange: RedisSourceOffsetRange):
  EntryIdWithFieldsIterator = {
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
        } yield xreadGroup(conn, offsetRange, startEntryOffset)
        responseOption.getOrElse(new util.ArrayList)
      }
    }
  }

  def unreadMessages(conn: Jedis, offsetRange: RedisSourceOffsetRange):
  EntryIdWithFieldsIterator = {
    logInfo("Reading unread stream entries...")
    messages(conn, offsetRange) {
      val startEntryOffset = new SimpleEntry(offsetRange.streamKey, EntryID.UNRECEIVED_ENTRY)
      Stream.continually {
        xreadGroup(conn, offsetRange, startEntryOffset)
      }
    }
  }

  private def xreadGroup(conn: Jedis, offsetRange: RedisSourceOffsetRange,
                         startEntryOffset: JMap.Entry[String, EntryID]):
  JList[StreamKeyWithEntries] = conn
    .xreadGroup(offsetRange.groupName, "consumer-123", 1000, 100, false, startEntryOffset)

  private def messages(conn: Jedis, offsetRange: RedisSourceOffsetRange)
                      (streamGroups: => StreamK): EntryIdWithFieldsIterator = {
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
      .iterator
  }

  private def flattenRddEntry(entry: StreamKeyWithEntries): EntryIdWithFieldsIterator = {
    entry.getValue.asScala.iterator
      .map { streamEntry =>
        streamEntry.getID -> streamEntry.getFields
      }
  }
}
