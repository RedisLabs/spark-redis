package org.apache.spark.sql.redis.stream

import java.util.AbstractMap.SimpleEntry
import java.util.{Map => JMap}

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.Logging
import org.apache.spark.sql.redis.stream.RedisSourceTypes.{StreamEntry, StreamEntryBatch, StreamEntryBatches}
import redis.clients.jedis.StreamEntryID

import scala.collection.JavaConverters._
import scala.math.Ordering.Implicits._

/**
  * @author The Viet Nguyen
  */
class RedisStreamReader(redisConfig: RedisConfig) extends Logging with Serializable {

  def unreadStreamEntries(offsetRange: RedisSourceOffsetRange): Iterator[StreamEntry] = {
    val config = offsetRange.config

    logInfo(s"Reading entries " +
      s"[${config.streamKey}, ${config.groupName}, ${config.consumerName}, start=${offsetRange.start} " +
      s"end=${offsetRange.end}]... "
    )

    val res = filterStreamEntries(offsetRange) {
      val startEntryOffset = new SimpleEntry(config.streamKey, StreamEntryID.UNRECEIVED_ENTRY)
      Iterator.continually {
        readStreamEntryBatches(offsetRange, startEntryOffset)
      }
    }
    res
  }

  private def readStreamEntryBatches(offsetRange: RedisSourceOffsetRange,
                                     startEntryOffset: JMap.Entry[String, StreamEntryID]): StreamEntryBatches = {
    val config = offsetRange.config
    withConnection(redisConfig.connectionForKey(config.streamKey)) { conn =>
      // we don't need acknowledgement, if spark processing fails, it will request the same batch again
      val noAck = true
      val response = conn.xreadGroup(config.groupName,
        config.consumerName,
        config.batchSize,
        config.block,
        noAck,
        startEntryOffset)
      logDebug(s"Got entries: $response")
      response
    }
  }

  private def filterStreamEntries(offsetRange: RedisSourceOffsetRange)
                                 (streamGroups: => Iterator[StreamEntryBatches]): Iterator[StreamEntry] = {
    val end = new StreamEntryID(offsetRange.end)
    streamGroups
      .takeWhile { response =>
        (response != null) && !response.isEmpty
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
  }

  private def flattenStreamEntries(entry: StreamEntryBatch): Iterator[StreamEntry] = {
    entry.getValue.asScala.iterator
      .map { streamEntry =>
        streamEntry.getID -> streamEntry.getFields
      }
  }
}
