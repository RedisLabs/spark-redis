package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.Logging
import com.redislabs.provider.redis.util.StreamUtils.{EntryIdEarliest, nextEntryId}
import org.apache.spark.sql.redis.stream.RedisSourceTypes.StreamEntry
import redis.clients.jedis.StreamEntryID

import scala.collection.JavaConverters._
import scala.math.Ordering.Implicits._

/**
  * @author The Viet Nguyen
  */
class RedisStreamReader(redisConfig: RedisConfig) extends Logging with Serializable {

  /**
    * Reads the entries of the offset range (start, end] with XRANGE.
    *
    * The entries only depend on the offset range, not on the state of the consumer group, so a partition returns
    * the same entries every time it is computed: when a task is retried, when the batch DataFrame is used by
    * several actions (e.g. in `foreachBatch`) or when a batch is executed again after a restart.
    */
  def streamEntries(offsetRange: RedisSourceOffsetRange): Iterator[StreamEntry] = {
    val config = offsetRange.config

    logInfo(s"Reading entries " +
      s"[${config.streamKey}, ${config.groupName}, ${config.consumerName}, start=${offsetRange.start} " +
      s"end=${offsetRange.end}]... "
    )

    val start = offsetRange.start.map(new StreamEntryID(_)).getOrElse(EntryIdEarliest)
    val end = new StreamEntryID(offsetRange.end)
    // each XRANGE call reads at most batchSize entries after the last entry of the previous call,
    // a call that returns less than batchSize entries has reached the end of the range
    Iterator.iterate(readEntries(config, start, end)) { previous =>
      if (previous.isEmpty || previous.size < config.batchSize) {
        Seq.empty
      } else {
        readEntries(config, previous.last._1, end)
      }
    }
      .takeWhile(_.nonEmpty)
      .flatten
  }

  /**
    * Reads at most batchSize entries of the range (after, end].
    */
  private def readEntries(config: RedisConsumerConfig, after: StreamEntryID,
                          end: StreamEntryID): Seq[StreamEntry] = {
    if (after >= end) {
      Seq.empty
    } else {
      withConnection(redisConfig.connectionForKey(config.streamKey)) { conn =>
        val entries = conn.xrange(config.streamKey, nextEntryId(after), end, config.batchSize).asScala
        logDebug(s"Got entries: $entries")
        entries.map(entry => entry.getID -> entry.getFields)
      }
    }
  }
}
