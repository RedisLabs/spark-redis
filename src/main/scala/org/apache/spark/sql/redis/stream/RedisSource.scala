package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.StreamUtils.{EntryIdEarliest, createConsumerGroupIfNotExist, nextEntryId,
  resetConsumerGroup}
import com.redislabs.provider.redis.util.{FieldParseException, Logging, ParseUtils}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, Offset, Source}
import org.apache.spark.sql.redis.stream.RedisSource._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import redis.clients.jedis.{StreamEntryID, Jedis}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * @author The Viet Nguyen
  */
class RedisSource(sqlContext: SQLContext, metadataPath: String,
                  userDefinedSchema: Option[StructType], parameters: Map[String, String])
  extends Source with Logging {

  private val sc = sqlContext.sparkContext

  implicit private val redisConfig: RedisConfig = RedisConfig.fromSparkConfAndParameters(sc.getConf, parameters)

  private val sourceConfig = RedisSourceConfig.fromMap(parameters)

  private val streamGroups = sourceConfig.consumerConfigs.map(c => c.streamKey -> c.groupName).distinct

  private val currentSchema = userDefinedSchema.getOrElse {
    throw new IllegalArgumentException("Please specify schema")
  }

  /**
    * Called once on the source startup. Creates consumer groups and resets their offsets if needed.
    */
  def start(): Unit = {
    sourceConfig.consumerConfigs.foreach { consumerConfig =>
      val offsetsByStreamKey = sourceConfig.start.map(_.offsets).getOrElse(Map())
      val streamKey = consumerConfig.streamKey
      val groupName = consumerConfig.groupName
      val configOffsetOption = offsetsByStreamKey.get(streamKey).map(_.offset).map(new StreamEntryID(_))
      withConnection(streamKey) { conn =>
        createConsumerGroupIfNotExist(conn, streamKey, groupName, configOffsetOption.getOrElse(StreamEntryID.LAST_ENTRY))
        // if config offset is defined, reset to its value
        configOffsetOption.foreach { offset =>
          resetConsumerGroup(conn, streamKey, groupName, offset)
        }
      }
    }
  }

  override def schema: StructType = currentSchema

  /**
    * Returns the maximum available offset for this source.
    * Returns `None` if this source has never received any data.
    */
  override def getOffset: Option[Offset] = {
    val initialOffset = RedisSourceOffset(Map())
    val sourceOffset = sourceConfig.consumerConfigs.foldLeft(initialOffset) { case (acc, e) =>
      val streamKey = e.streamKey
      withConnection(streamKey) { conn =>
        Try {
          // try to read last stream id, it will fail if doesn't exist
          val offsetId = streamLastId(conn, streamKey)
          val streamOffset = streamKey -> RedisConsumerOffset(e.groupName, offsetId)
          acc.copy(acc.offsets + streamOffset)
        } getOrElse {
          // stream key doesn't exist
          acc
        }
      }
    }
    if (sourceOffset.offsets.isEmpty) {
      None
    } else {
      Some(sourceOffset)
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo {
      s"""Getting batch...
         |  start: $start
         |  end: $end
      """.stripMargin
    }
    val localSchema = currentSchema
    val batchStart = start.orElse(Some(firstBatchStart))
    val offsetRanges = getOffsetRanges(batchStart, end, sourceConfig.consumerConfigs, firstEntryIds(batchStart, end))

    // the batch is read from its offset ranges, the consumer groups only record the progress
    setConsumerGroupsToEnd(end)

    // read data. Partition i of RedisSourceRdd reads offsetRanges(i).
    val streamKeys = offsetRanges.map(_.config.streamKey)
    val internalRdd = new RedisSourceRdd(sc, redisConfig, offsetRanges)
      .mapPartitionsWithIndex { case (partition, entries) =>
        val streamKey = streamKeys(partition)
        // ParseUtils returns external values (String, java.sql.Date, java.sql.Timestamp, ...), but an
        // InternalRow must hold Catalyst's internal representation (UTF8String, Int days, Long micros)
        val toCatalyst = CatalystTypeConverters.createToCatalystConverter(localSchema)
        entries.map { case (id, fields) =>
          val fieldMap = fields.asScala.toMap + ("_id" -> id.toString)
          val values = FieldParseException.withKey(streamKey, Some(id.toString)) {
            ParseUtils.parseFields(fieldMap, localSchema)
          }
          toCatalyst(Row.fromSeq(values)).asInstanceOf[InternalRow]
        }
      }
    sqlContext.internalCreateDataFrame(internalRdd, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = {
    logInfo(
      s"""Committing offset..
         |  end: ${end.json()}
         |""".stripMargin)
  }

  override def stop(): Unit = {
  }

  /**
    * The start of the first batch, for which Spark passes no start offset: the positions of the consumer groups
    * when the query runs for the first time with its checkpoint. It is saved in the checkpoint (`metadataPath`),
    * so that the first batch reads the same entries if it is executed again after a failure.
    */
  private lazy val firstBatchStart: RedisSourceOffset = {
    val metadataLog = new HDFSMetadataLog[RedisSourceOffset](sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offset = consumerGroupOffsets()
      metadataLog.add(0, offset)
      offset
    }
  }

  /**
    * The last delivered ids of the consumer groups.
    */
  private def consumerGroupOffsets(): RedisSourceOffset = {
    val offsets = streamGroups.map { case (streamKey, groupName) =>
      withConnection(streamKey) { conn =>
        val group = conn.xinfoGroup(streamKey).asScala.find(_.getName == groupName).getOrElse {
          throw new IllegalStateException(s"Consumer group $groupName of stream $streamKey doesn't exist")
        }
        streamKey -> RedisConsumerOffset(groupName, group.getLastDeliveredId.toString)
      }
    }
    RedisSourceOffset(offsets.toMap)
  }

  /**
    * The ids of the first entries of the batch, for the streams read by several consumers.
    */
  private def firstEntryIds(start: Option[Offset], end: Offset): Map[String, String] = {
    val offsetStarts = start.map(RedisSourceOffset.fromOffset).map(_.offsets).getOrElse(Map())
    val parallelStreamKeys = sourceConfig.consumerConfigs.groupBy(_.streamKey).filter(_._2.size > 1).keySet
    RedisSourceOffset.fromOffset(end).offsets
      .filter { case (streamKey, _) => parallelStreamKeys(streamKey) }
      .flatMap { case (streamKey, offsetEnd) =>
        val after = offsetStarts.get(streamKey).map(o => new StreamEntryID(o.offset)).getOrElse(EntryIdEarliest)
        withConnection(streamKey) { conn =>
          conn.xrange(streamKey, nextEntryId(after), new StreamEntryID(offsetEnd.offset), 1).asScala
            .headOption.map(entry => streamKey -> entry.getID.toString)
        }
      }
  }

  /**
    * Sets the consumer groups to the end of the batch, so that their last delivered id shows the progress
    * of the query, and a query started without checkpoint continues from there.
    */
  private def setConsumerGroupsToEnd(end: Offset): Unit = {
    val offsetEnds = RedisSourceOffset.fromOffset(end).offsets
    streamGroups.foreach { case (streamKey, groupName) =>
      offsetEnds.get(streamKey).foreach { offsetEnd =>
        withConnection(streamKey) { conn =>
          resetConsumerGroup(conn, streamKey, groupName, new StreamEntryID(offsetEnd.offset))
        }
      }
    }
  }

}

object RedisSource {

  /**
    * The offset ranges to read, one per consumer. The consumers of a stream read consecutive parts of its range.
    *
    * @param firstEntryIds ids of the first entries of the batch by stream key (if known), see [[splitOffsetRange]]
    */
  def getOffsetRanges(start: Option[Offset], end: Offset,
                      consumerConfigs: Seq[RedisConsumerConfig],
                      firstEntryIds: Map[String, String] = Map()): Seq[RedisSourceOffsetRange] = {

    val offsetStarts = start.map(RedisSourceOffset.fromOffset).map(_.offsets).getOrElse(Map())
    val offsetEnds = RedisSourceOffset.fromOffset(end)
    val configsByStreamKey = consumerConfigs.groupBy(_.streamKey)

    offsetEnds.offsets.flatMap { case (streamKey, offsetEnd) =>
      val offsetStart = offsetStarts.get(streamKey).map(_.offset)
      val configs = configsByStreamKey(streamKey)
      val ranges = splitOffsetRange(offsetStart, offsetEnd.offset, configs.size, firstEntryIds.get(streamKey))
      configs.zip(ranges).map { case (c, (rangeStart, rangeEnd)) => RedisSourceOffsetRange(rangeStart, rangeEnd, c) }
    }.toSeq
  }

  /**
    * Splits the range (start, end] of a stream into `n` consecutive ranges, at evenly spaced times between the
    * first entry of the range (or the start if it is not known) and the end.
    */
  def splitOffsetRange(start: Option[String], end: String, n: Int,
                       firstEntryId: Option[String] = None): Seq[(Option[String], String)] = {
    start match {
      case Some(startOffset) if n > 1 =>
        val startId = new StreamEntryID(startOffset)
        val endId = new StreamEntryID(end)
        val fromTime = firstEntryId.map(new StreamEntryID(_)).getOrElse(startId).getTime
        val bounds = (1 until n).map { i =>
          val bound = new StreamEntryID(fromTime + (endId.getTime - fromTime) * i / n, 0)
          if (bound.compareTo(startId) < 0) startId else if (bound.compareTo(endId) > 0) endId else bound
        }
        (startId +: bounds :+ endId).sliding(2).map { ids => Some(ids.head.toString) -> ids.last.toString }.toSeq
      case _ =>
        // the first consumer reads the whole range
        (start -> end) +: Seq.fill(n - 1)(Some(end) -> end)
    }
  }

  def streamLastId(conn: Jedis, streamKey: String): String = {
    val infoMap = conn.xinfo(XINFO.SubCommandStream, streamKey)
    String.valueOf(infoMap(XINFO.LastGeneratedId))
  }

}
