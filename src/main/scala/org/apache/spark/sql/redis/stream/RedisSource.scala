package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.CollectionUtils.RichCollection
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.StreamUtils.{createConsumerGroupIfNotExist, resetConsumerGroup}
import com.redislabs.provider.redis.util.{Logging, ParseUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.redis.stream.RedisSource._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
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
    val offsetRanges = getOffsetRanges(start, end, sourceConfig.consumerConfigs)

    // if 'start' is set, reset consumer group offset to read this batch
    resetConsumerGroupsIfHasOffset(offsetRanges)

    // read data
    val internalRdd = new RedisSourceRdd(sc, redisConfig, offsetRanges)
      .map { case (id, fields) =>
        val fieldMap = fields.asScala.toMap + ("_id" -> id.toString)
        val values = ParseUtils.parseFields(fieldMap, localSchema)
          .map {
            case str: String => UTF8String.fromString(str)
            case other: Any => other
          }
        InternalRow(values: _*)
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

  private def resetConsumerGroupsIfHasOffset(offsetRanges: Seq[RedisSourceOffsetRange]): Unit = {
    forEachOffsetRangeWithStreamConnection(offsetRanges) { case (conn, offsetRange) =>
      offsetRange.start.map(new StreamEntryID(_)).foreach { start =>
        val config = offsetRange.config
        resetConsumerGroup(conn, config.streamKey, config.groupName, start)
      }
    }
  }

  private def forEachOffsetRangeWithStreamConnection(offsetRanges: Seq[RedisSourceOffsetRange])
                                                    (op: (Jedis, RedisSourceOffsetRange) => Unit): Unit = {
    offsetRanges.groupBy(_.config.streamKey).foreach { case (streamKey, subRanges) =>
      withConnection(streamKey) { conn =>
        subRanges.distinctBy(_.config.groupName).foreach { offsetRange =>
          op(conn, offsetRange)
        }
      }
    }
  }

}

object RedisSource {

  def getOffsetRanges(start: Option[Offset], end: Offset,
                      consumerConfigs: Seq[RedisConsumerConfig]): Seq[RedisSourceOffsetRange] = {

    val offsetStarts = start.map(RedisSourceOffset.fromOffset).map(_.offsets).getOrElse(Map())
    val offsetEnds = RedisSourceOffset.fromOffset(end)
    val configsByStreamKey = consumerConfigs.groupBy(_.streamKey)

    offsetEnds.offsets.flatMap { case (streamKey, offsetEnd) =>
      val offsetStart = offsetStarts.get(streamKey).map(_.offset)
      val configs = configsByStreamKey(streamKey)
      configs.map { c => RedisSourceOffsetRange(offsetStart, offsetEnd.offset, c) }
    }.toSeq
  }

  def streamLastId(conn: Jedis, streamKey: String): String = {
    val infoMap = conn.xinfo(XINFO.SubCommandStream, streamKey)
    String.valueOf(infoMap(XINFO.LastGeneratedId))
  }

}
