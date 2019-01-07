package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.CollectionUtils.RichCollection
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.StreamUtils.{EntryIdEarliest, createConsumerGroupIfNotExist, resetConsumerGroup}
import com.redislabs.provider.redis.util.{Logging, ParseUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.redis.stream.RedisSource.getOffsetRanges
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
import redis.clients.jedis.{EntryID, Jedis}

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class RedisSource(sqlContext: SQLContext, metadataPath: String,
                  userDefinedSchema: Option[StructType], parameters: Map[String, String])
  extends Source with Logging {

  private val sc = sqlContext.sparkContext

  private val redisConfig = RedisConfig.fromSparkConf(sc.getConf)

  private val sourceConfig = RedisSourceConfig.fromMap(parameters)

  private val currentSchema = userDefinedSchema.getOrElse {
    throw new IllegalArgumentException("Please specify schema")
  }

  def start(): Unit = {
    getOffset.foreach { offset =>
      val offsetRanges = getOffsetRanges(sourceConfig.start, offset, sourceConfig.consumerConfigs)
      // if consumer group doesn't exist - create it starting from the config offset
      // or Earliest if offset is not specified in config
      createConsumerGroupsIfNotExist(offsetRanges)

      // if consumer group exists - reset offset to the specified in config
      resetConsumerGroupsIfHasOffset(offsetRanges)
    }
  }

  override def schema: StructType = currentSchema

  /**
    * Returns the maximum available offset for this source.
    * Returns `None` if this source has never received any data.
    */
  override def getOffset: Option[Offset] = {
    val initialOffset = RedisSourceOffset(Map())
    val combinedOffset = sourceConfig.consumerConfigs.foldLeft(initialOffset) { case (acc, e) =>
      val streamKey = e.streamKey
      withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
        val infoMap = conn.xinfo(XINFO.SubCommandStream, streamKey)
        val offsetId = infoMap(XINFO.LastGeneratedId)
        val streamOffset = streamKey -> RedisConsumerOffset(e.groupName, String.valueOf(offsetId))
        acc.copy(acc.offsets + streamOffset)
      }
    }
    Some(combinedOffset)
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

    // reset consumer group offset to read this batch
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

  private def createConsumerGroupsIfNotExist(offsetRanges: Seq[RedisSourceOffsetRange]): Unit = {
    // create or reset consumer groups
    forEachOffsetRangeWithStreamConnection(offsetRanges) { case (conn, offsetRange) =>
      val offsetRangeStart = offsetRange.start
      val start = offsetRangeStart.map(new EntryID(_)).getOrElse(EntryIdEarliest)
      val config = offsetRange.config
      createConsumerGroupIfNotExist(conn, config.streamKey, config.groupName, start,
        offsetRangeStart.isDefined)
    }
  }

  private def resetConsumerGroupsIfHasOffset(offsetRanges: Seq[RedisSourceOffsetRange]): Unit = {
    // create or reset consumer groups
    forEachOffsetRangeWithStreamConnection(offsetRanges) { case (conn, offsetRange) =>
      offsetRange.start.map(new EntryID(_)).foreach { start =>
        val config = offsetRange.config
        resetConsumerGroup(conn, config.streamKey, config.groupName, start)
      }
    }
  }

  private def forEachOffsetRangeWithStreamConnection(offsetRanges: Seq[RedisSourceOffsetRange])
                                                    (op: (Jedis, RedisSourceOffsetRange) => Unit):
  Unit = {
    offsetRanges.groupBy(_.config.streamKey).foreach { case (streamKey, subRanges) =>
      withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
        subRanges.distinctBy(_.config.groupName).foreach { offsetRange =>
          op(conn, offsetRange)
        }
      }
    }
  }
}

object RedisSource {

  def getConfigMap(configs: Seq[RedisConsumerConfig]): Map[String, RedisConsumerConfig] =
    configs
      .map { config =>
        config.streamKey -> config
      }
      .toMap

  def getOffsetRanges(start: Option[Offset], end: Offset,
                      consumerConfigs: Seq[RedisConsumerConfig]): Seq[RedisSourceOffsetRange] = {
    val offsetStarts = start.map(_.asInstanceOf[RedisSourceOffset]).map(_.offsets).getOrElse(Map())
    val offsetEnds = end.asInstanceOf[RedisSourceOffset]
    val configs = getConfigMap(consumerConfigs)
    offsetEnds.offsets
      .map { case (streamKey, offsetEnd) =>
        val offsetStart = offsetStarts.get(streamKey).map(_.offset)
        RedisSourceOffsetRange(offsetStart, offsetEnd.offset, configs(streamKey))
      }
      .toSeq
  }

  case class Entity(_id: String)

}
