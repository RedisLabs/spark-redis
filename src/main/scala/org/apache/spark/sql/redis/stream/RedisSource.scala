package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.{Logging, ParseUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Source}
import org.apache.spark.sql.redis.stream.RedisSource.{configsMap, getOffsetRanges}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

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

  override def schema: StructType = currentSchema

  override def getOffset: Option[Offset] = {
    val initialOffset = RedisSourceOffset(Map())
    val combinedOffset = sourceConfig.consumerConfigs.foldLeft(initialOffset) { case (acc, e) =>
      val streamKey = e.streamKey
      withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
        val infoMap = conn.xinfo(XINFO.StreamKey, streamKey)
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
    val internalRdd = new RedisSourceRdd(sc, redisConfig, offsetRanges, false)
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
    val offsetEnds = end match {
      case SerializedOffset(json) =>
        RedisSourceOffset.fromJson(json)
    }
    val configs = configsMap(sourceConfig.consumerConfigs)
    val streamReader = new RedisStreamReader(true)
    offsetEnds.offsets.foreach { case (streamKey, offsetEnd) =>
      val groupName = offsetEnd.groupName
      val offsetRange = RedisSourceOffsetRange(None, offsetEnd.offset, configs(streamKey))
      withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
        streamReader.pendingStreamEntries(conn, offsetRange)
          .map { entries => entries._1 }
          .grouped(sourceConfig.batchSize)
          .foreach { entries =>
            conn.xack(streamKey, groupName, entries: _*)
            logDebug(s"Committed entries: $entries")
          }
      }
    }
  }

  override def stop(): Unit = {
  }
}

object RedisSource {

  def configsMap(configs: Seq[RedisConsumerConfig]): Map[String, RedisConsumerConfig] =
    configs
      .map { config =>
        config.streamKey -> config
      }
      .toMap

  def getOffsetRanges(start: Option[Offset], end: Offset,
                      consumerConfigs: Seq[RedisConsumerConfig]): Seq[RedisSourceOffsetRange] = {
    val offsetStarts = start.map(_.asInstanceOf[RedisSourceOffset]).map(_.offsets)
      .map(_.groupBy(_._2.groupName)).getOrElse(Map())
    val offsetEnds = end.asInstanceOf[RedisSourceOffset]
    val configs = configsMap(consumerConfigs)
    offsetEnds.offsets
      .map { case (streamKey, offsetEnd) =>
        val offsetStart = offsetStarts.get(streamKey)
          .flatMap(_.get(offsetEnd.groupName))
          .map(_.offset)
        RedisSourceOffsetRange(offsetStart, offsetEnd.offset, configs(streamKey))
      }
      .toSeq
  }

  case class Entity(_id: String)

}
