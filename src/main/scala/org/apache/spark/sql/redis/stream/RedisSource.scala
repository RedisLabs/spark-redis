package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.CollectionUtils.RichCollection
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.StreamUtils.{EntryIdEarliest, createConsumerGroupIfNotExist}
import com.redislabs.provider.redis.util.{Logging, ParseUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.redis.stream.RedisSource.getOffsetRanges
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
import redis.clients.jedis.EntryID

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
    createOrResetConsumerGroups(offsetRanges)
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
  }

  override def stop(): Unit = {
  }

  private def createOrResetConsumerGroups(offsetRanges: Seq[RedisSourceOffsetRange]): Unit = {
    // create or reset consumer groups
    offsetRanges.groupBy(_.config.streamKey).foreach { case (streamKey, subRanges) =>
      withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
        subRanges.distinctBy(_.config.groupName).foreach { offsetRange =>
          val start = offsetRange.start.map(new EntryID(_)).getOrElse(EntryIdEarliest)
          val config = offsetRange.config
          createConsumerGroupIfNotExist(conn, config.streamKey, config.groupName, start,
            resetIfExist = true)
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
