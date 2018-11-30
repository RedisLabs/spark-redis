package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.{Logging, ParseUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.redis.StreamOptionStreamKey
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

  private val streamKey = parameters.getOrElse(StreamOptionStreamKey,
    throw new IllegalArgumentException("Please specify 'stream.key'"))

  private val currentSchema = userDefinedSchema.getOrElse {
    throw new IllegalArgumentException("Please specify schema")
  }

  override def schema: StructType = currentSchema

  override def getOffset: Option[Offset] = {
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      val info = conn.xinfo(XINFO.StreamKey, streamKey)
      info.get(XINFO.LastGeneratedId)
        .map {
          case index: String =>
            RedisSourceOffset(index)
        }
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val localSchema = currentSchema
    val startJson = start.map(_.json())
    val offsetRange = RedisSourceOffsetRange(streamKey, startJson, end.json())
    val internalRdd = new RedisSourceRdd(sc, redisConfig, offsetRange)
      .map { case (id, fields) =>
        val fieldMap = fields.asScala.toMap + ("_id" -> id)
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
}

object RedisSource {

  case class Entity(_id: String)

}
