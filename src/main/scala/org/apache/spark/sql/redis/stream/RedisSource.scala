package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.StreamUtils.EntryIdEarliest
import com.redislabs.provider.redis.util.{Logging, ParseUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Source}
import org.apache.spark.sql.redis.StreamOptionStreamKey
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
import redis.clients.jedis.{EntryID, PendingEntry}

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
          case offset: String =>
            RedisSourceOffset(Map(streamKey -> RedisConsumerOffset("group55", offset)))
        }
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
    val offsetStarts = start.map(_.asInstanceOf[RedisSourceOffset]).map(_.offsets)
      .map(_.groupBy(_._2.groupName)).getOrElse(Map())
    val offsetEnd = end.asInstanceOf[RedisSourceOffset].offsets(streamKey)
    val offsetStart = offsetStarts.get(streamKey).flatMap(_.get(offsetEnd.groupName)).map(_.offset)
    val offsetRange = RedisSourceOffsetRange(streamKey, offsetEnd.groupName,
      offsetStart, offsetEnd.offset)
    val internalRdd = new RedisSourceRdd(sc, redisConfig, Seq(offsetRange))
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
    val offsetEnd = offsetEnds.offsets(streamKey)
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      val targetOffsetEndId = new EntryID(offsetEnd.offset)
      val groupName = offsetEnd.groupName
      val initialOffsetStartId = conn.xpending(streamKey, groupName, EntryIdEarliest,
        targetOffsetEndId, 1, "consumer-123").asScala
        .collectFirst { case pe: PendingEntry => pe.getID }
        .getOrElse(EntryIdEarliest)
      val initialStreamEntries = conn.xrange(streamKey, initialOffsetStartId, targetOffsetEndId,
        1000).asScala
      Stream.iterate(initialOffsetStartId -> initialStreamEntries) { case (prevId, _) =>
        val offsetStartId = new EntryID(prevId.getTime, prevId.getSequence + 1)
        val streamEntries = conn.xrange(streamKey, offsetStartId, targetOffsetEndId, 1000).asScala
        val offsetEndId =
          if (streamEntries.isEmpty) {
            targetOffsetEndId
          } else {
            streamEntries.last.getID
          }
        offsetEndId -> streamEntries
      }
        .takeWhile { case (_, streamEntries) =>
          streamEntries.nonEmpty
        }
        .map { case (_, streamEntries) =>
          streamEntries.map(_.getID)
        }
        .foreach { entryIds =>
          conn.xack(streamKey, groupName, entryIds: _*)
        }
    }
  }

  override def stop(): Unit = {
  }
}

object RedisSource {

  case class Entity(_id: String)

}
