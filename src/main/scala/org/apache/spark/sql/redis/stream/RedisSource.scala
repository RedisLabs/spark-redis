package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.redis.StreamOptionStreamKey
import org.apache.spark.sql.redis.stream.RedisSource.Entity
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

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

  override def schema: StructType = userDefinedSchema.getOrElse {
    throw new IllegalArgumentException("Please specify schema")
  }

  override def getOffset: Option[Offset] = {
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      val info = conn.xinfo(XINFO.StreamKey, streamKey)
      info.get(XINFO.LastEntry)
        .flatMap {
          case entry: Map[String, AnyRef] =>
            entry.get(XINFO.EntryId)
        }
        .map {
          case index: String =>
            RedisSourceOffset(index)
        }
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val data = Seq(Entity("1"), Entity("2"), Entity("3"))
    val internalRdd = sc.parallelize(data).map { r => InternalRow(UTF8String.fromString(r._id)) }
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
