package org.apache.spark.sql.redis

import java.util.{List => JList}

import com.redislabs.provider.redis.util.ParseUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import redis.clients.jedis.{Pipeline, PipelineBase}

import scala.collection.JavaConverters._

/**
  * @param replace `write.mode = replace`: each row replaces the hash stored under its key, instead of being merged
  *                into it. See `save`.
  * @author The Viet Nguyen
  */
class HashRedisPersistence(replace: Boolean) extends RedisPersistence[Any] {

  /** `write.mode = merge`, the 3.x behaviour. */
  def this() = this(replace = false)

  /**
    * `merge`, the default: sets the row's fields, so fields the row doesn't have stay in the hash, then the TTL.
    *
    * `replace`: one `MULTI`/`EXEC` on the key, so a reader sees the old row or the new one, never a mix. It unlinks
    * the key, sets the row's fields, and sets the TTL when `ttl` is given. The key keeps no TTL from the hash it
    * replaces. A row with no non-null fields leaves no key, because Redis has no empty hashes.
    */
  override def save(pipeline: Pipeline, key: String, value: Any, ttl: Int): Unit =
    if (replace) saveReplacing(pipeline, key, value, ttl) else saveMerging(pipeline, key, value, ttl)

  private def saveMerging(pipeline: Pipeline, key: String, value: Any, ttl: Int): Unit = {
    val javaValue = value.asInstanceOf[Map[String, String]].asJava
    pipeline.hmset(key, javaValue)
    if (ttl > 0) {
      pipeline.expire(key, ttl.toLong)
    }
  }

  private def saveReplacing(pipeline: Pipeline, key: String, value: Any, ttl: Int): Unit = {
    val fields = value.asInstanceOf[Map[String, String]]
    pipeline.multi()
    (pipeline: PipelineBase).unlink(key) // fix ambiguous reference to overloaded definition
    if (fields.nonEmpty) {
      pipeline.hset(key, fields.asJava)
      if (ttl > 0) {
        pipeline.expire(key, ttl.toLong)
      }
    }
    pipeline.exec()
  }

  override def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit = {
    pipeline.hmget(key, requiredColumns: _*)
  }

  override def encodeRow(keyName: String, value: Row): Map[String, String] = {
    val fields = value.schema.fields.map(_.name)
    val kvMap = value.getValuesMap[Any](fields)
    kvMap
      .filter { case (_, v) =>
        // don't store null values
        v != null
      }
      .filter { case (k, _) =>
        // don't store key values
        k != keyName
      }
      .map { case (k, v) =>
        k -> String.valueOf(v)
      }
  }

  override def decodeRow(keyMap: (String, String), value: Any, schema: StructType,
                         requiredColumns: Seq[String]): Row = {
    val scalaValue = value.asInstanceOf[JList[String]].asScala
    val values = requiredColumns.zip(scalaValue)
    val results = values :+ keyMap
    val fieldsValue = ParseUtils.parseFields(results.toMap, schema)
    new GenericRowWithSchema(fieldsValue, schema)
  }
}
