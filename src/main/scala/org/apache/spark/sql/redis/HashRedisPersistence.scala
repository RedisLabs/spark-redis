package org.apache.spark.sql.redis

import java.util.{List => JList}
import com.redislabs.provider.redis.util.ParseUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import redis.clients.jedis.Pipeline

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashRedisPersistence extends RedisPersistence[Any] {

  override def save(pipeline: Pipeline, key: String, value: Any, ttl: Int): Unit = {
    val javaValue = value.asInstanceOf[Map[String, Array[Byte]]]
      .map { case (key, value) => key.getBytes(StandardCharsets.UTF_8) -> value }.asJava
    pipeline.hmset(key.getBytes(StandardCharsets.UTF_8), javaValue)
    if (ttl > 0) {
      pipeline.expire(key, ttl.toLong)
    }
  }

  override def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit = {
    val cols = requiredColumns.map(str => str.getBytes(StandardCharsets.UTF_8))

    pipeline.hmget(
      key.getBytes(StandardCharsets.UTF_8),
      cols: _*)
  }

  override def encodeRow(keyName: String, value: Row): Map[String, Array[Byte]] = {
    val fields = value.schema.fields.map(_.name)
    val kvMap = value.getValuesMap[Any](fields)
    kvMap
      .filter { case (_, v) =>
        // don't store null values
        v != null
      }
      .map { case (k, v) =>
        k -> (v match {
          case value: Array[Byte] => value
          case _ => throw new Exception("The Dressipi version of Spark Redis only supports byte arrays for now.")
        })
      }
      .filter { case (k, _) =>
        // don't store key values
        k != keyName
      }
  }

  override def decodeRow(keyMap: (String, String), value: Any, schema: StructType,
                         requiredColumns: Seq[String]): Row = {
    val scalaValue = value.asInstanceOf[JList[Array[Byte]]].asScala
    val values = requiredColumns.zip(scalaValue)
    val results = values :+ (keyMap._1, keyMap._2.getBytes)

    val fieldsValue = ParseUtils.parseFields(results.toMap, schema)
    new GenericRowWithSchema(fieldsValue, schema)
  }
}
