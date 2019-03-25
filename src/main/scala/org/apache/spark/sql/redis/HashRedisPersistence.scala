package org.apache.spark.sql.redis

import java.util.{List => JList}

import com.redislabs.provider.redis.util.ParseUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import redis.clients.jedis.Pipeline

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashRedisPersistence extends RedisPersistence[Any] {

  override def save(pipeline: Pipeline, key: String, value: Any, ttl: Int): Unit = {
    val javaValue = value.asInstanceOf[Map[String, String]].asJava
    pipeline.hmset(key, javaValue)
    if (ttl > 0) {
      pipeline.expire(key, ttl)
    }
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
