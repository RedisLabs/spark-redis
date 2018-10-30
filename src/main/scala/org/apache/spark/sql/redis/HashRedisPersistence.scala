package org.apache.spark.sql.redis

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.util.{List => JList, Map => JMap}

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
    value match {
      case v: Map[_, _] =>
        val javaValue = v.asInstanceOf[Map[String, String]].asJava
        pipeline.hmset(key, javaValue)
        if (ttl > 0) {
          pipeline.expire(key, ttl)
        }
    }
  }

  override def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit = {
    if (requiredColumns.isEmpty) {
      pipeline.hgetAll(key)
    } else {
      pipeline.hmget(key, requiredColumns: _*)
    }
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
    val values = value match {
      case v: JMap[_, _] => v.asInstanceOf[JMap[String, String]].asScala.toSeq
      case v: JList[_] => requiredColumns.zip(v.asInstanceOf[JList[String]].asScala)
    }
    val results = values :+ keyMap
    val fieldsValue = parseFields(results.toMap, schema)
    new GenericRowWithSchema(fieldsValue, schema)
  }

  private def parseFields(value: Map[String, String], schema: StructType): Array[Any] =
    schema.fields.map { field =>
      val fieldName = field.name
      val fieldValue = value(fieldName)
      parseValue(field.dataType, fieldValue)
    }

  private def parseValue(dataType: DataType, fieldValueStr: String): Any = {
    if (fieldValueStr == null) {
      null
    } else {
      parseNotNullValue(dataType, fieldValueStr)
    }
  }

  private def parseNotNullValue(dataType: DataType, fieldValueStr: String): Any =
    dataType match {
      case ByteType => JByte.parseByte(fieldValueStr)
      case IntegerType => Integer.parseInt(fieldValueStr)
      case LongType => JLong.parseLong(fieldValueStr)
      case FloatType => JFloat.parseFloat(fieldValueStr)
      case DoubleType => JDouble.parseDouble(fieldValueStr)
      case BooleanType => JBoolean.parseBoolean(fieldValueStr)
      case ShortType => JShort.parseShort(fieldValueStr)
      case DateType => java.sql.Date.valueOf(fieldValueStr)
      case TimestampType => java.sql.Timestamp.valueOf(fieldValueStr)
      case _ => fieldValueStr
    }
}
