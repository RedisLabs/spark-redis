package org.apache.spark.sql.redis

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.util.{List => JList, Map => JMap}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import redis.clients.jedis.Implicits.KvPipeline
import redis.clients.jedis.Pipeline

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashRedisPersistence extends RedisPersistence[Any] {

  override def save(pipeline: Pipeline, key: String, value: Any, ttl: Int): Unit = {
    value match {
      case v: Map[String, String] =>
        val javaValue = v.asJava
        pipeline.hmset(key, javaValue)
        if (ttl > 0) {
          pipeline.expire(key, ttl)
        }
    }
  }

  override def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit = {
    if (requiredColumns.isEmpty) {
      pipeline.getHashAllWithKey(key)
    } else {
      pipeline.getHashMultipleWithKey(key, requiredColumns: _*)
    }
  }

  override def encodeRow(key: (String, String), value: Row): Map[String, String] = {
    val fields = value.schema.fields.map(_.name)
    val kvMap = value.getValuesMap[Any](fields)
    kvMap
      .filter { case (_, v) =>
        // don't store null values
        v != null
      }
      .filter { case (k, _) =>
        // don't store key values
        k != key._1
      }
      .map { case (k, v) =>
        k -> String.valueOf(v)
      }
  }

  override def decodeRow(key: (String, String), value: Any, schema: => StructType,
                         inferSchema: Boolean, requiredColumns: Seq[String]): Row = {
    val values = value match {
      case v: JMap[String, String] => v.asScala.toSeq
      case v: JList[String] =>
        requiredColumns.filter(_ != key._1).zip(v.asScala)
    }
    val results = values :+ key
    val actualSchema = if (!inferSchema) schema else {
      val fields = results.map(kv => StructField(kv._1, StringType)).toArray
      StructType(fields)
    }
    val fieldsValue = parseFields(results.toMap, actualSchema)
    new GenericRowWithSchema(fieldsValue, actualSchema)
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
