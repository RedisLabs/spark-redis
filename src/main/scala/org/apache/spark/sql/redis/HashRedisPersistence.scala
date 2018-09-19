package org.apache.spark.sql.redis

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Long => JLong}
import java.nio.charset.StandardCharsets
import java.util.{Map => JMap}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import redis.clients.jedis.{Pipeline, Response}

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashRedisPersistence extends RedisPersistence[JMap[Array[Byte], Array[Byte]]] {

  override def save(pipeline: Pipeline,
                    key: Array[Byte], value: JMap[Array[Byte], Array[Byte]]): Unit =
    pipeline.hmset(key, value)

  override def load(pipeline: Pipeline,
                    key: Array[Byte]): Response[JMap[Array[Byte], Array[Byte]]] =
    pipeline.hgetAll(key)

  override def encodeRow(value: Row): JMap[Array[Byte], Array[Byte]] = {
    value.schema.fields.map(_.name)
      .map { field =>
        field -> value.getAs[Any](field)
      }
      .map { case (k, v) =>
        k.getBytes(StandardCharsets.UTF_8) ->
          String.valueOf(v).getBytes(StandardCharsets.UTF_8)
      }
      .toMap
      .asJava
  }

  override def decodeRow(value: JMap[Array[Byte], Array[Byte]], schema: StructType): Row = {
    val fieldsValue = sortFields(value, schema)
    new GenericRowWithSchema(fieldsValue, schema)
  }

  private def sortFields(value: JMap[Array[Byte], Array[Byte]], schema: StructType): Array[Any] =
    schema.fields
      .map { field =>
        field -> field.name
      }
      .map { case (field, fieldName) =>
        field -> fieldName.getBytes(StandardCharsets.UTF_8)
      }
      .map { case (field, fieldNameBytes) =>
        field -> value.get(fieldNameBytes)
      }
      .map { case (field, fieldValueBytes) =>
        field -> new String(fieldValueBytes, StandardCharsets.UTF_8)
      }
      .map { case (field, fieldValueStr) =>
        parseValue(field.dataType, fieldValueStr)
      }

  private def parseValue(dataType: DataType, fieldValueStr: String): Any =
    dataType match {
      case ByteType => JByte.parseByte(fieldValueStr)
      case IntegerType => Integer.parseInt(fieldValueStr)
      case LongType => JLong.parseLong(fieldValueStr)
      case DoubleType => JDouble.parseDouble(fieldValueStr)
      case BooleanType => JBoolean.parseBoolean(fieldValueStr)
      case _ => fieldValueStr
    }
}
