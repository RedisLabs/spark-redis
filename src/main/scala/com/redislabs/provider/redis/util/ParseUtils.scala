package com.redislabs.provider.redis.util


import org.apache.spark.sql.types._
import redis.clients.jedis.exceptions.JedisDataException

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.sql.Date
import scala.util.{Failure, Success, Try}

/**
  * @author The Viet Nguyen
  */
object ParseUtils {

  def parseFields(value: Map[String, Array[Byte]], schema: StructType): Array[Any] =
    schema.fields.map { field =>
      val fieldName = field.name
      val fieldValue = value(fieldName)
      parseValue(field.dataType, fieldValue)
    }

  private def parseValue(dataType: DataType, fieldValueStr: Array[Byte]): Any = {
    if (fieldValueStr == null) {
      null
    } else {
      parseNotNullValue(dataType, fieldValueStr)
    }
  }

  // scalastyle:off cyclomatic.complexity
  private def parseNotNullValue(dataType: DataType, data: Array[Byte]): Any = {
    dataType match {
      case BinaryType => data
      case ByteType => data.head
      case IntegerType => ByteBuffer.wrap(data).getInt()
      case LongType => ByteBuffer.wrap(data).getLong()
      case FloatType => ByteBuffer.wrap(data).getFloat()
      case DoubleType => ByteBuffer.wrap(data).getDouble()
      case BooleanType => data.head == 1
      case ShortType => ByteBuffer.wrap(data).getShort()
      case DateType => new Date(ByteBuffer.wrap(data).getLong())
      case TimestampType => new Timestamp(ByteBuffer.wrap(data).getLong())
      case StringType => new String(data, StandardCharsets.UTF_8)
      case _ => throw new IllegalArgumentException("Unsupported data type")
    }
  }

  private[redis] def ignoreJedisWrongTypeException[T](tried: Try[T]): Try[Option[T]] = {
    tried.transform(s => Success(Some(s)), {
      // Swallow this exception
      case e: JedisDataException if Option(e.getMessage).getOrElse("").contains("WRONGTYPE") => Success(None)
      case e: Throwable => Failure(e)
    })
  }
}
