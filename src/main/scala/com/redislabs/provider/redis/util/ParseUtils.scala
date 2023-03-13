package com.redislabs.provider.redis.util

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import org.apache.spark.sql.types._
import redis.clients.jedis.exceptions.JedisDataException

import java.nio.ByteBuffer
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
  private def parseNotNullValue(dataType: DataType, fieldValueByteArray: Array[Byte]): Any = {
    val buffer = ByteBuffer.wrap(fieldValueByteArray)

    dataType match {
      case BinaryType => buffer.array()
      case ByteType => buffer.get()
      case IntegerType => buffer.getInt()
      case LongType => buffer.getLong()
      case FloatType => buffer.getFloat()
      case DoubleType => buffer.getDouble()
      case BooleanType => buffer.get() != 0
      case ShortType => buffer.getShort()
      case DateType => new java.sql.Date(buffer.getLong())
      case TimestampType => new java.sql.Timestamp(buffer.getLong())
      case _ => fieldValueByteArray
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
