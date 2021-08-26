package com.redislabs.provider.redis.util

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}

import org.apache.spark.sql.types._
import redis.clients.jedis.exceptions.JedisDataException

import scala.util.{Failure, Success, Try}

/**
  * @author The Viet Nguyen
  */
object ParseUtils {

  def parseFields(value: Map[String, String], schema: StructType): Array[Any] =
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

  // scalastyle:off cyclomatic.complexity
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

  private[redis] def ignoreJedisWrongTypeException[T](tried: Try[T]): Try[Option[T]] = {
    tried.transform(s => Success(Some(s)), {
      // Swallow this exception
      case e: JedisDataException if Option(e.getMessage).getOrElse("").contains("WRONGTYPE") => Success(None)
      case e: Throwable => Failure(e)
    })
  }
}
