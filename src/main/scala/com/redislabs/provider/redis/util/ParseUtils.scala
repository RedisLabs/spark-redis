package com.redislabs.provider.redis.util

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.sql.Timestamp
import java.time.{LocalDateTime, OffsetDateTime}

import org.apache.spark.sql.types._
import redis.clients.jedis.exceptions.JedisDataException

import scala.util.{Failure, Success, Try}

/**
  * @author The Viet Nguyen
  */
object ParseUtils {

  /**
    * Parses each field of the schema from its string value.
    *
    * @throws IllegalArgumentException a value can't be parsed as its field's type. It is a [[FieldParseException]]
    *                                  that names the field, the value and the type, and a NumberFormatException for
    *                                  a number. Wrap the call in [[FieldParseException.withKey]] to name the key.
    */
  def parseFields(value: Map[String, String], schema: StructType): Array[Any] =
    schema.fields.map { field =>
      val fieldName = field.name
      val fieldValue = value(fieldName)
      parseValue(field, fieldValue)
    }

  private def parseValue(field: StructField, fieldValueStr: String): Any = {
    if (fieldValueStr == null) {
      null
    } else {
      try {
        parseNotNullValue(field.dataType, fieldValueStr)
      } catch {
        case e: IllegalArgumentException => throw FieldParseException(field.name, fieldValueStr, field.dataType, e)
      }
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
      case TimestampType => parseTimestamp(fieldValueStr)
      case _ => fieldValueStr
    }

  /**
    * Parses `yyyy-[m]m-[d]d hh:mm:ss[.f...]` (the format spark-redis writes timestamps in), falling back to
    * ISO-8601 date-times, either local (`2022-10-14T13:33:15.012345`) or with an offset
    * (`2022-10-14T13:33:15.012345Z`, `2022-10-14T13:33:15+02:00`). Like `Timestamp.valueOf`, values
    * without an offset are interpreted in the JVM default time zone.
    */
  private def parseTimestamp(str: String): Timestamp =
    try {
      Timestamp.valueOf(str)
    } catch {
      case e: IllegalArgumentException =>
        Try(Timestamp.valueOf(LocalDateTime.parse(str)))
          .orElse(Try(Timestamp.from(OffsetDateTime.parse(str).toInstant)))
          .getOrElse {
            throw new IllegalArgumentException(s"Cannot parse '$str' as a timestamp, expected " +
              "yyyy-mm-dd hh:mm:ss[.fffffffff] or an ISO-8601 date-time such as 2022-10-14T13:33:15.012345", e)
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
