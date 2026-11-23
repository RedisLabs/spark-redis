package com.redislabs.provider.redis.util

import org.apache.spark.sql.types.{DataType, DateType, TimestampType}

/**
  * A value read from Redis that can't be parsed as its column's type (#358). The message names the Redis key (and
  * the stream entry, for a stream), the field, the value and the type.
  *
  * Each implementation extends the exception the parser itself throws, so code that catches that keeps working:
  * [[FieldNumberFormatException]] for numbers, [[FieldFormatException]] for dates and timestamps. The parser's
  * exception is the cause.
  */
sealed trait FieldParseException { self: IllegalArgumentException =>
  def field: String
  def value: String
  def dataType: DataType
  def key: Option[String]
  def entryId: Option[String]

  /** The same failure, naming the Redis key (and stream entry) the value was read from. */
  def withKey(key: String, entryId: Option[String]): IllegalArgumentException with FieldParseException
}

object FieldParseException {

  /** Wraps the exception a parser threw for the value of a field. */
  private[util] def apply(field: String, value: String, dataType: DataType,
                          parseError: IllegalArgumentException): IllegalArgumentException with FieldParseException =
    parseError match {
      case e: NumberFormatException => new FieldNumberFormatException(field, value, dataType, None, None, e)
      case e => new FieldFormatException(field, value, dataType, None, None, e)
    }

  /**
    * Runs body, adding the Redis key (and stream entry) to a parse failure it throws. The DataFrame and stream
    * sources wrap each row they parse in this.
    */
  def withKey[T](key: String, entryId: Option[String] = None)(body: => T): T =
    try {
      body
    } catch {
      case e: FieldParseException if e.key.isEmpty => throw e.withKey(key, entryId)
    }

  private[util] def message(field: String, value: String, dataType: DataType, key: Option[String],
                            entryId: Option[String], parseError: IllegalArgumentException): String = {
    val where = key.map(k => s" in key '$k'" + entryId.map(id => s", entry $id").getOrElse("")).getOrElse("")
    // java.sql.Date.valueOf throws without a message
    val reason = dataType match {
      case DateType => Some("expected yyyy-[m]m-[d]d")
      case TimestampType =>
        Some("expected yyyy-[m]m-[d]d hh:mm:ss[.fffffffff] or an ISO-8601 date-time such as 2022-10-14T13:33:15")
      case _ => Option(parseError.getMessage)
    }
    s"Can't parse value '$value' of field '$field'$where as ${dataType.typeName}" + reason.map(": " + _).getOrElse("")
  }
}

/** A number that can't be parsed. See [[FieldParseException]]. */
final class FieldNumberFormatException private[util] (val field: String, val value: String, val dataType: DataType,
                                                      val key: Option[String], val entryId: Option[String],
                                                      parseError: NumberFormatException)
    extends NumberFormatException(FieldParseException.message(field, value, dataType, key, entryId, parseError))
    with FieldParseException {

  initCause(parseError)

  override def withKey(key: String, entryId: Option[String]): FieldNumberFormatException = {
    val e = new FieldNumberFormatException(field, value, dataType, Some(key), entryId, parseError)
    e.setStackTrace(getStackTrace)
    e
  }
}

/** A date or timestamp that can't be parsed. See [[FieldParseException]]. */
final class FieldFormatException private[util] (val field: String, val value: String, val dataType: DataType,
                                                val key: Option[String], val entryId: Option[String],
                                                parseError: IllegalArgumentException)
    extends IllegalArgumentException(FieldParseException.message(field, value, dataType, key, entryId, parseError),
      parseError)
    with FieldParseException {

  override def withKey(key: String, entryId: Option[String]): FieldFormatException = {
    val e = new FieldFormatException(field, value, dataType, Some(key), entryId, parseError)
    e.setStackTrace(getStackTrace)
    e
  }
}
