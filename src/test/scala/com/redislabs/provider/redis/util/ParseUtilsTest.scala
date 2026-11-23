package com.redislabs.provider.redis.util

import java.sql.{Date, Timestamp}
import java.time.Instant

import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

class ParseUtilsTest extends FunSuite with Matchers {

  private val timestampSchema = StructType(Array(StructField("time", TimestampType)))

  private def parseTimestamp(value: String): Any =
    ParseUtils.parseFields(Map("time" -> value), timestampSchema).head

  test("parse primitive, string, date and timestamp fields") {
    val schema = StructType(Array(
      StructField("int", IntegerType),
      StructField("long", LongType),
      StructField("double", DoubleType),
      StructField("bool", BooleanType),
      StructField("str", StringType),
      StructField("date", DateType),
      StructField("time", TimestampType),
      StructField("missing", StringType)
    ))
    val values = Map("int" -> "1", "long" -> "2", "double" -> "3.5", "bool" -> "true", "str" -> "s",
      "date" -> "2022-10-14", "time" -> "2022-10-14 13:33:15.012345", "missing" -> null)
    ParseUtils.parseFields(values, schema) shouldBe Array(1, 2L, 3.5d, true, "s",
      Date.valueOf("2022-10-14"), Timestamp.valueOf("2022-10-14 13:33:15.012345"), null)
  }

  test("parse ISO-8601 local date-time timestamps (#358)") {
    parseTimestamp("2022-10-14T13:33:15.012345") shouldBe Timestamp.valueOf("2022-10-14 13:33:15.012345")
    parseTimestamp("2022-10-14T13:33:15") shouldBe Timestamp.valueOf("2022-10-14 13:33:15")
    parseTimestamp("2022-10-14T13:33") shouldBe Timestamp.valueOf("2022-10-14 13:33:00")
  }

  test("parse ISO-8601 timestamps with an offset") {
    parseTimestamp("2022-10-14T13:33:15.012345Z") shouldBe
      Timestamp.from(Instant.parse("2022-10-14T13:33:15.012345Z"))
    parseTimestamp("2022-10-14T15:33:15.012345+02:00") shouldBe
      Timestamp.from(Instant.parse("2022-10-14T13:33:15.012345Z"))
  }

  test("reject unparseable timestamps") {
    val e = the[IllegalArgumentException] thrownBy parseTimestamp("14/10/2022 13:33")
    e.getMessage should include("14/10/2022 13:33")
  }
}
