package com.redislabs.provider.redis.util

import java.sql.{Date, Timestamp}
import java.time.Instant

import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

/**
  * #358: a value that can't be parsed as its column's type fails with an error that names the field, the value and
  * the type, and keeps the exception type the parser has always thrown.
  */
class ParseUtilsTest extends FunSuite with Matchers {

  private val Field = "col_358"

  private def parse(dataType: DataType, value: String): Any =
    ParseUtils.parseFields(Map(Field -> value), StructType(Array(StructField(Field, dataType))))(0)

  test("parses a valid value of every type") {
    parse(ByteType, "2") shouldBe (2: Byte)
    parse(ShortType, "7") shouldBe (7: Short)
    parse(IntegerType, "1") shouldBe 1
    parse(LongType, "3") shouldBe 3L
    parse(FloatType, "4.2") shouldBe 4.2f
    parse(DoubleType, "5.3") shouldBe 5.3d
    parse(BooleanType, "true") shouldBe true
    parse(BooleanType, "false") shouldBe false
    parse(DateType, "2022-10-14") shouldBe Date.valueOf("2022-10-14")
    parse(TimestampType, "2022-10-14 13:33:15.012345") shouldBe Timestamp.valueOf("2022-10-14 13:33:15.012345")
    parse(StringType, "str8") shouldBe "str8"
  }

  test("a missing value reads as null for every type") {
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, BooleanType, DateType, TimestampType,
      StringType).foreach { dataType =>
      parse(dataType, null) shouldBe (null: Any)
    }
  }

  // (type, a value it can't parse, the exception the parser throws for it)
  private val badValues = Seq[(DataType, String, Class[_ <: IllegalArgumentException])](
    (ByteType, "300", classOf[NumberFormatException]),
    (ShortType, "7s", classOf[NumberFormatException]),
    (IntegerType, "thirty", classOf[NumberFormatException]),
    (IntegerType, "", classOf[NumberFormatException]),
    (LongType, "3.5", classOf[NumberFormatException]),
    (FloatType, "4,2", classOf[NumberFormatException]),
    (DoubleType, "five", classOf[NumberFormatException]),
    (DateType, "2022/10/14", classOf[IllegalArgumentException]),
    (TimestampType, "14/10/2022 13:33", classOf[IllegalArgumentException])
  )

  badValues.foreach { case (dataType, value, parserException) =>
    test(s"a bad ${dataType.typeName} value '$value' names the field, the value and the type") {
      val e = intercept[IllegalArgumentException] {
        parse(dataType, value)
      }
      withClue(s"${e.getClass.getName} is not a ${parserException.getName}:") {
        parserException.isInstance(e) shouldBe true
      }
      e.getMessage should include(s"'$Field'")
      e.getMessage should include(s"'$value'")
      e.getMessage should include(dataType.typeName)
      withClue("the parser's own exception is kept as the cause:") {
        parserException.isInstance(e.getCause) shouldBe true
      }
    }
  }

  // ISO-8601 timestamps parse, and an unparseable one names its value.

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
