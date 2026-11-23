package com.redislabs.provider.redis.util

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

/**
  * [[JavaSerializedSchema]] reads Java-serialized `StructType`s written by any Spark and Scala version.
  *
  * The resources under `java-serialized-schemas/` hold [[JavaSerializedSchemaTest.Schema]], Java-serialized by Spark
  * 3.2.1 on Scala 2.12 and on Scala 2.13, and by Spark 3.1.3 on Scala 2.12 (without the interval types, which 3.1
  * lacks). Their `StructType` serialVersionUIDs differ from each other and from every later Spark version's, so Java
  * deserialization can't read them on any other build. The other tests serialize with the Spark and Scala versions
  * this build runs on, which CI varies.
  */
class JavaSerializedSchemaTest extends FunSuite with Matchers {

  import JavaSerializedSchemaTest._

  test("reads a schema that Spark 3.1.3 wrote on Scala 2.12") {
    JavaSerializedSchema.read(resource("spark-3.1.3-scala-2.12.b64")) shouldBe Spark31Schema
  }

  test("reads a schema that Spark 3.2.1 wrote on Scala 2.12") {
    JavaSerializedSchema.read(resource("spark-3.2.1-scala-2.12.b64")) shouldBe Schema
  }

  test("reads a schema that Spark 3.2.1 wrote on Scala 2.13") {
    JavaSerializedSchema.read(resource("spark-3.2.1-scala-2.13.b64")) shouldBe Schema
  }

  test("the resources carry serialVersionUIDs that Java deserialization rejects on other builds") {
    structTypeSerialVersionUid(resource("spark-3.1.3-scala-2.12.b64")) shouldBe 3211562616122276303L
    structTypeSerialVersionUid(resource("spark-3.2.1-scala-2.12.b64")) shouldBe -3919263063959084621L
    structTypeSerialVersionUid(resource("spark-3.2.1-scala-2.13.b64")) shouldBe 2945908649928691995L
  }

  test("reads a schema that this Spark and Scala version wrote") {
    JavaSerializedSchema.read(SerializationUtils.serialize(Schema)) shouldBe Schema
  }

  test("keeps nullability and metadata exactly") {
    val read = JavaSerializedSchema.read(SerializationUtils.serialize(Schema))
    read.fields.map(f => (f.name, f.nullable)) shouldBe Schema.fields.map(f => (f.name, f.nullable))
    read("mLarge").metadata shouldBe LargeMetadata
    read("m1").metadata.getString("comment") shouldBe "one entry"
    read("s").metadata shouldBe Metadata.empty
  }

  test("reads metadata maps of every size, which Scala serializes in different forms") {
    (0 to 6).foreach { size =>
      val metadata = (1 to size).foldLeft(new MetadataBuilder)((b, i) => b.putLong(s"k$i", i.toLong)).build()
      val schema = StructType(Seq(StructField("f", StringType, nullable = true, metadata)))
      withClue(s"$size entries:") {
        JavaSerializedSchema.read(SerializationUtils.serialize(schema)) shouldBe schema
      }
    }
  }

  test("reads deeply nested types") {
    val nested = (1 to 20).foldLeft[DataType](IntegerType)((t, i) =>
      StructType(Seq(StructField(s"level$i", ArrayType(MapType(StringType, t))))))
    val schema = StructType(Seq(StructField("root", nested)))
    JavaSerializedSchema.read(SerializationUtils.serialize(schema)) shouldBe schema
  }

  test("reads a user-defined type") {
    val schema = StructType(Seq(StructField("point", new TestPointUdt)))
    JavaSerializedSchema.read(SerializationUtils.serialize(schema)) shouldBe schema
  }

  test("a data type that doesn't exist in this Spark version fails, naming it") {
    // the same stream with StringType$ renamed to a type of the same length that no Spark version has
    val renamed = replaceSameLength(SerializationUtils.serialize(StructType(Seq(StructField("s", StringType)))),
      "org.apache.spark.sql.types.StringType$", "org.apache.spark.sql.types.StrangType$")
    val e = the[IllegalArgumentException] thrownBy JavaSerializedSchema.read(renamed)
    e.getMessage should include("org.apache.spark.sql.types.StrangType$")
    e.getMessage should include("doesn't exist in this Spark version")
  }

  test("a class that isn't a Spark data type is rejected, not used as one") {
    // StringType$ renamed to existing classes of the same length that aren't data types: StructType's companion
    // object, and StructField, which isn't a user-defined type
    Seq("org.apache.spark.sql.types.StructType$", "org.apache.spark.sql.types.StructField").foreach { name =>
      val renamed = replaceSameLength(SerializationUtils.serialize(StructType(Seq(StructField("s", StringType)))),
        "org.apache.spark.sql.types.StringType$", name)
      val e = the[IllegalArgumentException] thrownBy JavaSerializedSchema.read(renamed)
      e.getMessage should include(s"$name is not a Spark data type")
    }
  }

  test("a stream that holds something other than a StructType fails") {
    val e = the[IllegalArgumentException] thrownBy JavaSerializedSchema.read(
      SerializationUtils.serialize(new java.util.ArrayList[String]()))
    e.getMessage should include("java.util.ArrayList")
    an[IllegalArgumentException] should be thrownBy JavaSerializedSchema.read(SerializationUtils.serialize("text"))
    an[IllegalArgumentException] should be thrownBy JavaSerializedSchema.read(SerializationUtils.serialize(StringType))
  }

  test("a truncated stream fails") {
    val bytes = SerializationUtils.serialize(Schema)
    Seq(2, 10, bytes.length / 2, bytes.length - 1).foreach { length =>
      withClue(s"$length of ${bytes.length} bytes:") {
        an[IllegalArgumentException] should be thrownBy JavaSerializedSchema.read(bytes.take(length))
      }
    }
  }

  test("bytes that aren't a Java serialization stream fail") {
    an[IllegalArgumentException] should be thrownBy JavaSerializedSchema.read("not a schema".getBytes(UTF_8))
    an[IllegalArgumentException] should be thrownBy JavaSerializedSchema.read(Array[Byte]())
  }

  /** The 8 bytes after `StructType`'s class name in its class descriptor. */
  private def structTypeSerialVersionUid(bytes: Array[Byte]): Long = {
    val name = classOf[StructType].getName.getBytes(UTF_8)
    val at = bytes.indexOfSlice(name)
    assert(at > 0, "StructType's class descriptor is not in the stream")
    ByteBuffer.wrap(bytes, at + name.length, 8).getLong
  }

  private def replaceSameLength(bytes: Array[Byte], from: String, to: String): Array[Byte] = {
    require(from.length == to.length)
    val at = bytes.indexOfSlice(from.getBytes(UTF_8))
    assert(at > 0, s"$from is not in the stream")
    val copy = bytes.clone()
    to.getBytes(UTF_8).copyToArray(copy, at)
    copy
  }
}

object JavaSerializedSchemaTest {

  /** A resource under `java-serialized-schemas/`: a Java-serialized schema, in base64. */
  def resource(name: String): Array[Byte] = {
    val source = Source.fromInputStream(getClass.getResourceAsStream(s"/java-serialized-schemas/$name"), "UTF-8")
    try Base64.getMimeDecoder.decode(source.mkString) finally source.close()
  }

  /** What the Spark 3.1.3 resource holds: [[Schema]] without the interval types, which Spark 3.1 lacks. */
  def Spark31Schema: StructType = StructType(Schema.fields.filterNot(f => Set("dti", "ymi").contains(f.name)))

  private val NestedMetadata = new MetadataBuilder().putString("unit", "cm").putLong("scale", 2).build()

  /** More than four entries, so Scala serializes the map through its serialization proxy. */
  val LargeMetadata: Metadata = new MetadataBuilder()
    .putString("comment", "free text").putLong("max", 200L).putDouble("ratio", 0.5).putBoolean("pii", true)
    .putMetadata("nested", NestedMetadata).putLongArray("longs", Array(1L, 2L, 3L))
    .putDoubleArray("doubles", Array(1.5, 2.5)).putBooleanArray("flags", Array(true, false))
    .putStringArray("names", Array("a", "b")).putMetadataArray("children", Array(NestedMetadata, Metadata.empty))
    .build()

  /** The schema the Spark 3.2.1 resources hold: every kind of data type the reader supports, and metadata. */
  val Schema: StructType = StructType(Seq(
    StructField("s", StringType, nullable = false),
    StructField("i", IntegerType), StructField("l", LongType), StructField("sh", ShortType),
    StructField("b", ByteType), StructField("f", FloatType), StructField("d", DoubleType),
    StructField("bool", BooleanType), StructField("bin", BinaryType), StructField("date", DateType),
    StructField("ts", TimestampType), StructField("n", NullType), StructField("ci", CalendarIntervalType),
    StructField("dec", DecimalType(10, 2)), StructField("c", CharType(5)), StructField("vc", VarcharType(20)),
    StructField("dti", DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND)),
    StructField("ymi", YearMonthIntervalType()),
    StructField("arr", ArrayType(StringType, containsNull = false)),
    StructField("map", MapType(StringType, ArrayType(DoubleType), valueContainsNull = false)),
    StructField("st", StructType(Seq(StructField("city", StringType), StructField("zip", LongType, nullable = false)))),
    StructField("m1", StringType, nullable = true, new MetadataBuilder().putString("comment", "one entry").build()),
    StructField("mLarge", IntegerType, nullable = true, LargeMetadata)
  ))
}

/** A user-defined type with a no-arg constructor, as the reader requires. */
class TestPointUdt extends UserDefinedType[TestPoint] {
  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)
  override def serialize(p: TestPoint): Any = throw new UnsupportedOperationException
  override def deserialize(datum: Any): TestPoint = throw new UnsupportedOperationException
  override def userClass: Class[TestPoint] = classOf[TestPoint]
}

class TestPoint(val x: Double, val y: Double)
