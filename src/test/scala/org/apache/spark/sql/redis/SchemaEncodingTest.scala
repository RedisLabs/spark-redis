package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets.UTF_8

import com.redislabs.provider.redis.util.JavaSerializedSchemaTest.{Spark31Schema, resource, Schema => SchemaOfResources}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.redis.RedisSourceRelation.{decodeSchema, encodeSchema}
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}


/**
  * The schema key holds Spark's schema JSON, which every Spark and Scala version reads. Schemas that earlier versions
  * stored Java-serialized are still read, whichever Spark and Scala versions wrote them, without deserializing them.
  */
class SchemaEncodingTest extends FunSuite with Matchers {

  private val Key = "_spark:person:schema"

  private val schema = StructType(Seq(
    StructField("name", StringType, nullable = false),
    StructField("age", IntegerType),
    StructField("salary", DecimalType(10, 2)),
    StructField("born", DateType),
    StructField("updated", TimestampType),
    StructField("tags", ArrayType(StringType, containsNull = false)),
    StructField("scores", MapType(StringType, DoubleType)),
    StructField("address", StructType(Seq(StructField("city", StringType), StructField("zip", LongType)))),
    StructField("photo", BinaryType),
    StructField("note", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "free text").putLong("max", 200).build()),
    // metadata with more than four entries, arrays and nested metadata, which Scala serializes with other classes
    StructField("rated", DoubleType, nullable = true,
      new MetadataBuilder().putDouble("min", 0.5).putBoolean("public", true).putLongArray("bins", Array(1L, 2L))
        .putDoubleArray("weights", Array(0.5)).putBooleanArray("flags", Array(true)).putStringArray("tags", Array("a"))
        .putMetadata("source", new MetadataBuilder().putString("system", "crm").build())
        .putMetadataArray("history", Array(Metadata.empty)).build())
  ))

  test("a schema is stored as Spark's schema JSON") {
    new String(encodeSchema(schema), UTF_8) shouldBe schema.json
  }

  test("a stored schema reads back as the same schema, metadata and nullability included") {
    decodeSchema(Key, encodeSchema(schema)) shouldBe schema
  }

  test("a schema that an earlier version stored Java-serialized still reads") {
    decodeSchema(Key, SerializationUtils.serialize(schema)) shouldBe schema
  }

  test("a schema that Spark 3.1 or 3.2 Java-serialized on Scala 2.12 or 2.13 reads on this Spark and Scala version") {
    Seq("spark-3.1.3-scala-2.12.b64" -> Spark31Schema,
      "spark-3.2.1-scala-2.12.b64" -> SchemaOfResources,
      "spark-3.2.1-scala-2.13.b64" -> SchemaOfResources).foreach { case (name, expected) =>
      withClue(name) {
        decodeSchema(Key, resource(name)) shouldBe expected
      }
    }
  }

  test("a Java-serialized schema of every data type reads back, with the field lookups Spark caches in it") {
    schemasOfEveryType.foreach { schema =>
      withClue(schema.simpleString) {
        decodeSchema(Key, SerializationUtils.serialize(schema)) shouldBe schema
      }
    }
  }

  test("a Java-serialized schema key holding another class fails, naming the key and the class") {
    val e = the[IllegalStateException] thrownBy
      decodeSchema(Key, SerializationUtils.serialize(new java.util.HashMap[String, String]()))
    e.getMessage should include(Key)
    e.getMessage should include("java.util.HashMap")
    e.getMessage should include(".schema()")
  }

  test("a Java-serialized schema key that declares a huge array or nests too deep fails fast, naming the key") {
    // the elements of nested ArrayLists are the annotations of their writeObject, which take the most stack to read
    val nestedLists = RestrictedDeserializationTest.onThread(stackSize = 256L << 20) {
      val root = new java.util.ArrayList[AnyRef]()
      (1 to 5000).foldLeft(root) { (list, _) => val next = new java.util.ArrayList[AnyRef](); list.add(next); next }
      SerializationUtils.serialize(root)
    }.fold(e => throw e, identity)
    Seq("huge array" -> RestrictedDeserializationTest.hugeArrayRow,
      "nested arrays" -> RestrictedDeserializationTest.deeplyNestedArrays(100000),
      "nested lists" -> nestedLists).foreach { case (what, bytes) =>
      withClue(what) {
        // on a 512 KB stack, which reading 200 levels of nested lists on the caller's thread would overflow
        val result = RestrictedDeserializationTest.onThread(stackSize = 512L << 10)(decodeSchema(Key, bytes))
        result.left.get shouldBe an[IllegalStateException]
        result.left.get.getMessage should include(Key)
      }
    }
  }

  test("a Java-serialized value that is not a schema is not a schema") {
    val e = the[IllegalStateException] thrownBy decodeSchema(Key, SerializationUtils.serialize(Integer.valueOf(1)))
    e.getMessage should include(Key)
  }

  test("a Java-serialized schema from another Spark version, with another serialVersionUID, reads") {
    decodeSchema(Key, withOtherSerialVersionUid(schema)) shouldBe schema
  }

  test("a value that is JSON but not a struct type is not a schema") {
    val e = the[IllegalStateException] thrownBy decodeSchema(Key, StringType.json.getBytes(UTF_8))
    e.getMessage should include(Key)
  }

  test("a value that is neither JSON nor Java-serialized is not a schema") {
    val e = the[IllegalStateException] thrownBy decodeSchema(Key, "not a schema".getBytes(UTF_8))
    e.getMessage should include(Key)
  }

  /**
    * Schemas with every data type of this Spark version, and with 0 to 6 fields and more, since Scala serializes
    * small sets and maps with other classes than large ones. Spark caches a struct's field names and its lookups by
    * name in fields that Java serialization writes, once anything has looked a field up, so every lookup is made.
    */
  private def schemasOfEveryType: Seq[StructType] = {
    import DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
    import YearMonthIntervalType.{MONTH, YEAR}
    val atomic = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType(10, 2),
      DecimalType(38, 18), StringType, CharType(5), VarcharType(20), BinaryType, DateType, TimestampType, NullType,
      CalendarIntervalType, DayTimeIntervalType(DAY, SECOND), DayTimeIntervalType(HOUR, MINUTE),
      DayTimeIntervalType(SECOND, SECOND), YearMonthIntervalType(YEAR, MONTH), YearMonthIntervalType(MONTH, MONTH))
    // TIMESTAMP_NTZ is a Spark 3.4 type, which this build may not compile against
    val ntz = scala.util.Try(DataType.fromJson("\"timestamp_ntz\"")).toOption.toSeq
    val types = atomic ++ ntz ++ Seq(ArrayType(CharType(3)), MapType(StringType, DayTimeIntervalType()),
      StructType(Seq(StructField("c", VarcharType(3)), StructField("i", YearMonthIntervalType()))))
    val all = StructType(types.zipWithIndex.map { case (t, i) => StructField(s"f$i", t) })
    ((0 to 6).map(n => StructType(all.fields.take(n))) :+ all).map { schema =>
      def lookUp(struct: StructType): Unit = {
        struct.fieldNames.foreach { name => struct.fieldIndex(name); struct(name) }
        struct(struct.fieldNames.toSet)
        struct.fields.map(_.dataType).foreach { case nested: StructType => lookUp(nested); case _ => }
      }
      lookUp(schema)
      schema
    }
  }

  /**
    * The Java serialization of `schema`, with `StructType`'s serialVersionUID changed, as another Spark version
    * would have written it. In the stream, a class descriptor's 8-byte serialVersionUID follows its class name.
    */
  private def withOtherSerialVersionUid(schema: StructType): Array[Byte] = {
    val bytes = SerializationUtils.serialize(schema)
    val name = classOf[StructType].getName.getBytes(UTF_8)
    val at = bytes.indexOfSlice(name)
    assert(at > 0, "StructType's class descriptor is not in the stream")
    val uid = at + name.length
    bytes(uid) = (bytes(uid) ^ 0x01).toByte
    bytes
  }
}
