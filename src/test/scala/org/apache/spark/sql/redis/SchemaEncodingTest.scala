package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets.UTF_8

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.redis.RedisSourceRelation.{decodeSchema, encodeSchema}
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

/**
  * The schema key holds Spark's schema JSON, which every Spark and Scala version reads. Schemas that earlier versions
  * stored Java-serialized are still read with the Spark and Scala versions that wrote them.
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
      new MetadataBuilder().putString("comment", "free text").putLong("max", 200).build())
  ))

  test("a schema is stored as Spark's schema JSON") {
    new String(encodeSchema(schema), UTF_8) shouldBe schema.json
  }

  test("a stored schema reads back as the same schema, metadata and nullability included") {
    decodeSchema(Key, encodeSchema(schema)) shouldBe schema
  }

  test("a schema that an earlier version stored Java-serialized still reads with the same Spark version") {
    decodeSchema(Key, SerializationUtils.serialize(schema)) shouldBe schema
  }

  test("a Java-serialized schema from another Spark version fails with an error that says what to do") {
    val e = the[IllegalStateException] thrownBy decodeSchema(Key, withOtherSerialVersionUid(schema))
    e.getMessage should include(Key)
    e.getMessage should include(".schema()")
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
