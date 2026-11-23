package com.redislabs.provider.redis

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import com.redislabs.provider.redis.SchemaMigration.{Outcome, Result}
import com.redislabs.provider.redis.df.RedisDataframeSuite
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.JavaSerializedSchemaTest.{Schema, Spark31Schema, resource}
import com.redislabs.provider.redis.util.Person
import com.redislabs.provider.redis.util.TestUtils.findCause
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.redis.RedisSourceRelation.schemaKey
import org.apache.spark.sql.types.{DataType, StructType}
import redis.clients.jedis.Jedis

/**
  * [[SchemaMigration]] rewrites the Java-serialized schema keys that earlier versions of spark-redis wrote as JSON, and
  * leaves everything else alone. Each test uses tables of its own, under a prefix of its own, and migrates only those.
  */
trait SchemaMigrationSuite extends RedisDataframeSuite {

  /** A prefix no other test uses, so a test's `tables` pattern matches its own tables only. */
  private def newPrefix(): String = "migrate" + UUID.randomUUID().toString.replace("-", "")

  private def withKey[A](key: String)(body: Jedis => A): A = withConnection(redisConfig.connectionForKey(key))(body)

  private def setValue(key: String, value: Array[Byte]): Unit = withKey(key)(_.set(key.getBytes(UTF_8), value))

  private def value(key: String): Array[Byte] = withKey(key)(_.get(key.getBytes(UTF_8)))

  /**
    * `schema`, Java-serialized as another Spark version would have written it: `StructType`'s serialVersionUID, the 8
    * bytes after its class name, changed.
    */
  private def fromAnotherSparkVersion(schema: StructType): Array[Byte] = {
    val bytes = SerializationUtils.serialize(schema)
    val name = classOf[StructType].getName.getBytes(UTF_8)
    val at = bytes.indexOfSlice(name)
    assert(at > 0, "StructType's class descriptor is not in the stream")
    bytes(at + name.length) = (bytes(at + name.length) ^ 0x01).toByte
    bytes
  }

  // lazy: the SparkSession exists only once beforeAll has run
  private lazy val personSchema: StructType = spark.createDataFrame(Person.data).schema

  test("a table whose schema another Spark version Java-serialized loads after the migration") {
    val table = newPrefix()
    writeDf(table)
    val key = schemaKey(table)
    setValue(key, fromAnotherSparkVersion(personSchema))
    val before = intercept[Exception](loadAndVerifyDf(table))
    findCause[IllegalStateException](before).getMessage should include("SchemaMigration")

    SchemaMigration.migrate(redisConfig, table) shouldBe Seq(Result(key, Outcome.Migrated))
    new String(value(key), UTF_8) shouldBe personSchema.json
    loadAndVerifyDf(table)
  }

  test("migrates the schemas that Spark 3.1.3 wrote on Scala 2.12 and Spark 3.2.1 on Scala 2.12 and 2.13") {
    Seq(
      "spark-3.1.3-scala-2.12.b64" -> Spark31Schema,
      "spark-3.2.1-scala-2.12.b64" -> Schema,
      "spark-3.2.1-scala-2.13.b64" -> Schema
    ).foreach { case (name, expected) =>
      withClue(s"$name:") {
        val table = newPrefix()
        val key = schemaKey(table)
        setValue(key, resource(name))
        SchemaMigration.migrate(redisConfig, table) shouldBe Seq(Result(key, Outcome.Migrated))
        DataType.fromJson(new String(value(key), UTF_8)) shouldBe expected
      }
    }
  }

  test("a dry run reports what it would migrate and changes nothing") {
    val table = newPrefix()
    val key = schemaKey(table)
    val javaSerialized = fromAnotherSparkVersion(personSchema)
    setValue(key, javaSerialized)
    SchemaMigration.migrate(redisConfig, table, dryRun = true) shouldBe Seq(Result(key, Outcome.WouldMigrate))
    value(key) shouldBe javaSerialized
  }

  test("migrates every table that matches the pattern, on every node, and only those") {
    val prefix = newPrefix()
    val tables = (1 to 10).map(i => s"$prefix-$i")
    tables.foreach(table => setValue(schemaKey(table), fromAnotherSparkVersion(personSchema)))
    val outside = newPrefix()
    val outsideValue = fromAnotherSparkVersion(personSchema)
    setValue(schemaKey(outside), outsideValue)

    SchemaMigration.migrate(redisConfig, s"$prefix-*").sortBy(_.key) shouldBe
      tables.map(t => Result(schemaKey(t), Outcome.Migrated)).sortBy(_.key)
    tables.foreach(table => new String(value(schemaKey(table)), UTF_8) shouldBe personSchema.json)
    value(schemaKey(outside)) shouldBe outsideValue
  }

  test("a schema key that already holds JSON is left alone") {
    val table = newPrefix()
    writeDf(table)
    val key = schemaKey(table)
    val json = value(key)
    SchemaMigration.migrate(redisConfig, table) shouldBe Seq(Result(key, Outcome.NotJavaSerialized))
    value(key) shouldBe json
  }

  test("a Java-serialized value that isn't a schema fails, is left alone, and doesn't stop the others") {
    val prefix = newPrefix()
    val bad = schemaKey(s"$prefix-bad")
    val good = schemaKey(s"$prefix-good")
    val badValue = SerializationUtils.serialize("not a schema")
    setValue(bad, badValue)
    setValue(good, fromAnotherSparkVersion(personSchema))

    val results = SchemaMigration.migrate(redisConfig, s"$prefix-*").map(r => r.key -> r.outcome).toMap
    results(good) shouldBe Outcome.Migrated
    results(bad) match {
      case Outcome.Failed(reason) => reason should include("it holds a string, not a table schema")
      case other => fail(s"expected a failure, got $other")
    }
    value(bad) shouldBe badValue
  }

  test("keeps the schema key's TTL") {
    val table = newPrefix()
    val key = schemaKey(table)
    setValue(key, fromAnotherSparkVersion(personSchema))
    withKey(key)(_.expire(key, 1000L))
    SchemaMigration.migrate(redisConfig, table) shouldBe Seq(Result(key, Outcome.Migrated))
    withKey(key)(_.ttl(key).longValue) should (be > 0L and be <= 1000L)
  }

  test("the command line prints each key and a summary, and its exit code says whether anything failed") {
    def run(args: String*): (Int, String) = {
      val bytes = new ByteArrayOutputStream()
      val out = new PrintStream(bytes, true, "UTF-8")
      val code = SchemaMigration.run(args, conf, out)
      (code, new String(bytes.toByteArray, UTF_8))
    }
    val prefix = newPrefix()
    val good = schemaKey(s"$prefix-good")
    setValue(good, fromAnotherSparkVersion(personSchema))

    val (dryRunCode, dryRunOutput) = run("--dry-run", "--tables", s"$prefix-*")
    dryRunCode shouldBe 0
    dryRunOutput should include(s"would migrate\t$good")
    dryRunOutput should include("1 schema keys (would migrate: 1)")

    val (code, output) = run("--tables", s"$prefix-*")
    code shouldBe 0
    output should include(s"migrated\t$good")

    val bad = schemaKey(s"$prefix-bad")
    setValue(bad, SerializationUtils.serialize("not a schema"))
    val (failedCode, failedOutput) = run("--tables", s"$prefix-*")
    failedCode shouldBe 1
    failedOutput should include(s"\t$bad")
    failedOutput should include("not Java-serialized: 1")

    val (usageCode, usage) = run("--bogus")
    usageCode shouldBe 2
    usage should include("Usage:")
  }
}
