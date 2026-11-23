package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.RedisEndpoint
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.MonitoredCommands
import com.redislabs.provider.redis.util.TestUtils.{generateTableName, interceptSparkErr}
import org.apache.spark.SparkException
import org.apache.spark.sql.redis.{RedisFormat, SqlOptionKeyColumn, SqlOptionTTL, SqlOptionTableName}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.Matchers
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

// scalastyle:off multiple.string.literals magic.number

/**
  * The `write.mode` option of hash-model DataFrame writes (#351, spec §2.4).
  *
  *  - `merge`, the default, is today's behaviour: fields the new row doesn't have, or has as null, stay in Redis.
  *  - `replace` makes the stored hash exactly the new row, in one `MULTI`/`EXEC` per key that also sets the TTL.
  *  - Any other value fails, naming the option, before anything is written.
  *
  * The option name and its values are spelled out, not taken from constants, so the tests pin what users type.
  */
trait HashWriteModeSuite extends RedisDataframeSuite with MonitoredCommands with Matchers {

  private val WriteMode = "write.mode"
  private val Ttl = 3600

  private def schema(columns: String*): StructType = StructType(columns.map(StructField(_, StringType)))

  private val IdAB = schema("id", "a", "b")
  private val IdAC = schema("id", "a", "c")

  private def write(table: String, schema: StructType, rows: Seq[Row], options: Map[String, String] = Map(),
                    mode: SaveMode = SaveMode.Append): Unit =
    spark.createDataFrame(sc.parallelize(rows, 2), schema)
      .write.format(RedisFormat)
      .option(SqlOptionTableName, table)
      .option(SqlOptionKeyColumn, "id")
      .options(options)
      .mode(mode)
      .save()

  private def hash(key: String): Map[String, String] =
    withConnection(redisConfig.connectionForKey(key))(_.hgetAll(key).asScala.toMap)

  private def ttl(key: String): Long = withConnection(redisConfig.connectionForKey(key))(_.ttl(key).longValue)

  private def exists(key: String): Boolean =
    withConnection(redisConfig.connectionForKey(key))(_.exists(key).booleanValue)

  for ((name, options) <- Seq("by default" -> Map.empty[String, String], "with merge" -> Map(WriteMode -> "merge"))) {
    test(s"write.mode: $name, fields missing from the new row or null in it stay in Redis") {
      val t = generateTableName("write-mode-merge")
      write(t, IdAB, Seq(Row("1", "a1", "b1"), Row("2", "a2", "b2")))
      write(t, IdAC, Seq(Row("1", "a1-new", "c1"), Row("2", null, "c2")), options)
      hash(s"$t:1") shouldBe Map("a" -> "a1-new", "b" -> "b1", "c" -> "c1")
      hash(s"$t:2") shouldBe Map("a" -> "a2", "b" -> "b2", "c" -> "c2")
    }
  }

  test("write.mode: replace removes fields missing from the new row or null in it") {
    val t = generateTableName("write-mode-replace")
    write(t, IdAB, Seq(Row("1", "a1", "b1"), Row("2", "a2", "b2"), Row("3", "a3", "b3")))
    write(t, IdAC, Seq(Row("1", "a1-new", "c1"), Row("2", null, "c2")), Map(WriteMode -> "replace"))
    hash(s"$t:1") shouldBe Map("a" -> "a1-new", "c" -> "c1")
    hash(s"$t:2") shouldBe Map("c" -> "c2")
    // a row the second write doesn't have is left alone
    hash(s"$t:3") shouldBe Map("a" -> "a3", "b" -> "b3")
  }

  test("write.mode: replace with a row whose values are all null removes the key") {
    val t = generateTableName("write-mode-replace-nulls")
    write(t, IdAB, Seq(Row("1", "a1", "b1"), Row("2", "a2", "b2")))
    write(t, IdAB, Seq(Row("1", null, null)), Map(WriteMode -> "replace"))
    exists(s"$t:1") shouldBe false
    hash(s"$t:2") shouldBe Map("a" -> "a2", "b" -> "b2")
  }

  test("write.mode: replace with ttl sets the TTL") {
    val t = generateTableName("write-mode-replace-ttl")
    write(t, IdAB, Seq(Row("1", "a1", "b1")))
    write(t, IdAC, Seq(Row("1", "a1-new", "c1")), Map(WriteMode -> "replace", SqlOptionTTL -> Ttl.toString))
    hash(s"$t:1") shouldBe Map("a" -> "a1-new", "c" -> "c1")
    ttl(s"$t:1") should (be > 0L and be <= Ttl.toLong)
  }

  test("write.mode: replace without ttl removes the TTL the replaced row had") {
    val t = generateTableName("write-mode-replace-no-ttl")
    write(t, IdAB, Seq(Row("1", "a1", "b1")), Map(SqlOptionTTL -> Ttl.toString))
    write(t, IdAC, Seq(Row("1", "a1-new", "c1")), Map(WriteMode -> "replace"))
    hash(s"$t:1") shouldBe Map("a" -> "a1-new", "c" -> "c1")
    ttl(s"$t:1") shouldBe -1L
  }

  test("write.mode: replace writes each row in one MULTI/EXEC that also sets its TTL") {
    val t = generateTableName("write-mode-replace-atomic")
    val ids = (1 to 20).map(_.toString)
    write(t, IdAB, ids.map(i => Row(i, s"a$i", s"b$i")))
    val commands = monitored {
      write(t, IdAC, ids.map(i => Row(i, s"a$i-new", s"c$i")),
        Map(WriteMode -> "replace", SqlOptionTTL -> Ttl.toString))
    }
    ids.foreach { i =>
      val key = s"$t:$i"
      withClue(s"commands on $key: ") {
        val onKey = commands.filter(_.key.contains(key))
        onKey.map(_.name) shouldBe Seq("UNLINK", "HSET", "EXPIRE")
        onKey.map(_.client).distinct should have size 1
        // the client's own command stream, around the UNLINK: one transaction holding exactly this row's writes
        val stream = commands.filter(_.client == onKey.head.client)
        val unlink = stream.indexOf(onKey.head)
        stream.slice(unlink - 1, unlink + 4).map(_.normalized) shouldBe Seq(
          Seq("MULTI"),
          Seq("UNLINK", key),
          Seq("HSET", key, Map("a" -> s"a$i-new", "c" -> s"c$i")),
          Seq("EXPIRE", key, Ttl.toString),
          Seq("EXEC"))
      }
    }
  }

  test("write.mode: replace fails the write when Redis refuses it, and leaves the stored row whole") {
    val t = generateTableName("write-mode-replace-refused")
    write(t, IdAB, Seq(Row("1", "a1", "b1")))
    // a user that may do everything except UNLINK, so Redis rejects the transaction when it is queued
    val refused = "write-mode-no-unlink"
    primaries.foreach { p =>
      withConnection(p.connect())(_.aclSetUser(refused, "on", s">$userPassword", "~*", "&*", "+@all", "-unlink"))
    }
    try {
      val e = interceptSparkErr[SparkException] {
        write(t, IdAC, Seq(Row("1", "a1-new", "c1")),
          Map(WriteMode -> "replace", "user" -> refused, "password" -> userPassword))
      }
      e.getMessage should include("NOPERM")
    } finally {
      primaries.foreach(p => withConnection(p.connect())(_.aclDelUser(refused)))
    }
    hash(s"$t:1") shouldBe Map("a" -> "a1", "b" -> "b1")
  }

  for (mode <- Seq(SaveMode.Append, SaveMode.Overwrite)) {
    test(s"write.mode: any other value fails, naming the option, before anything is written ($mode)") {
      val t = generateTableName("write-mode-invalid")
      write(t, IdAB, Seq(Row("1", "a1", "b1")))
      val e = intercept[IllegalArgumentException] {
        write(t, IdAC, Seq(Row("1", "a1-new", "c1"), Row("2", "a2", "c2")), Map(WriteMode -> "upsert"), mode)
      }
      e.getMessage should include(WriteMode)
      e.getMessage should include("upsert")
      hash(s"$t:1") shouldBe Map("a" -> "a1", "b" -> "b1")
      exists(s"$t:2") shouldBe false
    }
  }
}
