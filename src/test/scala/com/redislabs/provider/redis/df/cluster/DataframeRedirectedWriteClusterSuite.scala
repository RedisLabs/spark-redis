package com.redislabs.provider.redis.df.cluster

import java.util.UUID

import scala.collection.JavaConverters._

import com.redislabs.provider.redis.SparkRedisSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.RedirectedWrites
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.redis.{RedisFormat, RedisSourceRelation, SqlOptionKeyColumn, SqlOptionModel,
  SqlOptionModelBinary, SqlOptionTTL, SqlOptionTableName, SqlOptionWriteMode, SqlOptionWriteModeReplace}
import org.scalatest.Matchers
import redis.clients.jedis.Jedis
import redis.clients.jedis.util.JedisClusterCRC16

/**
  * The DataFrame source writes rows to a slot that moved to another primary after the relation read the topology
  * (`MOVED`), or that is being migrated (`ASK`). Every row must still be stored, and an overwrite must still remove
  * the old rows: #347 itself.
  *
  * A `df.write` builds a new relation, which reads the topology when the write starts. A temporary view keeps its
  * relation, and the topology the relation read, for as long as the view exists, so the `MOVED` tests create the
  * view before the slot moves and then `INSERT` into it.
  *
  * The table name has no hash tag, so the schema key (`_spark:<table>:schema`) stays in a slot that doesn't move;
  * the row keys (`<table>:<id>`) take the suite's slot from the hash tag in each id.
  *
  * `write.mode = replace`, and the hash model with a `ttl`, write each row in a `MULTI`/`EXEC`, and the binary model
  * with a `ttl` writes a slot's rows in one `MSETEX` where the server has it. They must follow the redirects too.
  */
class DataframeRedirectedWriteClusterSuite extends SparkRedisSuite with RedisClusterEnv with RedirectedWrites
    with Matchers {

  private val HashTag = "{redirected-df}"
  private val Slot = JedisClusterCRC16.getSlot(HashTag)
  private val RowCount = 100
  private val Ttl = 600

  private type Person = (String, String, Int) // id, name, age

  private def uuid(): String = UUID.randomUUID().toString.replace("-", "")

  private def people(run: String, label: String): Seq[Person] = (1 to RowCount).map(i =>
    (s"$HashTag:$run:$label:$i", s"$label-$i", i))

  private def df(rows: Seq[Person]): DataFrame = spark.createDataFrame(rows).toDF("id", "name", "age")

  /** Runs `body` with a new table name, then drops the table's views and deletes its schema key. */
  private def withTable(body: String => Unit): Unit = {
    val table = s"person${uuid()}"
    val schemaKey = RedisSourceRelation.schemaKey(table)
    withClue("setup: the schema key is in another slot:") {
      JedisClusterCRC16.getSlot(schemaKey) should not be Slot
    }
    try body(table)
    finally {
      spark.catalog.dropTempView(table)
      spark.catalog.dropTempView(s"${table}_input")
      val conn = redisConfig.connectionForKey(schemaKey)
      try conn.del(schemaKey)
      finally conn.close()
    }
  }

  private def createView(table: String, options: Map[String, String] = Map.empty): Unit = {
    val extra = options.map { case (k, v) => s", '$k' '$v'" }.mkString
    spark.sql(
      s"""CREATE TEMPORARY VIEW $table (id STRING, name STRING, age INT)
         |  USING $RedisFormat OPTIONS ($SqlOptionTableName '$table', '$SqlOptionKeyColumn' 'id'$extra)
         |""".stripMargin)
  }

  private val Replace = Map(SqlOptionWriteMode -> SqlOptionWriteModeReplace)
  private val HashWithTtl = Map(SqlOptionTTL -> Ttl.toString)
  private val BinaryWithTtl = Map(SqlOptionModel -> SqlOptionModelBinary, SqlOptionTTL -> Ttl.toString)

  private def write(table: String, rows: Seq[Person], options: Map[String, String]): Unit =
    df(rows).write.format(RedisFormat)
      .option(SqlOptionTableName, table)
      .option(SqlOptionKeyColumn, "id")
      .options(options)
      .mode(SaveMode.Append)
      .save()

  /** Gives each row's hash a field the new rows don't have, which `write.mode = replace` must remove. */
  private def storeOldRows(table: String, rows: Seq[Person]): Unit = rows.foreach { case (id, _, _) =>
    val key = s"$table:$id"
    val conn = redisConfig.connectionForKey(key)
    try conn.hset(key, Map("name" -> "old", "age" -> "0", "extra" -> "old").asJava)
    finally conn.close()
  }

  /** Checks that each row is stored in the binary model with a TTL, and names how many aren't. */
  private def checkBinaryRows(conn: Jedis, table: String, expected: Seq[Person], when: String): Unit = {
    def key(id: String): Array[Byte] = s"$table:$id".getBytes("UTF-8")
    val stored = expected.map { case (id, _, _) =>
      id -> Option(conn.get(key(id))).map(bytes => SerializationUtils.deserialize[Array[Any]](bytes).toSeq)
    }.toMap
    val wanted = expected.map { case (id, name, age) => id -> Some(Seq(id, name, age)) }.toMap
    val missing = wanted.count { case (id, values) => stored(id) != values }
    val withoutTtl = expected.count { case (id, _, _) => conn.ttl(key(id)) <= 0 }
    withClue(s"$missing of ${expected.size} rows not stored and $withoutTtl without a TTL $when:") {
      stored shouldBe wanted
      withoutTtl shouldBe 0
    }
  }

  /** Checks that each row's hash has a TTL, and names how many don't. */
  private def checkTtls(conn: Jedis, table: String, expected: Seq[Person], when: String): Unit = {
    val withoutTtl = expected.count { case (id, _, _) => conn.ttl(s"$table:$id") <= 0 }
    withClue(s"$withoutTtl of ${expected.size} rows without a TTL $when:") {
      withoutTtl shouldBe 0
    }
  }

  private def insert(table: String, rows: Seq[Person], overwrite: Boolean): Unit = {
    df(rows).createOrReplaceTempView(s"${table}_input")
    spark.sql(s"INSERT ${if (overwrite) "OVERWRITE" else "INTO"} TABLE $table SELECT id, name, age FROM ${table}_input")
  }

  /** Checks that `expected` rows are stored and `removed` rows are gone, and names how many of each are wrong. */
  private def checkRows(conn: Jedis, table: String, expected: Seq[Person], removed: Seq[Person], when: String): Unit = {
    def key(id: String): String = s"$table:$id"
    val stored = expected.map { case (id, _, _) => id -> conn.hgetAll(key(id)).asScala.toMap }.toMap
    val wanted = expected.map { case (id, name, age) => id -> Map("name" -> name, "age" -> age.toString) }.toMap
    val missing = wanted.count { case (id, fields) => stored(id) != fields }
    val left = removed.map(_._1).filter(id => conn.exists(key(id)))
    withClue(s"$missing of ${expected.size} rows not stored and ${left.size} of ${removed.size} old rows " +
      s"not removed $when:") {
      stored shouldBe wanted
      left shouldBe empty
    }
  }

  test("INSERT INTO a temporary view stores every row after the rows' slot moved to another primary") {
    withTable { table =>
      val rows = people(uuid(), "new")
      afterSlotMoved(HashTag, beforeMove = () => createView(table)) { _ =>
        insert(table, rows, overwrite = false)
      } { conn =>
        checkRows(conn, table, rows, Seq.empty, "after their slot moved")
      }
    }
  }

  test("INSERT OVERWRITE into a temporary view replaces every row after the rows' slot moved to another primary") {
    withTable { table =>
      val run = uuid()
      val oldRows = people(run, "old")
      val newRows = people(run, "new")
      val beforeMove = () => {
        createView(table)
        insert(table, oldRows, overwrite = false)
      }
      afterSlotMoved(HashTag, beforeMove) { _ =>
        insert(table, newRows, overwrite = true)
      } { conn =>
        checkRows(conn, table, newRows, oldRows, "after their slot moved")
      }
    }
  }

  test("a DataFrame write stores every row while the rows' slot is being migrated to another primary") {
    withTable { table =>
      val rows = people(uuid(), "new")
      whileSlotMigrating(HashTag) { _ =>
        df(rows).write.format(RedisFormat)
          .option(SqlOptionTableName, table)
          .option(SqlOptionKeyColumn, "id")
          .mode(SaveMode.Append)
          .save()
      } { conn =>
        checkRows(conn, table, rows, Seq.empty, "while their slot was migrating")
      }
    }
  }

  test("write.mode = replace replaces every row after the rows' slot moved to another primary") {
    withTable { table =>
      val rows = people(uuid(), "new")
      val beforeMove = () => {
        createView(table, Replace)
        storeOldRows(table, rows)
      }
      afterSlotMoved(HashTag, beforeMove) { _ =>
        insert(table, rows, overwrite = false)
      } { conn =>
        checkRows(conn, table, rows, Seq.empty, "after their slot moved")
      }
    }
  }

  // The rows are new: the old owner answers ASK only for keys it doesn't hold, so stored rows would be replaced there.
  test("write.mode = replace stores every row while the rows' slot is being migrated to another primary") {
    withTable { table =>
      val rows = people(uuid(), "new")
      whileSlotMigrating(HashTag) { _ =>
        write(table, rows, Replace)
      } { conn =>
        checkRows(conn, table, rows, Seq.empty, "while their slot was migrating")
      }
    }
  }

  test("the hash model with a ttl stores every row with its TTL after the rows' slot moved to another primary") {
    withTable { table =>
      val rows = people(uuid(), "new")
      afterSlotMoved(HashTag, beforeMove = () => createView(table, HashWithTtl)) { _ =>
        insert(table, rows, overwrite = false)
      } { conn =>
        checkRows(conn, table, rows, Seq.empty, "after their slot moved")
        checkTtls(conn, table, rows, "after their slot moved")
      }
    }
  }

  test("the hash model with a ttl stores every row with its TTL while the rows' slot is being migrated") {
    withTable { table =>
      val rows = people(uuid(), "new")
      whileSlotMigrating(HashTag) { _ =>
        write(table, rows, HashWithTtl)
      } { conn =>
        checkRows(conn, table, rows, Seq.empty, "while their slot was migrating")
        checkTtls(conn, table, rows, "while their slot was migrating")
      }
    }
  }

  test("the binary model with a ttl stores every row after the rows' slot moved to another primary") {
    withTable { table =>
      val rows = people(uuid(), "new")
      afterSlotMoved(HashTag, beforeMove = () => createView(table, BinaryWithTtl)) { _ =>
        insert(table, rows, overwrite = false)
      } { conn =>
        checkBinaryRows(conn, table, rows, "after their slot moved")
      }
    }
  }

  test("the binary model with a ttl stores every row while the rows' slot is being migrated to another primary") {
    withTable { table =>
      val rows = people(uuid(), "new")
      whileSlotMigrating(HashTag) { _ =>
        write(table, rows, BinaryWithTtl)
      } { conn =>
        checkBinaryRows(conn, table, rows, "while their slot was migrating")
      }
    }
  }
}
