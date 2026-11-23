package com.redislabs.provider.redis.df

import java.nio.charset.StandardCharsets.UTF_8

import com.redislabs.provider.redis.RedisEndpoint
import com.redislabs.provider.redis.toRedisContext
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.TestUtils.generateTableName
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.redis._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.Matchers
import redis.clients.jedis.Jedis
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.util.{JedisClusterCRC16, SafeEncoder}

/**
  * The `binary` model writing with a `ttl`. Where the server has `MSETEX` (Redis 8.4+) each batch of rows goes in one
  * `MSETEX`, one per hash slot on a cluster. Where it doesn't, each row goes in one `SET … EX`. `SETEX` is never
  * sent, and the stored values and TTLs are the same either way.
  *
  * The suites run on every Redis version CI tests, so CI covers both paths: `redis:7.4` has no `MSETEX`, `redis:8.6`
  * has it. Each test finds out which path applies by running `MSETEX` itself (see `serverHasMsetex`), not from the
  * image tag, and asserts that path.
  *
  * The commands a write sent are counted with `INFO commandstats` on every primary. Only the test reads them, to watch
  * the server; the connector never parses `INFO` to choose a path.
  */
trait BinaryTtlWriteSuite extends RedisDataframeSuite with Matchers {

  /** True for the cluster suite: keys of one multi-key command must then share a hash slot. */
  protected def clusterMode: Boolean

  protected val Ttl = 3600
  protected val RowCount = 50
  protected val BatchSize = 10
  protected val KeyColumn = "id"
  protected val TablePrefix = "binary_ttl_"

  protected val schema: StructType =
    StructType(Seq(StructField(KeyColumn, StringType), StructField("n", IntegerType)))

  /**
    * Fixed rows. The ids carry five hash tags, so on a cluster each batch of ten rows spans five slots with two keys
    * each: one `MSETEX` then holds more than one key, and one batch still needs several.
    */
  protected val rows: Seq[Row] = (0 until RowCount).map(i => Row(s"{tag${i % 5}}$i", i))

  protected def dataKey(table: String, row: Row): String = s"$table:${row.getString(0)}"

  protected def dataFrame(partitions: Int): DataFrame =
    spark.createDataFrame(sc.parallelize(rows, partitions), schema)

  protected def write(table: String, partitions: Int, batchSize: Int = BatchSize): Unit = {
    dataFrame(partitions).write.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBinary)
      .option(SqlOptionTableName, table)
      .option(SqlOptionKeyColumn, KeyColumn)
      .option(SqlOptionTTL, Ttl.toString)
      .option(SqlOptionIteratorGroupingSize, batchSize.toString)
      .save()
  }

  /** Every primary once, also when a primary owns more than one slot range. */
  protected def primaries: Seq[RedisEndpoint] = redisConfig.hosts.map(_.endpoint).distinct.toSeq

  protected def onPrimaries[A](f: Jedis => A): Seq[A] =
    primaries.map(endpoint => withConnection(endpoint.connect())(f))

  protected def resetCommandStats(): Unit = onPrimaries(_.configResetStat())

  /** Calls of `command` on one server since the last reset. A subcommand is written `command|sub`. */
  protected def callsOn(conn: Jedis, command: String): Long = {
    val prefix = s"cmdstat_$command:"
    conn.info("commandstats").split("\r?\n").find(_.startsWith(prefix)) match {
      case Some(line) =>
        line.stripPrefix(prefix).split(",").map(_.split("=")).collectFirst { case Array("calls", n) => n.toLong }
          .getOrElse(fail(s"no calls in '$line'"))
      case None => 0L
    }
  }

  /** Calls of `command` since the last reset, summed over the primaries. */
  protected def calls(command: String): Long = onPrimaries(callsOn(_, command)).sum

  protected object MsetexCommand extends ProtocolCommand {
    override def getRaw: Array[Byte] = SafeEncoder.encode("MSETEX")
  }

  /**
    * Whether every primary runs `MSETEX`. Found by sending it with no arguments: a server that knows the command
    * rejects the arity, one that doesn't rejects the name. Nothing is written, and this is independent of the
    * connector's own probe.
    */
  protected lazy val serverHasMsetex: Boolean = onPrimaries { conn =>
    try {
      conn.sendCommand(MsetexCommand, Seq.empty[Array[Byte]]: _*)
      fail("MSETEX with no arguments was accepted")
    } catch {
      case e: JedisDataException if e.getMessage.contains("wrong number of arguments") => true
      case e: JedisDataException if e.getMessage.toLowerCase.contains("unknown command") => false
    }
  }.forall(has => has)

  /** `MSETEX` commands one write of `rows` from one partition sends: one per batch, or per slot in a batch. */
  protected def expectedMsetexCalls(table: String, batchSize: Int): Long =
    rows.grouped(batchSize).map { batch =>
      if (clusterMode) batch.map(row => JedisClusterCRC16.getSlot(dataKey(table, row))).distinct.size else 1
    }.sum.toLong

  /** Each row's key holds its Java-serialized values and expires in `Ttl` seconds, and there are no other keys. */
  protected def verifyStored(table: String): Unit = {
    rows.foreach { row =>
      val key = dataKey(table, row)
      withConnection(redisConfig.connectionForKey(key)) { conn =>
        val stored = conn.get(key.getBytes(UTF_8))
        withClue(s"key $key: ") {
          stored should not be null
          SerializationUtils.deserialize[Array[Any]](stored).toSeq shouldBe Seq(row.getString(0), row.getInt(1))
          conn.ttl(key).longValue should (be > (Ttl - 5).toLong and be <= Ttl.toLong)
        }
      }
    }
    sc.fromRedisKeyPattern(s"$table:*").count() shouldBe RowCount
  }

  protected def path: String = if (serverHasMsetex) "MSETEX" else "SET ... EX"

  test("binary write with ttl stores every row with its TTL") {
    val table = generateTableName(TablePrefix)
    write(table, partitions = 3)
    info(s"path: $path")
    verifyStored(table)
    val loaded = spark.read.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBinary)
      .option(SqlOptionTableName, table)
      .option(SqlOptionKeyColumn, KeyColumn)
      .load()
      .collect()
      .map(r => (r.getAs[String](KeyColumn), r.getAs[Int]("n")))
    loaded.toSeq.sorted shouldBe rows.map(r => (r.getString(0), r.getInt(1))).sorted
  }

  test("binary write with ttl sends one MSETEX per batch with MSETEX, one SET EX per row without, never SETEX") {
    val table = generateTableName(TablePrefix)
    resetCommandStats()
    write(table, partitions = 1)
    info(s"path: $path")
    calls("setex") shouldBe 0
    if (serverHasMsetex) {
      calls("msetex") shouldBe expectedMsetexCalls(table, BatchSize)
      calls("set") shouldBe 1 // the schema key
    } else {
      calls("msetex") shouldBe 0
      calls("set") shouldBe RowCount + 1 // each row, and the schema key
    }
    verifyStored(table)
  }

  test("binary write with ttl asks each primary once per write whether it has MSETEX") {
    val table = generateTableName(TablePrefix)
    resetCommandStats()
    write(table, partitions = 8, batchSize = 3)
    onPrimaries(callsOn(_, "command|info")) shouldBe Seq.fill(primaries.size)(1L)
    verifyStored(table)
  }
}
