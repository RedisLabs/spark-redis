package com.redislabs.provider.redis

import java.nio.charset.StandardCharsets.UTF_8

import com.redislabs.provider.redis.df.RedisDataframeSuite
import com.redislabs.provider.redis.util.MonitoredCommands
import com.redislabs.provider.redis.util.MonitoredCommands.Command
import com.redislabs.provider.redis.util.TestUtils.generateTableName
import org.apache.spark.sql.redis.SqlOptionTTL

import scala.collection.mutable

/**
  * A write with a `ttl` sets each key's value and its TTL in one step: one `MULTI`/`EXEC` on the key, so neither a
  * reader nor a failure between the two commands can leave the value without its TTL. A writer that streams many
  * elements into one key does this for each batch, so the key has its TTL from the first batch on, and the final TTL
  * counts from the last one, as before. Writes without a `ttl` send the same commands as before, with no `MULTI`.
  *
  * Each test records the commands the primaries receive with `MONITOR`.
  */
trait TtlWriteAtomicitySuite extends RedisDataframeSuite with MonitoredCommands {

  private val Ttl = 600

  /** Small batches, so that a single-key writer's elements take several. */
  private val SmallBatches = ReadWriteConfig(scanCount = 100, maxPipelineSize = 100, rddWriteIteratorGroupingSize = 4)

  /** The transactions in `commands`: each client's `MULTI` ... `EXEC` blocks, as the commands between the two. */
  private def transactions(commands: Seq[Command]): Seq[Seq[Command]] =
    commands.groupBy(_.client).values.toSeq.flatMap { stream =>
      val blocks = mutable.ListBuffer[Seq[Command]]()
      var current: Option[mutable.ListBuffer[Command]] = None
      stream.foreach { command =>
        command.name match {
          case "MULTI" => current = Some(mutable.ListBuffer())
          case "EXEC" =>
            current.foreach(block => blocks += block.toList)
            current = None
          case _ => current.foreach(_ += command)
        }
      }
      blocks.toList
    }

  /**
    * Checks that every command on `key` runs in a transaction, and that each of those transactions ends with the
    * key's `EXPIRE`. Returns the number of transactions.
    */
  private def checkAtomic(commands: Seq[Command], key: String): Int = {
    val onKey = commands.filter(_.key.contains(key))
    val onKeyTransactions = transactions(commands).filter(_.exists(_.key.contains(key)))
    withClue(s"commands on $key: ${onKey.map(_.normalized)}: ") {
      onKey should not be empty
      onKeyTransactions.flatten.size shouldBe onKey.size
      all(onKeyTransactions.map(_.last.normalized)) shouldBe Seq("EXPIRE", key, Ttl.toString)
    }
    onKeyTransactions.size
  }

  /** Checks that `commands` wrote keys starting with each of `prefixes`, and sent no `MULTI`. */
  private def noTransactions(commands: Seq[Command], prefixes: Seq[String]): Unit = {
    prefixes.foreach { prefix =>
      withClue(s"commands on $prefix*: ") {
        commands.filter(_.key.exists(_.startsWith(prefix))) should not be empty
      }
    }
    commands.filter(_.name == "MULTI") shouldBe empty
  }

  private def keys(prefix: String, n: Int): Seq[String] = (1 to n).map(i => s"$prefix:$i")

  test("toRedisHASHes with a ttl sets each hash and its TTL in one MULTI/EXEC") {
    val ks = keys(generateTableName("ttl-hashes"), 10)
    val commands = monitored {
      sc.toRedisHASHes(sc.parallelize(ks.map(k => k -> Map("f" -> "v")), 3), Ttl)
    }
    ks.foreach(k => checkAtomic(commands, k) shouldBe 1)
  }

  test("toRedisByteHASHes with a ttl sets each hash and its TTL in one MULTI/EXEC") {
    val ks = keys(generateTableName("ttl-byte-hashes"), 10)
    val commands = monitored {
      val rows = ks.map(k => k.getBytes(UTF_8) -> Map("f".getBytes(UTF_8) -> "v".getBytes(UTF_8)))
      sc.toRedisByteHASHes(sc.parallelize(rows, 3), Ttl)
    }
    ks.foreach(k => checkAtomic(commands, k) shouldBe 1)
  }

  test("toRedisLISTs and toRedisByteLISTs with a ttl set each list and its TTL in one MULTI/EXEC") {
    val ks = keys(generateTableName("ttl-lists"), 10)
    val byteKs = keys(generateTableName("ttl-byte-lists"), 10)
    val commands = monitored {
      sc.toRedisLISTs(sc.parallelize(ks.map(k => k -> Seq("a", "b")), 3), Ttl)
      sc.toRedisByteLISTs(sc.parallelize(byteKs.map(k => k.getBytes(UTF_8) -> Seq("a".getBytes(UTF_8))), 3), Ttl)
    }
    (ks ++ byteKs).foreach(k => checkAtomic(commands, k) shouldBe 1)
  }

  test("a single-key writer with a ttl sets each batch's elements and the key's TTL in one MULTI/EXEC") {
    val prefix = generateTableName("ttl-single-key")
    val (hash, zset, set, list) = (s"$prefix:hash", s"$prefix:zset", s"$prefix:set", s"$prefix:list")
    val elements = (1 to 10).map(i => s"e$i")
    val commands = monitored {
      sc.toRedisHASH(sc.parallelize(elements.map(e => e -> "v"), 1), hash, Ttl)(redisConfig, SmallBatches)
      sc.toRedisZSET(sc.parallelize(elements.map(e => e -> "1"), 1), zset, Ttl)(redisConfig, SmallBatches)
      sc.toRedisSET(sc.parallelize(elements, 1), set, Ttl)(redisConfig, SmallBatches)
      sc.toRedisLIST(sc.parallelize(elements, 1), list, Ttl)(redisConfig, SmallBatches)
    }
    // 10 elements in batches of 4: three transactions per key
    Seq(hash, zset, set, list).foreach(k => checkAtomic(commands, k) shouldBe 3)
  }

  test("a hash-model DataFrame write with a ttl sets each row and its TTL in one MULTI/EXEC") {
    val table = generateTableName("ttl-df-hash")
    val commands = monitored {
      writeDf(table, Map(SqlOptionTTL -> Ttl))
    }
    val rowKeys = commands.filter(_.name == "HSET").flatMap(_.key).distinct
    rowKeys should not be empty
    rowKeys.foreach(k => checkAtomic(commands, k) shouldBe 1)
  }

  test("without a ttl, the writes send no MULTI, as before") {
    val prefix = generateTableName("no-ttl")
    val elements = (1 to 10).map(i => s"e$i")
    val commands = monitored {
      sc.toRedisHASHes(sc.parallelize(keys(s"$prefix-hashes", 5).map(k => k -> Map("f" -> "v")), 2))
      sc.toRedisLISTs(sc.parallelize(keys(s"$prefix-lists", 5).map(k => k -> Seq("a")), 2))
      sc.toRedisHASH(sc.parallelize(elements.map(e => e -> "v"), 1), s"$prefix-hash")(redisConfig, SmallBatches)
      sc.toRedisSET(sc.parallelize(elements, 1), s"$prefix-set")(redisConfig, SmallBatches)
      writeDf(s"$prefix-df")
    }
    noTransactions(commands, Seq("hashes", "lists", "hash", "set", "df").map(s => s"$prefix-$s"))
  }
}
