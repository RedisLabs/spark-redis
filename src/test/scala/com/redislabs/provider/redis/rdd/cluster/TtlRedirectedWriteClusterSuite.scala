package com.redislabs.provider.redis.rdd.cluster

import java.util.UUID

import scala.collection.JavaConverters._

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.RedirectedWrites
import org.scalatest.Matchers
import redis.clients.jedis.Jedis

/**
  * With a `ttl`, the RDD writers set a key's elements and its `EXPIRE` in one `MULTI`/`EXEC`: one per batch for the
  * writers that fill one key (`toRedisHASH`, `toRedisZSET`, `toRedisSET`, `toRedisLIST`), one per key for the writers
  * of many keys (`toRedisHASHes`, `toRedisByteHASHes`, `toRedisLISTs`, `toRedisByteLISTs`). Those transactions must
  * follow `MOVED` and `ASK` like the writes without a `ttl` (#347): every element is stored, and every key has its TTL.
  *
  * One writer of each kind stands for the others, since they share the code that sends the transactions.
  */
class TtlRedirectedWriteClusterSuite extends SparkRedisSuite with RedisClusterEnv with RedirectedWrites
    with Matchers {

  private val HashTag = "{redirected-ttl}"
  private val Ttl = 600
  private val FieldCount = 100
  private val HashCount = 20
  private val FieldsPerHash = 5

  private def run(): String = UUID.randomUUID().toString.replace("-", "")

  /** Batches of 10, so that `toRedisHASH`'s partitions each send several transactions. */
  private def rwConfig: ReadWriteConfig =
    ReadWriteConfig.fromSparkConf(sc.getConf).copy(rddWriteIteratorGroupingSize = 10)

  private def oneHash(): (String, Seq[(String, String)]) =
    (s"$HashTag:${run()}:hash", (1 to FieldCount).map(i => (s"field-$i", s"value-$i")))

  private def manyHashes(): Seq[(String, Map[String, String])] = {
    val id = run()
    (1 to HashCount).map { h =>
      (s"$HashTag:$id:hash:$h", (1 to FieldsPerHash).map(f => (s"field-$f", s"value-$h-$f")).toMap)
    }
  }

  /** Checks that every hash holds exactly its fields and has a TTL, and names how many hashes don't. */
  private def checkStored(conn: Jedis, expected: Seq[(String, Map[String, String])], when: String): Unit = {
    val stored = expected.map { case (key, _) => key -> conn.hgetAll(key).asScala.toMap }.toMap
    val wrong = expected.count { case (key, fields) => stored(key) != fields }
    val withoutTtl = expected.count { case (key, _) => conn.ttl(key) <= 0 }
    withClue(s"$wrong of ${expected.size} hashes not stored and $withoutTtl without a TTL $when:") {
      stored shouldBe expected.toMap
      withoutTtl shouldBe 0
    }
  }

  private val Moved = "after their slot moved"
  private val Migrating = "while their slot was migrating"

  test("toRedisHASH with a ttl stores every field and the TTL after its slot moved to another primary") {
    val (key, fields) = oneHash()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisHASH(sc.parallelize(fields, 4), key, Ttl)(config, rwConfig)
    } { conn =>
      checkStored(conn, Seq(key -> fields.toMap), Moved)
    }
  }

  test("toRedisHASH with a ttl stores every field and the TTL while its slot is being migrated") {
    val (key, fields) = oneHash()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisHASH(sc.parallelize(fields, 4), key, Ttl)(config, rwConfig)
    } { conn =>
      checkStored(conn, Seq(key -> fields.toMap), Migrating)
    }
  }

  test("toRedisHASHes with a ttl stores every hash and its TTL after their slot moved to another primary") {
    val hashes = manyHashes()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisHASHes(sc.parallelize(hashes, 4), Ttl)(config, rwConfig)
    } { conn =>
      checkStored(conn, hashes, Moved)
    }
  }

  test("toRedisHASHes with a ttl stores every hash and its TTL while their slot is being migrated") {
    val hashes = manyHashes()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisHASHes(sc.parallelize(hashes, 4), Ttl)(config, rwConfig)
    } { conn =>
      checkStored(conn, hashes, Migrating)
    }
  }
}
