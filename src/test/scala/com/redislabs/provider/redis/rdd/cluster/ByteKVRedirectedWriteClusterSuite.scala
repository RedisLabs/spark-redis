package com.redislabs.provider.redis.rdd.cluster

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.RedirectedWrites
import org.scalatest.Matchers
import redis.clients.jedis.Jedis

/**
  * `toRedisByteKV`, which came from upstream, writes to a slot that moved to another primary after the `RedisConfig`
  * read the topology (`MOVED`), or that is being migrated (`ASK`). Every key must still be stored, with its TTL when
  * one is given, as [[MovedWriteClusterSuite]] and [[AskWriteClusterSuite]] require of `toRedisKV` (#347).
  */
class ByteKVRedirectedWriteClusterSuite extends SparkRedisSuite with RedisClusterEnv with RedirectedWrites
    with Matchers {

  private val HashTag = "{redirected-bytekv}"
  private val KeyCount = 100
  private val Ttl = 600

  private def rwConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)

  /** [[KeyCount]] binary keys and values in `HashTag`'s slot. */
  private def kvs(): Seq[(Array[Byte], Array[Byte])] = {
    val run = UUID.randomUUID().toString.replace("-", "")
    (1 to KeyCount).map(i => (s"$HashTag:$run:key:$i".getBytes(UTF_8), s"value-$i".getBytes(UTF_8)))
  }

  /** Checks that every key holds its value, and has a TTL when `ttl` is set, and names how many don't. */
  private def checkStored(conn: Jedis, expected: Seq[(Array[Byte], Array[Byte])], ttl: Boolean, when: String): Unit = {
    val stored = expected.map { case (key, _) => new String(key, UTF_8) -> Option(conn.get(key)).map(new String(_, UTF_8)) }
    val wanted = expected.map { case (key, value) => new String(key, UTF_8) -> Some(new String(value, UTF_8)) }
    val missing = stored.zip(wanted).count { case (s, w) => s != w }
    withClue(s"$missing of $KeyCount keys not stored $when:") {
      stored shouldBe wanted
    }
    if (ttl) {
      val withoutTtl = expected.count { case (key, _) => conn.ttl(key) <= 0 }
      withClue(s"$withoutTtl of $KeyCount keys have no TTL $when:") {
        withoutTtl shouldBe 0
      }
    }
  }

  test("toRedisByteKV stores every key after its slot moved to another primary") {
    val rows = kvs()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisByteKV(sc.parallelize(rows, 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, rows, ttl = false, "after their slot moved")
    }
  }

  test("toRedisByteKV stores every key with its TTL while its slot is being migrated to another primary") {
    val rows = kvs()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisByteKV(sc.parallelize(rows, 4), Ttl)(config, rwConfig)
    } { conn =>
      checkStored(conn, rows, ttl = true, "while their slot was migrating")
    }
  }
}
