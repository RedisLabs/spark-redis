package com.redislabs.provider.redis.rdd.cluster

import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.RedirectedWrites
import org.scalatest.Matchers
import redis.clients.jedis.{Jedis, ScanParams}

/**
  * The set and sorted-set writers (`toRedisSET`, `toRedisZSET`) write to a slot that moved to another primary after
  * the `RedisConfig` read the topology (`MOVED`), or that is being migrated (`ASK`). Every member must still be
  * stored, as [[MovedWriteClusterSuite]] and [[AskWriteClusterSuite]] require of `toRedisKV` (#347).
  */
class SetRedirectedWriteClusterSuite extends SparkRedisSuite with RedisClusterEnv with RedirectedWrites
    with Matchers {

  private val HashTag = "{redirected-set}"
  private val MemberCount = 100

  private def key(kind: String): String = s"$HashTag:${UUID.randomUUID().toString.replace("-", "")}:$kind"

  private def rwConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)

  private val members = (1 to MemberCount).map(i => s"member-$i")

  /** Members and their scores, as `toRedisZSET` takes them: (member, score). */
  private val scored = (1 to MemberCount).map(i => (s"member-$i", s"$i"))

  /** The set's members, read with SSCAN. */
  private def setMembers(conn: Jedis, key: String): Set[String] = {
    val found = mutable.Set[String]()
    var cursor = ScanParams.SCAN_POINTER_START
    do {
      val page = conn.sscan(key, cursor)
      found ++= page.getResult.asScala
      cursor = page.getCursor
    } while (cursor != ScanParams.SCAN_POINTER_START)
    found.toSet
  }

  private def checkSet(conn: Jedis, key: String, when: String): Unit = {
    val stored = setMembers(conn, key)
    val missing = members.count(m => !stored.contains(m))
    withClue(s"$missing of $MemberCount set members not stored $when:") {
      stored shouldBe members.toSet
    }
  }

  private def checkZset(conn: Jedis, key: String, when: String): Unit = {
    val stored = conn.zrangeWithScores(key, 0, -1).asScala.map(t => (t.getElement, t.getScore)).toMap
    val expected = scored.map { case (member, score) => (member, score.toDouble) }.toMap
    val missing = expected.count { case (member, score) => !stored.get(member).contains(score) }
    withClue(s"$missing of $MemberCount sorted-set members not stored $when:") {
      stored shouldBe expected
    }
  }

  private val Moved = "after its slot moved"
  private val Migrating = "while its slot was migrating"

  test("toRedisSET stores every member after its slot moved to another primary") {
    val setKey = key("set")
    afterSlotMoved(HashTag) { config =>
      sc.toRedisSET(sc.parallelize(members, 4), setKey)(config, rwConfig)
    } { conn =>
      checkSet(conn, setKey, Moved)
    }
  }

  test("toRedisSET stores every member while its slot is being migrated to another primary") {
    val setKey = key("set")
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisSET(sc.parallelize(members, 4), setKey)(config, rwConfig)
    } { conn =>
      checkSet(conn, setKey, Migrating)
    }
  }

  test("toRedisZSET stores every member after its slot moved to another primary") {
    val zsetKey = key("zset")
    afterSlotMoved(HashTag) { config =>
      sc.toRedisZSET(sc.parallelize(scored, 4), zsetKey)(config, rwConfig)
    } { conn =>
      checkZset(conn, zsetKey, Moved)
    }
  }

  test("toRedisZSET stores every member while its slot is being migrated to another primary") {
    val zsetKey = key("zset")
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisZSET(sc.parallelize(scored, 4), zsetKey)(config, rwConfig)
    } { conn =>
      checkZset(conn, zsetKey, Migrating)
    }
  }
}
