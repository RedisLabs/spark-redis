package com.redislabs.provider.redis.rdd

import java.util.UUID

import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.{RedisEndpoint, SparkRedisSuite, toRedisContext}
import org.scalatest.Matchers

/**
  * The set and sorted-set readers use the commands that replace the legacy ones: `fromRedisSet` reads each set with
  * `SSCAN`, in pages, instead of all at once with `SMEMBERS`, and `fromRedisZRangeByScore` reads with
  * `ZRANGE … BYSCORE` instead of `ZRANGEBYSCORE`, deprecated since Redis 6.2. They return the same elements as
  * before.
  *
  * Each test counts the calls the primaries report in `INFO commandstats` while the read runs.
  */
trait RedisRddReadCommandsSuite extends SparkRedisSuite with Matchers {

  private val SetCount = 5
  /** Ten times `scanCount`'s default, so that each set takes several `SSCAN` pages. */
  private val MembersPerSet = 1000
  private val ZSetCount = 5
  private val MembersPerZSet = 100

  private def run(): String = UUID.randomUUID().toString.replace("-", "")

  /** Each primary once. `redisConfig.hosts` lists a cluster primary once per slot range it owns. */
  private def primaries: Seq[RedisEndpoint] =
    redisConfig.hosts.toSeq.map(_.endpoint).groupBy(e => (e.host, e.port)).values.map(_.head).toSeq

  private val CommandStat = """cmdstat_([^:]+):calls=(\d+),.*""".r

  /** The calls of each command, summed over the primaries. */
  private def commandCalls(): Map[String, Long] =
    primaries.flatMap { endpoint =>
      withConnection(endpoint.connect()) { conn =>
        conn.info("commandstats").split("\r\n").toSeq.collect { case CommandStat(name, calls) => name -> calls.toLong }
      }
    }.groupBy(_._1).map { case (name, counts) => name -> counts.map(_._2).sum }

  /** Runs `body`, and returns its result with the calls of each command the primaries received while it ran. */
  private def counted[T](body: => T): (T, Map[String, Long]) = {
    val before = commandCalls()
    val result = body
    val after = commandCalls()
    (result, after.map { case (name, calls) => name -> (calls - before.getOrElse(name, 0L)) })
  }

  private def calls(counts: Map[String, Long], command: String): Long = counts.getOrElse(command, 0L)

  test("fromRedisSet reads every member with SSCAN, and sends no SMEMBERS") {
    val prefix = s"read-commands-set:${run()}"
    val sets = (1 to SetCount).map(s => s"$prefix:$s" -> (1 to MembersPerSet).map(m => s"member-$s-$m"))
    sets.foreach { case (key, members) =>
      withConnection(redisConfig.connectionForKey(key))(_.sadd(key, members: _*))
    }

    val (read, counts) = counted(sc.fromRedisSet(s"$prefix:*").collect().toSeq)

    read should contain theSameElementsAs sets.flatMap(_._2)
    withClue(s"commands sent: ${counts.filter(_._2 > 0)}: ") {
      calls(counts, "smembers") shouldBe 0
      // more than one page per set (COUNT is only a hint, so not an exact number)
      calls(counts, "sscan") should be > SetCount.toLong
    }
  }

  test("fromRedisZRangeByScore reads with ZRANGE BYSCORE, and sends no ZRANGEBYSCORE") {
    val prefix = s"read-commands-zset:${run()}"
    val zsets = (1 to ZSetCount).map { z =>
      s"$prefix:$z" -> (1 to MembersPerZSet).map(m => (s"member-$z-$m", m.toDouble))
    }
    zsets.foreach { case (key, members) =>
      withConnection(redisConfig.connectionForKey(key)) { conn =>
        members.foreach { case (member, score) => conn.zadd(key, score, member) }
      }
    }
    def inRange(min: Double, max: Double): Seq[(String, Double)] =
      zsets.flatMap(_._2).filter { case (_, score) => score >= min && score <= max }

    val ((some, all, someWithScores, allWithScores), counts) = counted {
      (sc.fromRedisZRangeByScore(s"$prefix:*", 10, 20).collect().toSeq,
        sc.fromRedisZRangeByScore(s"$prefix:*", Double.NegativeInfinity, Double.PositiveInfinity).collect().toSeq,
        sc.fromRedisZRangeByScoreWithScore(s"$prefix:*", 10, 20).collect().toSeq,
        sc.fromRedisZRangeByScoreWithScore(s"$prefix:*", Double.NegativeInfinity, Double.PositiveInfinity)
          .collect().toSeq)
    }

    some should contain theSameElementsAs inRange(10, 20).map(_._1)
    all should contain theSameElementsAs zsets.flatMap(_._2).map(_._1)
    someWithScores should contain theSameElementsAs inRange(10, 20)
    allWithScores should contain theSameElementsAs zsets.flatMap(_._2)
    withClue(s"commands sent: ${counts.filter(_._2 > 0)}: ") {
      calls(counts, "zrangebyscore") shouldBe 0
      // one per sorted set in each of the four reads
      calls(counts, "zrange") should be >= (4 * ZSetCount).toLong
    }
  }

  test("each read returns members in score order within each sorted set, as before") {
    val key = s"read-commands-order:${run()}"
    val members = (1 to MembersPerZSet).map(m => (s"member-$m", (MembersPerZSet - m).toDouble))
    withConnection(redisConfig.connectionForKey(key)) { conn =>
      members.foreach { case (member, score) => conn.zadd(key, score, member) }
    }
    sc.fromRedisZRangeByScoreWithScore(Array(key), 0, MembersPerZSet, 1).collect().toSeq shouldBe
      members.sortBy(_._2)
  }
}
