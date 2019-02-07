package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.{RedisConfig, SparkRedisSuite, toRedisContext}
import org.scalatest.Matchers

import scala.io.Source.fromInputStream

/**
  * @author The Viet Nguyen
  */
trait RedisRddSuite extends SparkRedisSuite with Matchers {

  implicit val redisConfig: RedisConfig

  val content: String = fromInputStream(getClass.getClassLoader.getResourceAsStream("blog"))
    .getLines.toArray.mkString("\n")

  override def beforeAll() {
    super.beforeAll()
    val wcnts = sc.parallelize(content.split("\\W+")
      .filter(_.nonEmpty))
      .map { w =>
        (w, 1)
      }
      .reduceByKey {
        _ + _
      }
      .map { x =>
        (x._1, x._2.toString)
      }
    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))
    // Flush all the hosts
    redisConfig.hosts.foreach(node => {
      val conn = node.connect()
      conn.flushAll
      conn.close()
    })
    sc.toRedisKV(wcnts)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset")
    sc.toRedisHASH(wcnts, "all:words:cnt:hash")
    sc.toRedisLIST(wds, "all:words:list")
    sc.toRedisSET(wds, "all:words:set")
  }

  test("RedisKVRDD - default(cluster)") {
    val redisKVRDD = sc.fromRedisKV("*")
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toString)).toArray.sortBy(_._1)
    kvContents shouldBe wcnts
  }

  test("RedisKVRDD - cluster") {
    val redisKVRDD = sc.fromRedisKV("*")
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toString)).toArray.sortBy(_._1)
    kvContents should be(wcnts)
  }

  test("RedisZsetRDD - default(cluster)") {
    val redisZSetWithScore = sc.fromRedisZSetWithScore("all:words:cnt:sortedset")
    val zsetWithScore = redisZSetWithScore.sortByKey().collect

    val redisZSet = sc.fromRedisZSet("all:words:cnt:sortedset")
    val zset = redisZSet.collect.sorted

    val redisZRangeWithScore = sc.fromRedisZRangeWithScore("all:words:cnt:sortedset", 0, 15)
    val zrangeWithScore = redisZRangeWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRange = sc.fromRedisZRange("all:words:cnt:sortedset", 0, 15)
    val zrange = redisZRange.collect.sorted

    val redisZRangeByScoreWithScore =
      sc.fromRedisZRangeByScoreWithScore("all:words:cnt:sortedset", 3, 9)
    val zrangeByScoreWithScore = redisZRangeByScoreWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRangeByScore = sc.fromRedisZRangeByScore("all:words:cnt:sortedset", 3, 9)
    val zrangeByScore = redisZRangeByScore.collect.sorted

    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toDouble))

    zsetWithScore should be(wcnts.toArray.sortBy(_._1))
    zset should be(wcnts.keys.toArray.sorted)
    zrangeWithScore should be(wcnts.toArray.sortBy(x => (x._2, x._1)).take(16))
    zrange should be(wcnts.toArray.sortBy(x => (x._2, x._1)).take(16).map(_._1))
    zrangeByScoreWithScore should be(wcnts.toArray.filter(x => x._2 >= 3 && x._2 <= 9)
      .sortBy(x => (x._2, x._1)))
    zrangeByScore should be(wcnts.toArray.filter(x => x._2 >= 3 && x._2 <= 9).map(_._1).sorted)
  }

  test("RedisZsetRDD - cluster") {
    val redisZSetWithScore = sc.fromRedisZSetWithScore("all:words:cnt:sortedset")
    val zsetWithScore = redisZSetWithScore.sortByKey().collect

    val redisZSet = sc.fromRedisZSet("all:words:cnt:sortedset")
    val zset = redisZSet.collect.sorted

    val redisZRangeWithScore = sc.fromRedisZRangeWithScore("all:words:cnt:sortedset", 0, 15)
    val zrangeWithScore = redisZRangeWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRange = sc.fromRedisZRange("all:words:cnt:sortedset", 0, 15)
    val zrange = redisZRange.collect.sorted

    val redisZRangeByScoreWithScore =
      sc.fromRedisZRangeByScoreWithScore("all:words:cnt:sortedset", 3, 9)
    val zrangeByScoreWithScore = redisZRangeByScoreWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRangeByScore = sc.fromRedisZRangeByScore("all:words:cnt:sortedset", 3, 9)
    val zrangeByScore = redisZRangeByScore.collect.sorted

    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toDouble))

    zsetWithScore should be(wcnts.toArray.sortBy(_._1))
    zset should be(wcnts.map(_._1).toArray.sorted)
    zrangeWithScore should be(wcnts.toArray.sortBy(x => (x._2, x._1)).take(16))
    zrange should be(wcnts.toArray.sortBy(x => (x._2, x._1)).take(16).map(_._1))
    zrangeByScoreWithScore should be(wcnts.toArray.filter(x => (x._2 >= 3 && x._2 <= 9)).sortBy(x => (x._2, x._1)))
    zrangeByScore should be(wcnts.toArray.filter(x => (x._2 >= 3 && x._2 <= 9)).map(_._1).sorted)
  }

  test("RedisHashRDD - default(cluster)") {
    val redisHashRDD = sc.fromRedisHash("all:words:cnt:hash")
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be(wcnts)
  }

  test("RedisHashRDD - cluster") {
    val redisHashRDD = sc.fromRedisHash("all:words:cnt:hash")
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be(wcnts)
  }

  test("RedisListRDD - default(cluster)") {
    val redisListRDD = sc.fromRedisList("all:words:list")
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be(ws)
  }

  test("RedisListRDD - cluster") {
    val redisListRDD = sc.fromRedisList("all:words:list")
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be(ws)
  }

  test("RedisSetRDD - default(cluster)") {
    val redisSetRDD = sc.fromRedisSet("all:words:set")
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be(ws)
  }

  test("RedisSetRDD - cluster") {
    val redisSetRDD = sc.fromRedisSet("all:words:set")
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be(ws)
  }

  test("Expire - default(cluster)") {
    val expireTime = 1
    val prefix = s"#expire in $expireTime#:"
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (prefix + x._1, x._2.toString))
    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))
    sc.toRedisKV(wcnts, expireTime)
    sc.toRedisZSET(wcnts, prefix + "all:words:cnt:sortedset", expireTime)
    sc.toRedisHASH(wcnts, prefix + "all:words:cnt:hash", expireTime)
    sc.toRedisLIST(wds, prefix + "all:words:list", expireTime)
    sc.toRedisSET(wds, prefix + "all:words:set", expireTime)
    Thread.sleep(expireTime * 1000 + 1)
    sc.fromRedisKeyPattern(prefix + "*").count should be(0)
  }

  test("Expire - cluster") {
    val expireTime = 1
    val prefix = s"#expire in $expireTime#:"
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (prefix + x._1, x._2.toString))
    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))
    sc.toRedisKV(wcnts, expireTime)
    sc.toRedisZSET(wcnts, prefix + "all:words:cnt:sortedset", expireTime)
    sc.toRedisHASH(wcnts, prefix + "all:words:cnt:hash", expireTime)
    sc.toRedisLIST(wds, prefix + "all:words:list", expireTime)
    sc.toRedisSET(wds, prefix + "all:words:set", expireTime)
    Thread.sleep(expireTime * 1000 + 1)
    sc.fromRedisKeyPattern(prefix + "*").count should be(0)
  }
}
