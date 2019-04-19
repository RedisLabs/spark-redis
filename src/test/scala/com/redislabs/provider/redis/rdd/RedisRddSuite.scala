package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.{RedisConfig, SparkRedisSuite, toRedisContext}
import org.scalatest.Matchers

import scala.io.Source.fromInputStream

/**
  * @author The Viet Nguyen
  */
trait RedisRddSuite extends SparkRedisSuite with Keys with Matchers {

  implicit val redisConfig: RedisConfig

  val content: String = fromInputStream(getClass.getClassLoader.getResourceAsStream("blog"))
    .getLines.toArray.mkString("\n")

  val contentWords: Array[String] = content.split("\\W+").filter(_.nonEmpty)
  val zSetKey: String = "all:words:cnt:sortedset"
  val hashKey: String = "all:words:cnt:hash"
  val listKey: String = "all:words:list"
  val setKey: String = "all:words:set"

  val wcntsPrefix: String = "wcnts"
  val hllPrefix: String = "hll"

  override def beforeAll() {
    super.beforeAll()
    val wcnts = sc.parallelize(contentWords)
      .map { w =>
        (w, 1)
      }
      .reduceByKey {
        _ + _
      }
      .map { x =>
        (s"$wcntsPrefix-${x._1}", x._2.toString)
      }
    val wds = sc.parallelize(contentWords)
    // Flush all the hosts
    redisConfig.hosts.foreach(node => {
      val conn = node.connect()
      conn.flushAll
      conn.close()
    })
    val hllRDD = sc.parallelize(
      Seq(
        (s"$hllPrefix-apple","gala"),
        (s"$hllPrefix-apple","red-delicious"),
        (s"$hllPrefix-pear","barlett"),
        (s"$hllPrefix-peach","freestone")
      )
    )
    sc.toRedisKV(wcnts)
    sc.toRedisZSET(wcnts, zSetKey)
    sc.toRedisHASH(wcnts, hashKey)
    sc.toRedisLIST(wds, listKey)
    sc.toRedisSET(wds, setKey)
    sc.toRedisHLL(hllRDD)
  }

  test("RedisKVRDD") {
    val redisKVRDD = sc.fromRedisKV(s"$wcntsPrefix-*")
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = contentWords.map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toString)).toArray.sortBy(_._1)
    kvContents shouldBe wcnts
  }

  test("RedisZsetRDD") {
    val redisZSetWithScore = sc.fromRedisZSetWithScore(zSetKey)
    val zsetWithScore = redisZSetWithScore.sortByKey().collect

    val redisZSet = sc.fromRedisZSet("all:words:cnt:sortedset")
    val zset = redisZSet.collect.sorted

    val redisZRangeWithScore = sc.fromRedisZRangeWithScore(zSetKey, 0, 15)
    val zrangeWithScore = redisZRangeWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRange = sc.fromRedisZRange(zSetKey, 0, 15)
    val zrange = redisZRange.collect.sorted

    val redisZRangeByScoreWithScore =
      sc.fromRedisZRangeByScoreWithScore(zSetKey, 3, 9)
    val zrangeByScoreWithScore = redisZRangeByScoreWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRangeByScore = sc.fromRedisZRangeByScore(zSetKey, 3, 9)
    val zrangeByScore = redisZRangeByScore.collect.sorted

    val wcnts = contentWords.map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toDouble))

    zsetWithScore should be(wcnts.toArray.sortBy(_._1))
    zset should be(wcnts.keys.toArray.sorted)
    zrangeWithScore should be(wcnts.toArray.sortBy(x => (x._2, x._1)).take(16))
    zrange should be(wcnts.toArray.sortBy(x => (x._2, x._1)).take(16).map(_._1))
    zrangeByScoreWithScore should be(wcnts.toArray.filter(x => x._2 >= 3 && x._2 <= 9)
      .sortBy(x => (x._2, x._1)))
    zrangeByScore should be(wcnts.toArray.filter(x => x._2 >= 3 && x._2 <= 9).map(_._1).sorted)
  }

  test("RedisHashRDD") {
    val redisHashRDD = sc.fromRedisHash(hashKey)
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = contentWords.map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toString)).toArray.sortBy(_._1)
    hashContents should be(wcnts)
  }

  test("RedisListRDD") {
    val redisListRDD = sc.fromRedisList(listKey)
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = contentWords.sorted
    listContents should be(ws)
  }

  test("RedisSetRDD") {
    val redisSetRDD = sc.fromRedisSet(setKey)
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be(ws)
  }

  test("RedisHLLRDD") {
    val redisSetRDD = sc.fromRedisHLL( s"$hllPrefix-apple")
    redisSetRDD.count() should be (1)
    redisSetRDD.take(1)(0)._2 should be (2)

    val redisSetRDDp = sc.fromRedisHLL(s"$hllPrefix-p*")
    redisSetRDDp.count() should be (2)
    redisSetRDDp.take(1)(0)._2 should be (1)

    val all = sc.fromRedisHLL(s"$hllPrefix-*")
    all.count() should be (3)
  }

  test("Expire") {
    val expireTime = 1
    val prefix = s"#expire in $expireTime#:"
    val wcnts = sc.parallelize(contentWords).map((_, 1)).
      reduceByKey(_ + _).map(x => (prefix + x._1, x._2.toString))
    val wds = sc.parallelize(contentWords)

    sc.toRedisKV(wcnts, expireTime)
    sc.toRedisZSET(wcnts, prefix + zSetKey, expireTime)
    sc.toRedisHASH(wcnts, prefix + hashKey, expireTime)
    sc.toRedisLIST(wds, prefix + listKey, expireTime)
    sc.toRedisSET(wds, prefix + setKey, expireTime)
    Thread.sleep(expireTime * 1000 + 1)
    sc.fromRedisKeyPattern(prefix + "*").count should be(0)
  }

}
