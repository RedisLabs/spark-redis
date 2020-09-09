package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.{RedisConfig, SparkRedisSuite, toRedisContext}
import org.scalatest.Matchers
import scala.collection.JavaConverters._

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
  val missingRedisKey: String = "missingRedisKey"

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
        (x._1, x._2.toString)
      }
    val wds = sc.parallelize(contentWords)
    // Flush all the hosts
    redisConfig.hosts.foreach(node => {
      val conn = node.connect()
      conn.flushAll
      conn.close()
    })
    sc.toRedisKV(wcnts)
    sc.toRedisZSET(wcnts, zSetKey)
    sc.toRedisHASH(wcnts, hashKey)
    sc.toRedisLIST(wds, listKey)
    sc.toRedisSET(wds, setKey)
  }

  test("RedisKVRDD") {
    val redisKVRDD = sc.fromRedisKV("*")
    val kvContents = redisKVRDD.sortByKey().collect
    val wrongTypeKeysRes = List(hashKey, zSetKey, listKey, setKey).map(sc.fromRedisKV(_).collect)
    val missingKeyRes = sc.fromRedisKV(missingRedisKey).collect()
    val wcnts = contentWords.map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toString)).toArray.sortBy(_._1)
    kvContents shouldBe wcnts
    all(wrongTypeKeysRes) should have size 0
    missingKeyRes should have size 0
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

    val wrongTypeKeysRes = List(hashKey, setKey, listKey, contentWords(0)).map(sc.fromRedisZSetWithScore(_).collect)
    val missingKeyRes = sc.fromRedisZSetWithScore(missingRedisKey).collect()

    val wcnts = contentWords.map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toDouble))

    zsetWithScore should be(wcnts.toArray.sortBy(_._1))
    zset should be(wcnts.keys.toArray.sorted)
    zrangeWithScore should be(wcnts.toArray.sortBy(x => (x._2, x._1)).take(16))
    zrange should be(wcnts.toArray.sortBy(x => (x._2, x._1)).take(16).map(_._1))
    zrangeByScoreWithScore should be(wcnts.toArray.filter(x => x._2 >= 3 && x._2 <= 9)
      .sortBy(x => (x._2, x._1)))
    zrangeByScore should be(wcnts.toArray.filter(x => x._2 >= 3 && x._2 <= 9).map(_._1).sorted)
    all(wrongTypeKeysRes) should have length 0
    missingKeyRes should have length 0
  }

  test("RedisHashRDD") {
    val redisHashRDD = sc.fromRedisHash(hashKey)
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = contentWords.map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).sum.toString)).toArray.sortBy(_._1)
    val wrongTypeKeysRes = List(zSetKey, setKey, listKey, contentWords(0)).map(sc.fromRedisHash(_).collect)
    val missingKeyRes = sc.fromRedisHash(missingRedisKey).collect()

    hashContents should be(wcnts)
    all(wrongTypeKeysRes) should have length 0
    missingKeyRes should have length 0
  }

  test("RedisListRDD") {
    val redisListRDD = sc.fromRedisList(listKey)
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = contentWords.sorted
    val wrongTypeKeysRes = List(zSetKey, setKey, hashKey, contentWords(0)).map(sc.fromRedisList(_).collect)
    val missingKeyRes = sc.fromRedisList(missingRedisKey).collect()

    listContents should be(ws)
    all(wrongTypeKeysRes) should have length 0
    missingKeyRes should have length 0
  }

  test("RedisSetRDD") {
    val redisSetRDD = sc.fromRedisSet(setKey)
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    val wrongTypeKeysRes = List(zSetKey, listKey, hashKey, contentWords(0)).map(sc.fromRedisSet(_).collect)
    val missingKeyRes = sc.fromRedisSet(missingRedisKey).collect()

    setContents should be(ws)
    all(wrongTypeKeysRes) should have length 0
    missingKeyRes should have length 0
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
