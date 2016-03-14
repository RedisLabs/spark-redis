package com.redislabs.provider.redis.rdd

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, FunSuite}
import scala.io.Source.fromInputStream
import com.redislabs.provider.redis._

class RedisRDDStandaloneSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {

  override def beforeAll() {
    super.beforeAll()

    sc = new SparkContext(new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "6379")
      .set("redis.auth", "passwd")
    )
    content = fromInputStream(getClass.getClassLoader.getResourceAsStream("blog")).
      getLines.toArray.mkString("\n")

    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toString))

    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))

    redisConfig= new RedisConfig(new RedisEndpoint("127.0.0.1", 6379, "passwd"))

    sc.toRedisKV(wcnts)(redisConfig)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset")(redisConfig)
    sc.toRedisHASH(wcnts, "all:words:cnt:hash" )(redisConfig)
    sc.toRedisLIST(wds, "all:words:list" )(redisConfig)
    sc.toRedisSET(wds, "all:words:set")(redisConfig)
  }

  test("RedisKVRDD - default(standalone)") {
    val redisKVRDD = sc.fromRedisKV("*")
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisKVRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisKVRDD = sc.fromRedisKV("*")
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisZsetRDD - default(standalone)") {
    val redisZSetRDD = sc.fromRedisZSetWithScore("all:words:cnt:sortedset")
    val zsetContents = redisZSetRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toDouble)).toArray.sortBy(_._1)
    zsetContents should be (wcnts)
  }

  test("RedisZsetRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisZSetRDD = sc.fromRedisZSetWithScore("all:words:cnt:sortedset")
    val zsetContents = redisZSetRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toDouble)).toArray.sortBy(_._1)
    zsetContents should be (wcnts)
  }

  test("RedisHashRDD - default(standalone)") {
    val redisHashRDD = sc.fromRedisHash( "all:words:cnt:hash")
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisHashRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisHashRDD = sc.fromRedisHash( "all:words:cnt:hash")
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisListRDD - default(standalone)") {
    val redisListRDD = sc.fromRedisList( "all:words:list")
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisListRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisListRDD = sc.fromRedisList( "all:words:list")
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisSetRDD - default(standalone)") {
    val redisSetRDD = sc.fromRedisSet( "all:words:set")
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  test("RedisSetRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisSetRDD = sc.fromRedisSet( "all:words:set")
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  test("Expire - default(standalone)") {
    val expireTime = 1
    val prefix = s"#expire in ${expireTime}#:"
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (prefix + x._1, x._2.toString))
    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))
    sc.toRedisKV(wcnts, expireTime)
    sc.toRedisZSET(wcnts, prefix + "all:words:cnt:sortedset", expireTime)
    sc.toRedisHASH(wcnts, prefix + "all:words:cnt:hash", expireTime)
    sc.toRedisLIST(wds, prefix + "all:words:list", expireTime)
    sc.toRedisSET(wds, prefix + "all:words:set", expireTime)
    Thread.sleep(expireTime * 1000 + 1)
    sc.fromRedisKeyPattern(prefix + "*").count should be (0)
  }

  test("Expire - standalone") {
    val expireTime = 1
    val prefix = s"#expire in ${expireTime}#:"
    implicit val c: RedisConfig = redisConfig
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (prefix + x._1, x._2.toString))
    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))
    sc.toRedisKV(wcnts, expireTime)
    sc.toRedisZSET(wcnts, prefix + "all:words:cnt:sortedset", expireTime)
    sc.toRedisHASH(wcnts, prefix + "all:words:cnt:hash", expireTime)
    sc.toRedisLIST(wds, prefix + "all:words:list", expireTime)
    sc.toRedisSET(wds, prefix + "all:words:set", expireTime)
    Thread.sleep(expireTime * 1000 + 1)
    sc.fromRedisKeyPattern(prefix + "*").count should be (0)
  }

  override def afterAll(): Unit = {
    sc.stop
    System.clearProperty("spark.driver.port")
  }
}

