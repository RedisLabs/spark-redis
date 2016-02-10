package com.redislabs.provider.redis.rdd

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, FunSuite}
import scala.io.Source.fromInputStream
import com.redislabs.provider.redis._
import org.scalatest.Matchers._

class RedisRDDSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {
  override def beforeAll() {
    super.beforeAll()

    sc = new SparkContext(new SparkConf().setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "localhost").set("redis.port", "6379"))

    redisStandaloneDB = ("127.0.0.1", 6379)
    redisClusterDB = ("127.0.0.1", 7379)
    content = fromInputStream(getClass.getClassLoader.getResourceAsStream("blog")).
      getLines.toArray.mkString("\n")

    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toString))
    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))

    sc.toRedisKV(wcnts)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset" )
    sc.toRedisHASH(wcnts, "all:words:cnt:hash")
    sc.toRedisLIST(wds, "all:words:list" )
    sc.toRedisSET(wds, "all:words:set")

    sc.toRedisKV(wcnts)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset")
    sc.toRedisHASH(wcnts, "all:words:cnt:hash" )
    sc.toRedisLIST(wds, "all:words:list" )
    sc.toRedisSET(wds, "all:words:set")
  }

  test("RedisKVRDD - standalone") {
    val redisKVRDD = sc.fromRedisKeyPattern(redisStandaloneDB, "*").getKV
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisKVRDD - cluster") {
    val redisKVRDD = sc.fromRedisKeyPattern(redisClusterDB, "*").getKV
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisZsetRDD - standalone") {
    val redisZSetRDD = sc.fromRedisKeyPattern(redisStandaloneDB, "all:words:cnt:sortedset").getZSet
    val zsetContents = redisZSetRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString + ".0")).toArray.sortBy(_._1)
    zsetContents should be (wcnts)
  }

  test("RedisZsetRDD - cluster") {
    val redisZSetRDD = sc.fromRedisKeyPattern(redisClusterDB, "all:words:cnt:sortedset").getZSet
    val zsetContents = redisZSetRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString + ".0")).toArray.sortBy(_._1)
    zsetContents should be (wcnts)
  }

  test("RedisHashRDD - standalone") {
    val redisHashRDD = sc.fromRedisKeyPattern(redisStandaloneDB, "all:words:cnt:hash").getHash
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisHashRDD - cluster") {
    val redisHashRDD = sc.fromRedisKeyPattern(redisClusterDB, "all:words:cnt:hash").getHash
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisListRDD - standalone") {
    val redisListRDD = sc.fromRedisKeyPattern(redisStandaloneDB, "all:words:list").getList
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisListRDD - cluster") {
    val redisListRDD = sc.fromRedisKeyPattern(redisClusterDB, "all:words:list").getList
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisSetRDD - standalone") {
    val redisSetRDD = sc.fromRedisKeyPattern(redisStandaloneDB, "all:words:set").getSet
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  test("RedisSetRDD - cluster") {
    val redisSetRDD = sc.fromRedisKeyPattern(redisClusterDB, "all:words:set").getSet
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  override def afterAll(): Unit = {
    sc.stop
    System.clearProperty("spark.driver.port")
  }
}

trait ENV {
  var sc: SparkContext = _
  var redisStandaloneDB: (String, Int) = _
  var redisClusterDB: (String, Int) = _
  var content: String = _
}
