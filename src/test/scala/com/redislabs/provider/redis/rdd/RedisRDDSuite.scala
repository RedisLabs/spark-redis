package com.redislabs.provider.redis.rdd

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, FunSuite}
import scala.io.Source.fromInputStream
import com.redislabs.provider.redis._
import org.scalatest.Matchers._

class RedisRDDSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {

  override def beforeAll() {
    super.beforeAll()

    sc = new SparkContext(new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "localhost")
      .set("redis.port", "6379")
      .set("redis.auth", "")
    )
    content = fromInputStream(getClass.getClassLoader.getResourceAsStream("blog")).
      getLines.toArray.mkString("\n")

    redisConfigStandalone = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379))
    redisConfigCluster = new RedisConfig(new RedisEndpoint("127.0.0.1", 7379))

    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toString))

    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))

    sc.toRedisKV(wcnts)(redisConfigStandalone)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset")(redisConfigStandalone)
    sc.toRedisHASH(wcnts, "all:words:cnt:hash" )(redisConfigStandalone)
    sc.toRedisLIST(wds, "all:words:list" )(redisConfigStandalone)
    sc.toRedisSET(wds, "all:words:set")(redisConfigStandalone)

    sc.toRedisKV(wcnts)(redisConfigCluster)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset" )(redisConfigCluster)
    sc.toRedisHASH(wcnts, "all:words:cnt:hash")(redisConfigCluster)
    sc.toRedisLIST(wds, "all:words:list" )(redisConfigCluster)
    sc.toRedisSET(wds, "all:words:set")(redisConfigCluster)
  }

  test("RedisKVRDD - default(standalone)") {
    val redisKVRDD = sc.fromRedisKeyPattern("*").getKV
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisKVRDD - standalone") {
    implicit val c: RedisConfig = redisConfigStandalone
    val redisKVRDD = sc.fromRedisKeyPattern("*").getKV
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

//  test("RedisKVRDD - with keys list") {
//    implicit val c: RedisConfig = redisConfigStandalone
//    val redisKVRDD = sc.fromRedisKeys(Array("cluster", "RDD"))
//    val kvContents = redisKVRDD.collect
//    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
//      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
//    kvContents should be (wcnts)
//  }
  test("RedisKVRDD - cluster") {
    implicit val c: RedisConfig = redisConfigCluster
    val redisKVRDD = sc.fromRedisKeyPattern("*").getKV
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisZsetRDD - default(standalone)") {
    val redisZSetRDD = sc.fromRedisKeyPattern("all:words:cnt:sortedset").getZSet
    val zsetContents = redisZSetRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString + ".0")).toArray.sortBy(_._1)
    zsetContents should be (wcnts)
  }

  test("RedisZsetRDD - standalone") {
    implicit val c: RedisConfig = redisConfigStandalone
    val redisZSetRDD = sc.fromRedisKeyPattern("all:words:cnt:sortedset").getZSet
    val zsetContents = redisZSetRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString + ".0")).toArray.sortBy(_._1)
    zsetContents should be (wcnts)
  }

  test("RedisZsetRDD - cluster") {
    implicit val c: RedisConfig = redisConfigCluster
    val redisZSetRDD = sc.fromRedisKeyPattern("all:words:cnt:sortedset").getZSet
    val zsetContents = redisZSetRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString + ".0")).toArray.sortBy(_._1)
    zsetContents should be (wcnts)
  }

  test("RedisHashRDD - default(standalone)") {
    val redisHashRDD = sc.fromRedisKeyPattern( "all:words:cnt:hash").getHash
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisHashRDD - standalone") {
    implicit val c: RedisConfig = redisConfigStandalone
    val redisHashRDD = sc.fromRedisKeyPattern( "all:words:cnt:hash").getHash
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisHashRDD - cluster") {
    implicit val c: RedisConfig = redisConfigCluster
    val redisHashRDD = sc.fromRedisKeyPattern( "all:words:cnt:hash").getHash
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisListRDD - default(standalone)") {
    val redisListRDD = sc.fromRedisKeyPattern( "all:words:list").getList
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisListRDD - standalone") {
    implicit val c: RedisConfig = redisConfigStandalone
    val redisListRDD = sc.fromRedisKeyPattern( "all:words:list").getList
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisListRDD - cluster") {
    implicit val c: RedisConfig = redisConfigCluster
    val redisListRDD = sc.fromRedisKeyPattern("all:words:list").getList
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisSetRDD - default(standalone)") {
    val redisSetRDD = sc.fromRedisKeyPattern( "all:words:set").getSet
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  test("RedisSetRDD - standalone") {
    implicit val c: RedisConfig = redisConfigStandalone
    val redisSetRDD = sc.fromRedisKeyPattern( "all:words:set").getSet
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  test("RedisSetRDD - cluster") {
    implicit val c: RedisConfig = redisConfigCluster
    val redisSetRDD = sc.fromRedisKeyPattern("all:words:set").getSet
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
  var redisConfigStandalone: RedisConfig = _
  var redisConfigCluster: RedisConfig = _
  var content: String = _
}
