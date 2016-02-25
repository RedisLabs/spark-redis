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
      //.set("redis.auth", "foobar")
    )
    content = fromInputStream(getClass.getClassLoader.getResourceAsStream("blog")).
      getLines.toArray.mkString("\n")

    clusterInfoStandalone = new ClusterInfo(new RedisEndpoint("127.0.0.1", 6379))
    clusterInfoCluster = new ClusterInfo(new RedisEndpoint("127.0.0.1", 7379))

    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toString))

    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))

    sc.toRedisKV(wcnts)(clusterInfoStandalone)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset")(clusterInfoStandalone)
    sc.toRedisHASH(wcnts, "all:words:cnt:hash" )(clusterInfoStandalone)
    sc.toRedisLIST(wds, "all:words:list" )(clusterInfoStandalone)
    sc.toRedisSET(wds, "all:words:set")(clusterInfoStandalone)

    sc.toRedisKV(wcnts)(clusterInfoCluster)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset" )(clusterInfoCluster)
    sc.toRedisHASH(wcnts, "all:words:cnt:hash")(clusterInfoCluster)
    sc.toRedisLIST(wds, "all:words:list" )(clusterInfoCluster)
    sc.toRedisSET(wds, "all:words:set")(clusterInfoCluster)
  }

  test("RedisKVRDD - default(standalone)") {
    val redisKVRDD = sc.fromRedisKeyPattern("*").getKV
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisKVRDD - standalone") {
    implicit val c: ClusterInfo = clusterInfoStandalone
    val redisKVRDD = sc.fromRedisKeyPattern("*").getKV
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisKVRDD - cluster") {
    implicit val c: ClusterInfo = clusterInfoCluster
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
    implicit val c: ClusterInfo = clusterInfoStandalone
    val redisZSetRDD = sc.fromRedisKeyPattern("all:words:cnt:sortedset").getZSet
    val zsetContents = redisZSetRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString + ".0")).toArray.sortBy(_._1)
    zsetContents should be (wcnts)
  }

  test("RedisZsetRDD - cluster") {
    implicit val c: ClusterInfo = clusterInfoCluster
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
    implicit val c: ClusterInfo = clusterInfoStandalone
    val redisHashRDD = sc.fromRedisKeyPattern( "all:words:cnt:hash").getHash
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisHashRDD - cluster") {
    implicit val c: ClusterInfo = clusterInfoCluster
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
    implicit val c: ClusterInfo = clusterInfoStandalone
    val redisListRDD = sc.fromRedisKeyPattern( "all:words:list").getList
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisListRDD - cluster") {
    implicit val c: ClusterInfo = clusterInfoCluster
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
    implicit val c: ClusterInfo = clusterInfoStandalone
    val redisSetRDD = sc.fromRedisKeyPattern( "all:words:set").getSet
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  test("RedisSetRDD - cluster") {
    implicit val c: ClusterInfo = clusterInfoCluster
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
  var clusterInfoStandalone: ClusterInfo = _
  var clusterInfoCluster: ClusterInfo = _
  var content: String = _
}
