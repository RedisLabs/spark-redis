package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._
import scala.io.Source.fromInputStream

class KeysClusterSuite extends FunSuite with Keys with ENV with BeforeAndAfterAll with ShouldMatchers {

  override def beforeAll() {
    super.beforeAll()

    sc = new SparkContext(new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "7379")
    )
    content = fromInputStream(getClass.getClassLoader.getResourceAsStream("blog")).
      getLines.toArray.mkString("\n")

    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toString))

    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))

    // THERE IS NOT AUTH FOR CLUSTER
    redisConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 7379))

    // Flush all the hosts
    redisConfig.hosts.foreach( node => {
      val conn = node.connect
      conn.flushAll
      conn.close
    })

    sc.toRedisKV(wcnts)(redisConfig)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset")(redisConfig)
    sc.toRedisHASH(wcnts, "all:words:cnt:hash" )(redisConfig)
    sc.toRedisLIST(wds, "all:words:list" )(redisConfig)
    sc.toRedisSET(wds, "all:words:set")(redisConfig)
  }

  test("getKeys - cluster") {
    val returnedKeys = getKeys(redisConfig.hosts, 0, 1024, "*").toSet.toArray.sorted

    val targetKeys = (sc.parallelize(content.split("\\W+")).collect :+
      "all:words:cnt:sortedset" :+
      "all:words:cnt:hash" :+
      "all:words:list" :+
      "all:words:set").filter( x => {
      val slot = JedisClusterCRC16.getSlot(x)
      !x.isEmpty &&  slot >= 0 && slot <= 1024
    }).distinct.sorted

    returnedKeys should be (targetKeys)
  }

//  test("groupKeysByNode - cluster") {
//    val allkeys = getKeys(redisConfig.hosts, 0, 16383, "*").toSet.iterator
//    val nodeKeysPairs = groupKeysByNode(redisConfig.hosts, allkeys)
//
//    for (nodeKeys <- nodeKeysPairs) {
//      val node = nodeKeys._1
//      val keys = nodeKeys._2
//      for (key <- keys) {
//        val slot = JedisClusterCRC16.getSlot(key)
//        assert(slot >= node.startSlot && slot <= node.endSlot)
//      }
//    }
//  }

  test("groupKeysByNode - cluster") {
    val allkeys = getKeys(redisConfig.hosts, 0, 16383, "*").toSet.iterator
    val nodeKeysPairs = groupKeysByNode(redisConfig.hosts, allkeys)
    val returnedCnt = nodeKeysPairs.map(x => filterKeysByType(x._1.connect, x._2, "string").size).
      reduce(_ + _)
    val targetCnt = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).distinct.count
    assert(returnedCnt == targetCnt)
  }

  override def afterAll(): Unit = {
    sc.stop
    System.clearProperty("spark.driver.port")
  }
}
