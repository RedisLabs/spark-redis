package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.ReadWriteConfig
import org.scalatest.Matchers
import redis.clients.jedis.util.JedisClusterCRC16

import scala.collection.JavaConverters._

trait RedisKeysSuite extends RedisRddSuite with Keys with Matchers {

  implicit val readWriteConfig: ReadWriteConfig = ReadWriteConfig.Default

  test("getKeys") {
    val returnedKeys = getKeys(redisConfig.hosts, 0, 1024, "*")
      .toArray.sorted

    val targetKeys = (sc.parallelize(content.split("\\W+")).collect :+
      "all:words:cnt:sortedset" :+
      "all:words:cnt:hash" :+
      "all:words:list" :+
      "all:words:set").filter(x => {
      val slot = JedisClusterCRC16.getSlot(x)
      !x.isEmpty && slot >= 0 && slot <= 1024
    }).distinct.sorted

    returnedKeys should be(targetKeys)
  }

  test("groupKeysByNode") {
    val allkeys = getKeys(redisConfig.hosts, 0, 16383, "*")
    val nodeKeysPairs = groupKeysByNode(redisConfig.hosts, allkeys)
    val returnedCnt = nodeKeysPairs.map { x =>
      filterKeysByType(x._1.connect(), x._2, "string").length
    }
      .sum
    val targetCnt = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).distinct.count
    assert(returnedCnt == targetCnt)
  }

}
