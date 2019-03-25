package com.redislabs.provider.redis

import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.util.JedisClusterCRC16

class RedisConfigSuite extends FunSuite with Matchers {

  val redisStandaloneConfig = new RedisConfig(RedisEndpoint("127.0.0.1", 6379, "passwd"))
  val redisClusterConfig = new RedisConfig(RedisEndpoint("127.0.0.1", 7379))

  test("getNodesBySlots") {
    redisStandaloneConfig.getNodesBySlots(0, 16383).length shouldBe 1
    redisClusterConfig.getNodesBySlots(0, 16383).length shouldBe 7
  }

  test("getHost") {
    val key = "getHost"
    val slot = JedisClusterCRC16.getSlot(key)
    val standaloneHost = redisStandaloneConfig.getHost(key)
    assert(standaloneHost.startSlot <= slot && standaloneHost.endSlot >= slot)
    val clusterHost = redisClusterConfig.getHost(key)
    assert(clusterHost.startSlot <= slot && clusterHost.endSlot >= slot)
  }

  test("getNodes") {
    redisStandaloneConfig.getNodes(RedisEndpoint("127.0.0.1", 6379, "passwd")).length shouldBe 1
    redisClusterConfig.getNodes(RedisEndpoint("127.0.0.1", 7379)).length shouldBe 7
  }
}
