package com.redislabs.provider.redis

import com.redislabs.provider.redis.env.RedisContainers
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.util.JedisClusterCRC16

class RedisConfigSuite extends FunSuite with Matchers {
  // This suite has no env trait, so it asks RedisContainers for both ports directly.
  private val standalonePort = RedisContainers.standalonePort
  private val clusterPort = RedisContainers.clusterPort

  val redisStandaloneConfig = new RedisConfig(RedisEndpoint(host = "127.0.0.1", port = standalonePort, auth = "passwd"))
  val redisClusterConfig = new RedisConfig(RedisEndpoint(host = "127.0.0.1", port = clusterPort))

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
    redisStandaloneConfig.getNodes(RedisEndpoint(host = "127.0.0.1", port = standalonePort, auth = "passwd"))
      .length shouldBe 1
    redisClusterConfig.getNodes(RedisEndpoint(host = "127.0.0.1", port = clusterPort)).length shouldBe 7
  }
}
