package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis._
import org.scalatest.{FunSuite, ShouldMatchers}
import redis.clients.util.JedisClusterCRC16

class RedisConfigSuite extends FunSuite with ShouldMatchers {

  val redisStandaloneConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379, "passwd"))
  val redisClusterConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 7379))

  test("getNodesBySlots") {
    assert(redisStandaloneConfig.getNodesBySlots(0, 16383).size == 1)
    assert(redisClusterConfig.getNodesBySlots(0, 16383).size == 7)
  }

//  test("connectionForKey") {
//    val key = "connectionForKey"
//    val slot = JedisClusterCRC16.getSlot(key)
//    val standaloneConn = redisStandaloneConfig.connectionForKey(key)
//    assert()
//    assert()
//  }

  test("getHost") {
    val key = "getHost"
    val slot = JedisClusterCRC16.getSlot(key)
    val standaloneHost = redisStandaloneConfig.getHost(key)
    assert(standaloneHost.startSlot <= slot && standaloneHost.endSlot >= slot)
    val clusterHost = redisClusterConfig.getHost(key)
    assert(clusterHost.startSlot <= slot && clusterHost.endSlot >= slot)
  }

  test("getNodes") {
    assert(redisStandaloneConfig.getNodes(new RedisEndpoint("127.0.0.1", 6379, "passwd")).size == 1)
    assert(redisClusterConfig.getNodes(new RedisEndpoint("127.0.0.1", 7379)).size == 7)
  }
}
