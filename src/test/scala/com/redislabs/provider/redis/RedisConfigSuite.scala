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

  test("starting from a replica, the master keeps the initial endpoint's settings, the timeout included") {
    val replicaPort = RedisContainers.replicaPort
    assume(replicaPort.isDefined, "the replica runs only in a container")
    val fromReplica = RedisEndpoint(host = "127.0.0.1", port = replicaPort.get, auth = "passwd", timeout = 12345,
      clientName = "replica-test")
    val nodes = new RedisConfig(fromReplica).hosts
    nodes.map(n => (n.endpoint.host, n.endpoint.port)).toSeq shouldBe Seq(("127.0.0.1", standalonePort))
    nodes.map(_.endpoint.timeout).toSeq shouldBe Seq(12345)
    nodes.map(_.endpoint.clientName).toSeq shouldBe Seq("replica-test")
  }

  test("getNodes") {
    redisStandaloneConfig.getNodes(RedisEndpoint(host = "127.0.0.1", port = standalonePort, auth = "passwd"))
      .length shouldBe 1
    redisClusterConfig.getNodes(RedisEndpoint(host = "127.0.0.1", port = clusterPort)).length shouldBe 7
  }
}
