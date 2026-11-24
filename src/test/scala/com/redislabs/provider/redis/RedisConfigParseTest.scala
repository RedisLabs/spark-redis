package com.redislabs.provider.redis

import org.scalatest.{FunSuite, Matchers}

/** How [[RedisConfig]] reads `INFO` and finds a slot's primary: no Redis needed. */
class RedisConfigParseTest extends FunSuite with Matchers {

  private val Primary =
    """# Replication
      |role:master
      |connected_slaves:3
      |slave0:ip=10.0.0.2,port=6380,state=online,offset=1234,lag=0
      |slave1:ip=10.0.0.3,port=6381,state=wait_bgsave,offset=0,lag=0
      |slave10:ip=::1,port=6390,state=online,offset=1234,lag=1
      |slave2:ip=10.0.0.4,port=6382,state=online,offset=1234,lag=0
      |master_failover_state:no-failover
      |master_replid:8a7e0c1f
      |""".stripMargin.replace("\n", "\r\n")

  test("INFO is read as field -> value, without section headers and blank lines") {
    val info = RedisConfig.parseInfo("# Server\r\nredis_version:7.4.1\r\n\r\n# Cluster\r\ncluster_enabled:1\r\n")
    info shouldBe Map("redis_version" -> "7.4.1", "cluster_enabled" -> "1")
  }

  test("an INFO value keeps the colons after the first, and a line without one is skipped") {
    RedisConfig.parseInfo("master_host:fe80::1\nnot a field\n") shouldBe Map("master_host" -> "fe80::1")
  }

  test("a primary's online replicas are read by name, in the order INFO numbers them, IPv6 included") {
    RedisConfig.onlineReplicas(RedisConfig.parseInfo(Primary)) shouldBe
      Seq(("10.0.0.2", 6380), ("10.0.0.4", 6382), ("::1", 6390))
  }

  test("a replica field without an ip, a valid port or a state is skipped") {
    val info = RedisConfig.parseInfo(
      "slave0:port=6380,state=online\nslave1:ip=10.0.0.3,port=x,state=online\nslave2:ip=10.0.0.4,port=6382\n")
    RedisConfig.onlineReplicas(info) shouldBe empty
  }

  test("a server without replicas has none") {
    RedisConfig.onlineReplicas(RedisConfig.parseInfo("role:master\r\nconnected_slaves:0\r\n")) shouldBe empty
  }

  private def node(port: Int, start: Int, end: Int, idx: Int = 0): RedisNode =
    RedisNode(RedisEndpoint(port = port), start, end, idx, 2)

  test("a slot's primary is the primary whose range holds it, never a replica") {
    val nodes = Array(node(7001, 0, 8191, idx = 1), node(7000, 0, 8191), node(7002, 8192, 16383))
    RedisConfig.primaryForSlot(nodes, 0).endpoint.port shouldBe 7000
    RedisConfig.primaryForSlot(nodes, 8191).endpoint.port shouldBe 7000
    RedisConfig.primaryForSlot(nodes, 8192).endpoint.port shouldBe 7002
    RedisConfig.primaryForSlot(nodes, 16383).endpoint.port shouldBe 7002
  }

  test("a slot no primary serves fails with an error that names it") {
    val e = the[IllegalStateException] thrownBy
      RedisConfig.primaryForSlot(Array(node(7000, 0, 8191), node(7001, 8192, 16383, idx = 1)), 9000)
    e.getMessage should include("hash slot 9000")
    e.getMessage should include("1 primaries")
  }
}
