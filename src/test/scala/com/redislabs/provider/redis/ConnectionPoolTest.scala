package com.redislabs.provider.redis

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import org.scalatest.{FunSuite, Matchers}

/**
  * Integration tests for ConnectionPool, specifically testing CLIENT SETINFO functionality.
  * Requires a running Redis server (provided by RedisStandaloneEnv).
  */
class ConnectionPoolTest extends FunSuite with Matchers with RedisStandaloneEnv {

  /**
    * Cancels the test on servers older than Redis 7.2, which introduced the lib-name / lib-ver
    * fields, detected from the CLIENT LIST reply the caller already holds.
    */
  private def assumeServerReportsClientLibraryInfo(clientList: String): Unit =
    assume(clientList.contains("lib-name="),
      "CLIENT SETINFO requires Redis 7.2+; this server does not report lib-name/lib-ver")

  test("connection should be established successfully") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)
    val conn = ConnectionPool.connect(endpoint)
    try {
      conn.ping() shouldBe "PONG"
    } finally {
      conn.close()
    }
  }

  test("CLIENT SETINFO should set library name visible in CLIENT LIST") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)
    val conn = ConnectionPool.connect(endpoint)
    try {
      val clientList = conn.clientList()
      assumeServerReportsClientLibraryInfo(clientList)
      clientList should include("lib-name=" + RedisClientLibraryInfo.libName)
    } finally {
      conn.close()
    }
  }

  test("CLIENT SETINFO should set library version visible in CLIENT LIST") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)
    val conn = ConnectionPool.connect(endpoint)
    try {
      val clientList = conn.clientList()
      assumeServerReportsClientLibraryInfo(clientList)
      clientList should include("lib-ver=" + RedisClientLibraryInfo.libVersion)
    } finally {
      conn.close()
    }
  }

  test("multiple connections should all have library info set") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)
    val conn1 = ConnectionPool.connect(endpoint)
    val conn2 = ConnectionPool.connect(endpoint)
    try {
      // One call covers both connections: they share a pool to a single server, so the
      // presence of the lib-* fields is a property of that server, not of either connection.
      val clientList1 = conn1.clientList()
      assumeServerReportsClientLibraryInfo(clientList1)

      // Verify from conn1's perspective
      clientList1 should include("lib-name=" + RedisClientLibraryInfo.libName)
      clientList1 should include("lib-ver=" + RedisClientLibraryInfo.libVersion)

      // Verify from conn2's perspective
      val clientList2 = conn2.clientList()
      clientList2 should include("lib-name=" + RedisClientLibraryInfo.libName)
      clientList2 should include("lib-ver=" + RedisClientLibraryInfo.libVersion)
    } finally {
      conn1.close()
      conn2.close()
    }
  }
}
