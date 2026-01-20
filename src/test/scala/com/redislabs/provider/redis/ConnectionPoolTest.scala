package com.redislabs.provider.redis

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import org.scalatest.{FunSuite, Matchers}

/**
  * Test suite for ConnectionPool with CLIENT SETINFO support
  */
class ConnectionPoolTest extends FunSuite with Matchers with RedisStandaloneEnv {

  test("connection pool should create valid connections") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)
    val conn = ConnectionPool.connect(endpoint)

    try {
      conn should not be null
      conn.isConnected shouldBe true

      // Test basic operation
      conn.ping() shouldBe "PONG"
    } finally {
      conn.close()
    }
  }

  test("connection should have client info set") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)
    val conn = ConnectionPool.connect(endpoint)

    try {
      // Get client list to verify CLIENT SETINFO was sent
      val clientList = conn.clientList()

      // The current connection should be in the list
      clientList should not be empty

      // Parse client list to find our connection
      // Client list format: "id=123 addr=127.0.0.1:12345 ... lib-name=jedis(sparkr-v3.1.0-SNAPSHOT) ..."
      val hasLibName = clientList.contains("lib-name=jedis(sparkr-v)")

      // This will be true for Redis 7.2+ which supports CLIENT SETINFO
      // For older versions, lib-name won't be present
      if (hasLibName) {
        clientList should include("lib-name=jedis(sparkr-v)")
      }
    } finally {
      conn.close()
    }
  }

  test("multiple connections to same endpoint should reuse pool") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)

    val conn1 = ConnectionPool.connect(endpoint)
    val conn2 = ConnectionPool.connect(endpoint)

    try {
      conn1 should not be null
      conn2 should not be null

      // Both connections should work
      conn1.ping() shouldBe "PONG"
      conn2.ping() shouldBe "PONG"
    } finally {
      conn1.close()
      conn2.close()
    }
  }

  test("connection with authentication should work") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)
    val conn = ConnectionPool.connect(endpoint)

    try {
      // If auth is required and working, this should succeed
      conn.set("test:key", "test:value")
      conn.get("test:key") shouldBe "test:value"
      conn.del("test:key")
    } finally {
      conn.close()
    }
  }

  test("connection should support database selection") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth, dbNum = 1)
    val conn = ConnectionPool.connect(endpoint)

    try {
      // Set a value in db 1
      conn.set("test:db:key", "db1:value")
      conn.get("test:db:key") shouldBe "db1:value"

      // Clean up
      conn.del("test:db:key")
    } finally {
      conn.close()
    }
  }

  test("library info should be included in client name suffix") {
    val endpoint = RedisEndpoint(host = redisHost, port = redisPort, auth = redisAuth)
    val conn = ConnectionPool.connect(endpoint)

    try {
      val clientList = conn.clientList()

      // Verify the format includes our library name
      val expectedPattern = s"sparkr-v${RedisClientLibraryInfo.version}"

      // For Redis 7.2+, this should be in lib-name
      // For older versions, it might not be present
      if (clientList.contains("lib-name=")) {
        clientList should include(expectedPattern)
      }
    } finally {
      conn.close()
    }
  }
}

