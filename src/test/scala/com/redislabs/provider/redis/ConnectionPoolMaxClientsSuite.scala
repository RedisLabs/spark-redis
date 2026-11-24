package com.redislabs.provider.redis

import java.util.concurrent.atomic.AtomicInteger

import com.redislabs.provider.redis.env.RedisContainers
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.Jedis

/**
  * A server at its `maxclients` refuses new connections, and [[ConnectionPool.connect]] waits until it takes them
  * again. Each test lowers `maxclients` to the server's current clients, and restores it a moment later.
  */
class ConnectionPoolMaxClientsSuite extends FunSuite with Matchers {

  private val RefusedMillis = 400L

  // A timeout no other test uses gives each endpoint a new pool, so connect has to open a new connection.
  private val timeouts = new AtomicInteger(23456)

  /** Refuses new connections to host:port for [[RefusedMillis]], while `body` runs. */
  private def refusingNewConnections[A](host: String, port: Int, auth: Option[String])(body: => A): A = {
    val admin = new Jedis(host, port)
    try {
      auth.foreach(admin.auth)
      val original = admin.configGet("maxclients").get(1)
      val clients = RedisConfig.parseInfo(admin.info("clients"))("connected_clients")
      admin.configSet("maxclients", clients)
      val restore = new Thread(new Runnable {
        override def run(): Unit = { Thread.sleep(RefusedMillis); admin.configSet("maxclients", original) }
      })
      restore.start()
      try body finally restore.join()
    } finally {
      admin.close()
    }
  }

  private def connectsOnceAccepted(endpoint: RedisEndpoint, auth: Option[String]): Unit = {
    val (conn, waitedMillis) = refusingNewConnections(endpoint.host, endpoint.port, auth) {
      val start = System.nanoTime()
      val conn = ConnectionPool.connect(endpoint)
      (conn, (System.nanoTime() - start) / 1000000)
    }
    try {
      // it waited for the server, and the connection it returns works
      waitedMillis should be >= RefusedMillis - 100
      conn.ping() shouldBe "PONG"
    } finally {
      conn.close()
    }
  }

  test("with a password, connect waits while the server is at maxclients, then connects") {
    val endpoint = RedisEndpoint(host = "127.0.0.1", port = RedisContainers.standalonePort, auth = "passwd",
      timeout = timeouts.incrementAndGet())
    connectsOnceAccepted(endpoint, Some("passwd"))
  }

  test("without a password, connect waits while the server is at maxclients, then connects") {
    // With no AUTH, SELECT or client name, CLIENT SETINFO reads the refusal: the cluster's nodes have no password. A
    // cluster node words it "max number of clients + cluster connections reached".
    val endpoint = RedisEndpoint(host = "127.0.0.1", port = RedisContainers.clusterPort,
      timeout = timeouts.incrementAndGet())
    connectsOnceAccepted(endpoint, None)
  }
}
