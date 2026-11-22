package com.redislabs.provider.redis

import java.lang.management.ManagementFactory
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import javax.management.ObjectName
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.JedisPool

import scala.collection.JavaConverters._

/**
  * Unit tests for the per-endpoint pool cache in ConnectionPool (issue #90).
  * No Redis server is needed: creating a JedisPool opens no connection.
  */
class ConnectionPoolThreadSafetyTest extends FunSuite with Matchers {

  private val mbeanServer = ManagementFactory.getPlatformMBeanServer

  /**
    * Pools built by ConnectionPool that are still registered in JMX. Commons Pool registers every
    * pool at construction and unregisters it only on close, so this also counts pools that were
    * built and then dropped without being closed.
    */
  private def registeredSparkRedisPools(): Int =
    mbeanServer.queryNames(new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,*"), null)
      .asScala
      .count(name => String.valueOf(mbeanServer.getAttribute(name, "FactoryType")).contains("SparkRedisJedisFactory"))

  test("concurrent first use of an endpoint creates exactly one pool") {
    val threads = 16
    val executor = Executors.newFixedThreadPool(threads)
    try {
      (1 to 10).foreach { round =>
        // A fresh endpoint each round, so all threads race to create its pool. Nothing connects to it.
        val endpoint = RedisEndpoint(host = s"issue-90-round-$round.invalid")
        val poolsBefore = registeredSparkRedisPools()
        val start = new CountDownLatch(1)
        val futures = (1 to threads).map { _ =>
          executor.submit(new Callable[JedisPool] {
            override def call(): JedisPool = {
              start.await()
              ConnectionPool.poolFor(endpoint)
            }
          })
        }
        start.countDown()
        val pools = futures.map(_.get(30, TimeUnit.SECONDS))

        withClue(s"round $round: ") {
          pools.foreach(_ should be theSameInstanceAs pools.head)
          registeredSparkRedisPools() - poolsBefore shouldBe 1
        }
      }
    } finally {
      executor.shutdownNow()
    }
  }

  test("an endpoint that already has a pool reuses it") {
    val endpoint = RedisEndpoint(host = "issue-90-reuse.invalid")
    val pool = ConnectionPool.poolFor(endpoint)
    val poolsBefore = registeredSparkRedisPools()

    ConnectionPool.poolFor(endpoint.copy()) should be theSameInstanceAs pool
    registeredSparkRedisPools() shouldBe poolsBefore
  }
}
