package com.redislabs.provider.redis

import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisDataException}

import scala.collection.mutable

/** [[ConnectionPool.retryWhileMaxClients]], on a fake clock: no Redis needed. */
class ConnectionPoolRetryTest extends FunSuite with Matchers {

  /** How the pool reports a server at its maxclients: the error reply is the cause. */
  private def maxClientsRefusal = new JedisConnectionException("Could not get a resource from the pool",
    new JedisDataException("ERR max number of clients reached"))

  /** A clock that only moves when the retry sleeps, and the sleeps it was asked for. */
  private class FakeClock {
    var nowMillis = 0L
    val sleeps: mutable.Buffer[Long] = mutable.Buffer()
    def sleep(millis: Long): Unit = { sleeps += millis; nowMillis += millis }
    def nanoTime(): Long = nowMillis * 1000000
  }

  private def retry[A](clock: FakeClock, maxWaitMillis: Long = 60000)(attempt: => A): A =
    ConnectionPool.retryWhileMaxClients("redis.example.com:6379", maxWaitMillis, clock.sleep, () => clock.nanoTime())(
      attempt)

  test("a connection refused at maxclients is retried, waiting longer each time up to 512 ms") {
    val clock = new FakeClock
    var attempts = 0
    val result = retry(clock) {
      attempts += 1
      if (attempts <= 9) throw maxClientsRefusal
      "connection"
    }
    result shouldBe "connection"
    clock.sleeps shouldBe Seq(8, 16, 32, 64, 128, 256, 512, 512, 512)
  }

  test("the retry gives up once the wait would go past the limit, saying how long it retried, with the refusal") {
    val clock = new FakeClock
    var attempts = 0
    val e = the[JedisConnectionException] thrownBy retry(clock, maxWaitMillis = 5000) {
      attempts += 1
      throw maxClientsRefusal
    }
    clock.nowMillis should be <= 5000L
    clock.nowMillis should be > 5000L - 512
    attempts shouldBe clock.sleeps.size + 1
    e.getMessage should include("redis.example.com:6379")
    e.getMessage should include("max number of clients reached")
    e.getMessage should include("maxclients")
    ConnectionPool.isMaxClients(e.getCause) shouldBe true
  }

  test("any other connection failure, with or without a cause, fails at once") {
    Seq(new JedisConnectionException("Connection refused"),
      new JedisConnectionException("Could not get a resource from the pool", new JedisDataException("WRONGPASS"))
    ).foreach { failure =>
      withClue(failure.toString) {
        val clock = new FakeClock
        val e = the[JedisConnectionException] thrownBy retry(clock)(throw failure)
        e shouldBe theSameInstanceAs(failure)
        clock.sleeps shouldBe empty
      }
    }
  }

  test("a max-clients refusal is recognized anywhere in the cause chain, and nowhere else") {
    ConnectionPool.isMaxClients(maxClientsRefusal) shouldBe true
    ConnectionPool.isMaxClients(new RuntimeException(maxClientsRefusal)) shouldBe true
    ConnectionPool.isMaxClients(new JedisDataException("ERR max number of clients reached")) shouldBe true
    ConnectionPool.isMaxClients(
      new JedisDataException("ERR max number of clients + cluster connections reached")) shouldBe true
    ConnectionPool.isMaxClients(new JedisConnectionException("ERR max number of clients reached")) shouldBe false
    ConnectionPool.isMaxClients(new JedisDataException(null: String)) shouldBe false
    ConnectionPool.isMaxClients(new JedisConnectionException("timeout")) shouldBe false
  }
}
