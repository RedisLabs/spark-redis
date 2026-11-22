package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.PipelineUtils.foreachWithPipeline
import com.redislabs.provider.redis.util.TestUtils
import com.redislabs.provider.redis.util.TestUtils.interceptSparkErr
import org.apache.spark.SparkException
import org.scalatest.Matchers
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.{Minutes, Span}
import redis.clients.jedis.exceptions.JedisDataException

/**
  * Writes that Redis rejects must fail instead of being silently dropped (issue #33).
  *
  * Each test writes to a key that holds a String, so Redis answers the write with a WRONGTYPE error.
  */
// scalastyle:off multiple.string.literals
trait RedisRddWriteErrorSuite extends SparkRedisSuite with Keys with Matchers with TimeLimits {

  implicit val redisConfig: RedisConfig

  // pipeline size 1 makes the error come back from an intermediate sync rather than the last one
  private val onePerPipeline = ReadWriteConfig(ReadWriteConfig.ScanCountDefault, 1,
    ReadWriteConfig.RddWriteIteratorGroupingSizeDefault)

  /** @return a new key that holds a String */
  private def stringKey(): String = {
    val key = TestUtils.generateRandomKey()
    withConnection(redisConfig.connectionForKey(key)) { conn =>
      conn.set(key, "a string")
    }
    key
  }

  private def assertWrongType(write: => Any): Unit = {
    val e = interceptSparkErr[SparkException](write)
    e.getMessage should include("WRONGTYPE")
  }

  test("toRedisLIST fails when the key holds a String") {
    val key = stringKey()
    assertWrongType(sc.toRedisLIST(sc.parallelize(Seq("a", "b")), key))
    assertWrongType(sc.toRedisLIST(sc.parallelize(Seq("a", "b", "c"), 1), key)(redisConfig, onePerPipeline))
  }

  test("toRedisFixedLIST fails when the key holds a String") {
    val key = stringKey()
    assertWrongType(sc.toRedisFixedLIST(sc.parallelize(Seq("a", "b")), key, 10))
  }

  test("toRedisSET fails when the key holds a String") {
    val key = stringKey()
    assertWrongType(sc.toRedisSET(sc.parallelize(Seq("a", "b")), key))
  }

  test("toRedisZSET fails when the key holds a String") {
    val key = stringKey()
    assertWrongType(sc.toRedisZSET(sc.parallelize(Seq(("a", "1"), ("b", "2"))), key))
  }

  test("toRedisHASH fails when the key holds a String") {
    val key = stringKey()
    assertWrongType(sc.toRedisHASH(sc.parallelize(Seq(("f1", "v1"), ("f2", "v2"))), key))
  }

  test("toRedisHASHes fails when a key holds a String") {
    val key = stringKey()
    val hashes = Seq((key, Map("f" -> "v")), (TestUtils.generateRandomKey(), Map("f" -> "v")))
    assertWrongType(sc.toRedisHASHes(sc.parallelize(hashes)))
    assertWrongType(sc.toRedisHASHes(sc.parallelize(hashes, 1))(redisConfig, onePerPipeline))
  }

  test("toRedisByteHASHes fails when a key holds a String") {
    val key = stringKey()
    assertWrongType(sc.toRedisByteHASHes(sc.parallelize(Seq((key.getBytes, Map("f".getBytes -> "v".getBytes))))))
  }

  test("toRedisLISTs fails when a key holds a String") {
    val key = stringKey()
    assertWrongType(sc.toRedisLISTs(sc.parallelize(Seq((key, Seq("a", "b"))))))
  }

  test("toRedisByteLISTs fails when a key holds a String") {
    val key = stringKey()
    assertWrongType(sc.toRedisByteLISTs(sc.parallelize(Seq((key.getBytes, Seq("a".getBytes))))))
  }

  test("foreachWithPipeline reports the Redis error and leaves the connection usable") {
    val key = stringKey()
    // hash tags keep every key in the slot of `key`, so they can share its connection on a cluster
    val otherKey = s"{$key}:other"
    withConnection(redisConfig.connectionForKey(key)) { conn =>
      val e = intercept[JedisDataException] {
        foreachWithPipeline(conn, Seq(1, 2, 3)) { (pipeline, i) =>
          if (i == 2) pipeline.rpush(key, "x") else pipeline.incr(s"{$key}:$i")
        }(ReadWriteConfig.Default)
      }
      e.getMessage should include("1 of 3")
      e.getMessage should include("WRONGTYPE")
      // the replies were all read, so the next command on this connection gets its own reply
      conn.set(otherKey, "v") shouldBe "OK"
      conn.get(otherKey) shouldBe "v"
    }
  }

  test("failed writes return their connections to the pool") {
    implicit val signaler: Signaler = ThreadSignaler
    val rw = ReadWriteConfig.Default
    val key = stringKey()
    val writes: Seq[() => Unit] = Seq(
      () => RedisContext.setHash(key, Iterator(("f", "v")), 0, redisConfig, rw),
      () => RedisContext.setZset(key, Iterator(("m", "1")), 0, redisConfig, rw),
      () => RedisContext.setSet(key, Iterator("m"), 0, redisConfig, rw),
      () => RedisContext.setList(key, Iterator("v"), 0, redisConfig, rw),
      () => RedisContext.setFixedList(key, 10, Iterator("v"), redisConfig, rw)
    )
    // the pool holds at most 250 connections per endpoint; a leak would block the next borrow forever
    failAfter(Span(2, Minutes)) {
      writes.foreach { write =>
        (1 to 300).foreach { _ =>
          intercept[JedisDataException](write())
        }
      }
    }
  }
}
