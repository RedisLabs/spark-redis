package com.redislabs.provider.redis.stream

import com.redislabs.provider.redis.streaming._
import com.redislabs.provider.redis.util.TestUtils
import com.redislabs.provider.redis.{ConnectionPool, SparkStreamingRedisSuite}
import org.apache.spark.storage.StorageLevel
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Millis, Span}

/**
  * The DStream receivers hold one connection per consumer (Redis Stream) or per list key (Redis List) while they run.
  * The connections must go back to the pool when the receiver stops, otherwise every stop or restart leaks them.
  */
// scalastyle:off multiple.string.literals
trait RedisReceiverConnectionLeakSuite extends SparkStreamingRedisSuite with Matchers {

  // the List receiver blocks in BLPOP for up to 2 seconds before it notices that it was stopped
  implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  private def activeConnections(key: String): Int =
    ConnectionPool.poolFor(redisConfig.getHost(key).endpoint).getNumActive

  test("createRedisXStream returns its connections when the stream stops") {
    val streamKey = TestUtils.generateRandomKey()
    val before = activeConnections(streamKey)

    val stream = ssc.createRedisXStream(Seq(
      ConsumerConfig(streamKey, "g1", "c1", Earliest, block = 100),
      ConsumerConfig(streamKey, "g1", "c2", Earliest, block = 100)
    ), StorageLevel.MEMORY_ONLY)
    stream.foreachRDD(_ => ())
    ssc.start()

    eventually {
      activeConnections(streamKey) shouldBe before + 2
    }
    ssc.stop(stopSparkContext = false, stopGracefully = false)
    eventually {
      activeConnections(streamKey) shouldBe before
    }
  }

  test("createRedisStream returns its connections when the stream stops") {
    val listKey = TestUtils.generateRandomKey()
    val before = activeConnections(listKey)

    val stream = ssc.createRedisStream(Array(listKey), StorageLevel.MEMORY_ONLY)
    stream.foreachRDD(_ => ())
    ssc.start()

    eventually {
      activeConnections(listKey) shouldBe before + 1
    }
    ssc.stop(stopSparkContext = false, stopGracefully = false)
    eventually {
      activeConnections(listKey) shouldBe before
    }
  }
}
