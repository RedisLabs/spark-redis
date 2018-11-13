package com.redislabs.provider.redis.stream

import com.redislabs.provider.redis.{SparkStreamingRedisSuite, _}
import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.streaming.{ConsumerConfig, Earliest}
import com.redislabs.provider.redis.util.TestUtils
import org.apache.spark.storage.StorageLevel
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Millis, Span}
import redis.clients.jedis.EntryID

import scala.collection.JavaConversions._

trait RedisXStreamSuite extends SparkStreamingRedisSuite with Matchers {

  // timeout for eventually function
  implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(5000, Millis)))

  test("basic createRedisXStream") {
    val streamKey = TestUtils.generateRandomKey()

    // the data can be written to the stream earlier than we start receiver, so set offset to Earliest
    val stream = ssc.createRedisXStream(Seq(ConsumerConfig(streamKey, "g1", "c1", Earliest)), StorageLevel.MEMORY_ONLY)

    val _redisConfig = redisConfig // to make closure serializable

    // iterate over items and save to redis as K/V
    stream.foreachRDD { rdd =>
      rdd.foreach { item =>
        val k = s"${item.streamKey}:${item.id.v1}-${item.id.v2}"
        val jedis = _redisConfig.connectionForKey(k)
        jedis.set(k, item.fields.mkString(" "))
      }
    }

    // write to stream
    val jedis = redisConfig.connectionForKey(streamKey)
    jedis.xadd(streamKey, new EntryID(1, 0), Map("a" -> "1", "z" -> "4"))
    jedis.xadd(streamKey, new EntryID(1, 1), Map("b" -> "2"))
    jedis.xadd(streamKey, new EntryID(2, 0), Map("c" -> "3"))

    ssc.start()

    // eventually there should be KVs
    eventually {
      jedisGet(s"$streamKey:1-0") shouldBe "a -> 1 z -> 4"
      jedisGet(s"$streamKey:1-1") shouldBe "b -> 2"
      jedisGet(s"$streamKey:2-0") shouldBe "c -> 3"
    }
  }

  def jedisGet(key: String): String = {
    redisConfig.connectionForKey(key).get(key)
  }

}
