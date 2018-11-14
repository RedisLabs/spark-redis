package com.redislabs.provider.redis.stream

import com.redislabs.provider.redis.streaming.{ConsumerConfig, Earliest}
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.TestUtils
import com.redislabs.provider.redis.{SparkStreamingRedisSuite, _}
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

    // iterate over items and save to redis list
    stream.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        for (item <- partition) {
          val listKey = s"${item.streamKey}:list"
          withConnection(_redisConfig.connectionForKey(listKey)) { conn =>
            conn.rpush(listKey, s"${item.id.v1}-${item.id.v2} " + item.fields.mkString(" "))
          }
        }
      }
    }

    // write to stream
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      conn.xadd(streamKey, new EntryID(1, 0), Map("a" -> "1", "z" -> "4"))
      conn.xadd(streamKey, new EntryID(1, 1), Map("b" -> "2"))
      conn.xadd(streamKey, new EntryID(2, 0), Map("c" -> "3"))
    }

    ssc.start()

    // eventually there should be items in the list
    val listKey = s"$streamKey:list"
    withConnection(redisConfig.connectionForKey(listKey)) { conn =>
      eventually {
        conn.llen(listKey) shouldBe 3
        conn.lpop(listKey) should be("1-0 a -> 1 z -> 4")
        conn.lpop(listKey) should be("1-1 b -> 2")
        conn.lpop(listKey) should be("2-0 c -> 3")
      }
    }

  }

}
