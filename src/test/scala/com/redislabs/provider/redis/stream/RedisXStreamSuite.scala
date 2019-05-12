package com.redislabs.provider.redis.stream

import com.redislabs.provider.redis.streaming.{ConsumerConfig, Earliest}
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.TestUtils
import com.redislabs.provider.redis.SparkStreamingRedisSuite
import com.redislabs.provider.redis.streaming._
import org.apache.spark.storage.StorageLevel
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Millis, Span}
import redis.clients.jedis.StreamEntryID

import scala.collection.JavaConversions._

// scalastyle:off multiple.string.literals
trait RedisXStreamSuite extends SparkStreamingRedisSuite with Matchers {

  // timeout for eventually function
  implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(5000, Millis)))

  test("createRedisXStream, 1 stream, 1 consumer") {
    val streamKey = TestUtils.generateRandomKey()

    // the data can be written to the stream earlier than we start receiver, so set offset to Earliest
    val stream = ssc.createRedisXStream(Seq(ConsumerConfig(streamKey, "g1", "c1", Earliest)), StorageLevel.MEMORY_ONLY)

    val _redisConfig = redisConfig // to make closure serializable

    // iterate over items and save to redis list
    // repartition to 1 to avoid concurrent write issues
    stream.repartition(1).foreachRDD { rdd =>
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
      conn.xadd(streamKey, new StreamEntryID(1, 0), Map("a" -> "1", "z" -> "4"))
      conn.xadd(streamKey, new StreamEntryID(1, 1), Map("b" -> "2"))
      conn.xadd(streamKey, new StreamEntryID(2, 0), Map("c" -> "3"))
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

  test("createRedisXStream, 1 stream, 2 consumers") {
    val streamKey = TestUtils.generateRandomKey()

    // the data can be written to the stream earlier than we start receiver, so set offset to Earliest
    val stream = ssc.createRedisXStream(Seq(
      ConsumerConfig(streamKey, "g1", "c1", Earliest, batchSize = 1),
      ConsumerConfig(streamKey, "g1", "c2", Earliest, batchSize = 1)
    ), StorageLevel.MEMORY_ONLY)

    val _redisConfig = redisConfig // to make closure serializable

    // iterate over items and save to redis list
    // repartition to 1 to avoid concurrent write issues
    stream.repartition(1).foreachRDD { rdd =>
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
      conn.xadd(streamKey, new StreamEntryID(1, 0), Map("a" -> "1", "z" -> "4"))
      conn.xadd(streamKey, new StreamEntryID(1, 1), Map("b" -> "2"))
      conn.xadd(streamKey, new StreamEntryID(2, 0), Map("c" -> "3"))
    }

    ssc.start()

    // eventually there should be items in the list, the ordering is not deterministic
    val listKey = s"$streamKey:list"
    withConnection(redisConfig.connectionForKey(listKey)) { conn =>
      eventually {
        conn.llen(listKey) shouldBe 3
        (1 to 3).map(_ => conn.lpop(listKey)).toSet shouldBe Set(
          "1-0 a -> 1 z -> 4",
          "1-1 b -> 2",
          "2-0 c -> 3"
        )
      }
    }
  }

  test("createRedisXStream, 2 streams, 2 consumers") {
    val stream1Key = TestUtils.generateRandomKey()
    val stream2Key = TestUtils.generateRandomKey()

    logInfo("stream1Key " + stream1Key)
    logInfo("stream2Key " + stream2Key)

    // the data can be written to the stream earlier than we start receiver, so set offset to Earliest
    val stream = ssc.createRedisXStream(Seq(
      ConsumerConfig(stream1Key, "g1", "c1", Earliest, batchSize = 1),
      ConsumerConfig(stream2Key, "g1", "c2", Earliest, batchSize = 1)
    ), StorageLevel.MEMORY_ONLY)

    val _redisConfig = redisConfig // to make closure serializable

    // iterate over items and save to redis list
    // repartition to 1 to avoid concurrent write issues
    stream.repartition(1).foreachRDD { rdd =>
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
    withConnection(redisConfig.connectionForKey(stream1Key)) { conn =>
      conn.xadd(stream1Key, new StreamEntryID(1, 0), Map("a" -> "1", "z" -> "4"))
    }
    withConnection(redisConfig.connectionForKey(stream2Key)) { conn =>
      conn.xadd(stream2Key, new StreamEntryID(1, 1), Map("b" -> "2"))
      conn.xadd(stream2Key, new StreamEntryID(2, 0), Map("c" -> "3"))
    }

    ssc.start()

    // eventually there should be items in the list
    val list1Key = s"$stream1Key:list"
    withConnection(redisConfig.connectionForKey(list1Key)) { conn =>
      eventually {
        conn.llen(list1Key) shouldBe 1
        conn.lpop(list1Key) should be("1-0 a -> 1 z -> 4")
      }
    }

    val list2Key = s"$stream2Key:list"
    withConnection(redisConfig.connectionForKey(list2Key)) { conn =>
      eventually {
        conn.llen(list2Key) shouldBe 2
        conn.lpop(list2Key) should be("1-1 b -> 2")
        conn.lpop(list2Key) should be("2-0 c -> 3")
      }
    }

  }

}
