package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import org.apache.spark.sql.redis.stream.RedisSource
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.StreamEntryID

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class ConnectionUtilsTest extends FunSuite with Matchers with RedisStandaloneEnv {

  test("streamLastId") {
    val streamKey = TestUtils.generateRandomKey()
    val conn = redisConfig.connectionForKey(streamKey)
    val data = Map("key" -> "value").asJava
    val entryId = conn.xadd(streamKey, new StreamEntryID(0, 1), data)
    RedisSource.streamLastId(conn, streamKey) shouldBe entryId.toString
  }

  test("streamLastId reads a stream whose entries are all trimmed") {
    val streamKey = TestUtils.generateRandomKey()
    val conn = redisConfig.connectionForKey(streamKey)
    try {
      val entryId = conn.xadd(streamKey, new StreamEntryID(0, 1), Map("key" -> "value").asJava)
      // XINFO STREAM then returns first-entry and last-entry as nil
      conn.xtrim(streamKey, 0, false)
      RedisSource.streamLastId(conn, streamKey) shouldBe entryId.toString
    } finally conn.close()
  }
}
