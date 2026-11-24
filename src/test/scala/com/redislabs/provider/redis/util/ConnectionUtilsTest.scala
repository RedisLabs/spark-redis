package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import org.apache.spark.sql.redis.stream.RedisSource
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.StreamEntryID
import redis.clients.jedis.exceptions.JedisDataException

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

  test("streamLastIdIfExists reads the last id of a stream, and is None for a key that doesn't exist") {
    val streamKey = TestUtils.generateRandomKey()
    val conn = redisConfig.connectionForKey(streamKey)
    try {
      RedisSource.streamLastIdIfExists(conn, streamKey) shouldBe None
      val entryId = conn.xadd(streamKey, new StreamEntryID(0, 1), Map("key" -> "value").asJava)
      RedisSource.streamLastIdIfExists(conn, streamKey) shouldBe Some(entryId.toString)
    } finally conn.close()
  }

  test("streamLastIdIfExists throws for a key that isn't a stream, rather than treating it as missing") {
    val key = TestUtils.generateRandomKey()
    val conn = redisConfig.connectionForKey(key)
    try {
      conn.set(key, "not a stream")
      val e = the[JedisDataException] thrownBy RedisSource.streamLastIdIfExists(conn, key)
      e.getMessage should include("WRONGTYPE")
    } finally conn.close()
  }
}
