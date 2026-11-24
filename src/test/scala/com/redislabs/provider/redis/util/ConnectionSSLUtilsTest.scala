package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.env.RedisStandaloneSSLEnv
import org.apache.spark.sql.redis.stream.RedisSource
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.StreamEntryID

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class ConnectionSSLUtilsTest extends FunSuite with Matchers with RedisStandaloneSSLEnv {

  test("streamLastId") {
    val streamKey = TestUtils.generateRandomKey()
    val conn = redisConfig.connectionForKey(streamKey)
    val data = Map("key" -> "value").asJava
    val entryId = conn.xadd(streamKey, new StreamEntryID(0, 1), data)
    RedisSource.streamLastId(conn, streamKey) shouldBe entryId.toString
  }
}
