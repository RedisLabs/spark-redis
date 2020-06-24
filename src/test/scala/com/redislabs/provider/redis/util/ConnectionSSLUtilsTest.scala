package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.env.RedisStandaloneSSLEnv
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO}
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.StreamEntryID

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class ConnectionSSLUtilsTest extends FunSuite with Matchers with RedisStandaloneSSLEnv {

  test("xinfo") {
    val streamKey = TestUtils.generateRandomKey()
    val conn = redisConfig.connectionForKey(streamKey)
    val data = Map("key" -> "value").asJava
    val entryId = conn.xadd(streamKey, new StreamEntryID(0, 1), data)
    val info = conn.xinfo(XINFO.SubCommandStream, streamKey)
    info.get(XINFO.LastGeneratedId) shouldBe Some(entryId.toString)
  }
}
