package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO}
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.EntryID

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class ConnectionUtilsTest extends FunSuite with Matchers with RedisStandaloneEnv {

  test("xinfo") {
    val streamKey = TestUtils.generateRandomKey()
    val conn = redisConfig.connectionForKey(streamKey)
    val data = Map("key" -> "value").asJava
    val entryId = conn.xadd(streamKey, new EntryID(0, 1), data)
    val info = conn.xinfo(XINFO.StreamKey, streamKey)
    info.get(XINFO.LastEntry) shouldBe Some(entryId.toString)
  }
}
