package com.redislabs.provider.redis.util

import java.util.Collections.{singletonList, singletonMap}
import java.util.UUID

import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.Implicits.KvPipeline

/**
  * @author The Viet Nguyen
  */
class KvPipelineSuite extends FunSuite with Matchers {

  val redisHost = "127.0.0.1"
  val redisPort = 6379
  val redisAuth = "passwd"

  val redisConfig: RedisConfig =
    new RedisConfig(RedisEndpoint(redisHost, redisPort, redisAuth))

  test("k/v hgetall") {
    withConnection(redisConfig.initialHost) { conn =>
      val pipeline = conn.pipelined()
      val key = UUID.randomUUID().toString
      val hash = singletonMap("key", "value")
      pipeline.hmset(key, hash)
      pipeline.getHashAllWithKey(key)
      val results = pipeline.syncAndReturnAll()
      results should contain(key -> hash)
    }
  }

  test("k/v hmget") {
    withConnection(redisConfig.initialHost) { conn =>
      val pipeline = conn.pipelined()
      val key = UUID.randomUUID().toString
      val hash = singletonMap("key", "value")
      pipeline.hmset(key, hash)
      pipeline.getHashMultipleWithKey(key, "key")
      val results = pipeline.syncAndReturnAll()
      results should contain(key -> singletonList("value"))
    }
  }
}
