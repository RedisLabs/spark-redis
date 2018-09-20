package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.spark.SparkConf

/**
  * @author The Viet Nguyen
  */
trait RedisStandaloneSuite extends SparkRedisSuite {

  private val redisHost = "127.0.0.1"
  private val redisPort = 6379
  private val redisAuth = "passwd"

  override val conf: SparkConf = new SparkConf()
    .setMaster("local").setAppName(getClass.getName)
    .set("redis.host", redisHost)
    .set("redis.port", s"$redisPort")
    .set("redis.auth", redisAuth)

  redisConfig = new RedisConfig(RedisEndpoint(redisHost, redisPort, redisAuth))
}
