package com.redislabs.provider.redis.env

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.spark.SparkConf

/**
 * Standalone with user/password authentication
 */
trait RedisStandaloneAclEnv extends Env {

  override val conf: SparkConf = new SparkConf()
    .setMaster("local[*]").setAppName(getClass.getName)
    .set("spark.redis.host", redisHost)
    .set("spark.redis.port", s"$redisPort")
    .set("spark.redis.user", user)
    .set("spark.redis.auth", userPassword)
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
    .set("spark.driver.bindAddress", "127.0.0.1")

  override val redisConfig: RedisConfig =
    new RedisConfig(RedisEndpoint(
      host = redisHost,
      port = redisPort,
      user = user,
      auth = userPassword))
}
