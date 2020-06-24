package com.redislabs.provider.redis.env

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.spark.SparkConf

/**
  * @author The Viet Nguyen
  */
trait RedisClusterEnv extends Env {

  override val redisPort = 7379

  override val conf: SparkConf = new SparkConf()
    .setMaster("local[*]").setAppName(getClass.getName)
    .set("spark.redis.host", redisHost)
    .set("spark.redis.port", s"$redisPort")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
    .set("spark.driver.bindAddress", "127.0.0.1")

  override val redisConfig: RedisConfig = new RedisConfig(RedisEndpoint(redisHost, redisPort))
}
