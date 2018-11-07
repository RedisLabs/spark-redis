package com.redislabs.provider.redis.env

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.spark.SparkConf

/**
  * @author The Viet Nguyen
  */
trait RedisStandaloneEnv extends Env {

  override val conf: SparkConf = new SparkConf()
    .setMaster("local[*]").setAppName(getClass.getName)
    .set("spark.redis.host", redisHost)
    .set("spark.redis.port", s"$redisPort")
    .set("spark.redis.auth", redisAuth)

  override val redisConfig: RedisConfig =
    new RedisConfig(RedisEndpoint(redisHost, redisPort, redisAuth))
}
