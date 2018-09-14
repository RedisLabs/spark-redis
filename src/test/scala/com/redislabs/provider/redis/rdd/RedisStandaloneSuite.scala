package com.redislabs.provider.redis.rdd

import org.apache.spark.SparkConf

/**
  * @author The Viet Nguyen
  */
trait RedisStandaloneSuite extends SparkRedisSuite {

  override val conf: SparkConf = new SparkConf()
    .setMaster("local").setAppName(getClass.getName)
    .set("redis.host", "127.0.0.1")
    .set("redis.port", "6379")
    .set("redis.auth", "passwd")
}
