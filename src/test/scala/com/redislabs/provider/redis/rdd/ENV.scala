package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.RedisConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait ENV {
  var sc: SparkContext = _
  var redisConfig: RedisConfig = _
  var content: String = _
  var spark: SparkSession = _
}

