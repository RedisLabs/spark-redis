package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.RedisConfig
import org.apache.spark.SparkContext

trait ENV {
  var sc: SparkContext = _
  var redisConfig: RedisConfig = _
  var content: String = _
}

