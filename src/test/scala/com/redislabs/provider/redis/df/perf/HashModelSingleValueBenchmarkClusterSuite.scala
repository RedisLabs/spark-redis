package com.redislabs.provider.redis.df.perf

import com.redislabs.provider.redis.df.SingleValueBenchmarkSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import org.apache.spark.sql.redis.SqlOptionModelHash

/**
  * @author The Viet Nguyen
  */
class HashModelSingleValueBenchmarkClusterSuite extends SingleValueBenchmarkSuite
  with RedisClusterEnv {

  override def suiteTags: String = s"${super.suiteTags}, Hash"

  override def persistentModel: String = SqlOptionModelHash
}
