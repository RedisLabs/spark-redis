package com.redislabs.provider.redis.df.benchmark.cluster

import com.redislabs.provider.redis.df.benchmark.SingleValueBenchmarkSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import org.apache.spark.sql.redis.SqlOptionModelHash

/**
  * @author The Viet Nguyen
  */
class HashModelSingleValueClusterBenchmarkSuite extends SingleValueBenchmarkSuite
  with RedisClusterEnv {

  override def suiteTags: String = s"${super.suiteTags}, Hash"

  override def persistentModel: String = SqlOptionModelHash
}
