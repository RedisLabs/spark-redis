package com.redislabs.provider.redis.df.benchmark.cluster

import com.redislabs.provider.redis.df.benchmark.SingleValueBenchmarkSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import org.apache.spark.sql

/**
  * @author The Viet Nguyen
  */
class BinaryModelSingleValueClusterBenchmarkSuite extends SingleValueBenchmarkSuite
  with RedisClusterEnv {

  override def suiteTags: String = s"${super.suiteTags}, Binary"

  override def persistentModel: String = sql.redis.SqlOptionModelBinary
}
