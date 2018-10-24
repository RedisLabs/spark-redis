package com.redislabs.provider.redis.df.perf

import com.redislabs.provider.redis.df.ManyValueBenchmarkSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import org.apache.spark.sql

/**
  * @author The Viet Nguyen
  */
class BinaryModelManyValueBenchmarkClusterSuite extends ManyValueBenchmarkSuite
  with RedisClusterEnv {

  override def suiteTags: String = s"${super.suiteTags}, Binary"

  override def persistentModel: String = sql.redis.SqlOptionModelBinary
}
