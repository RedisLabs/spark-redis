package com.redislabs.provider.redis.df.perf

import com.redislabs.provider.redis.df.ManyValueBenchmarkSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import org.apache.spark.sql.redis.SqlOptionModelHash

/**
  * @author The Viet Nguyen
  */
class HashModelManyValueBenchmarkClusterSuite extends ManyValueBenchmarkSuite
  with RedisClusterEnv {

  override def suiteTags: String = s"${super.suiteTags}, Hash"

  override def persistentModel: String = SqlOptionModelHash
}
