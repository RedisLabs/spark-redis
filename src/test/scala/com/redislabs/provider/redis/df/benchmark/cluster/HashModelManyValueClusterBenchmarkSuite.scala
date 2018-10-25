package com.redislabs.provider.redis.df.benchmark.cluster

import com.redislabs.provider.redis.df.benchmark.ManyValueBenchmarkSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.BenchmarkTest
import org.apache.spark.sql.redis.SqlOptionModelHash

/**
  * @author The Viet Nguyen
  */
@BenchmarkTest
class HashModelManyValueClusterBenchmarkSuite extends ManyValueBenchmarkSuite
  with RedisClusterEnv {

  override def suiteTags: String = s"${super.suiteTags}, Hash"

  override def persistentModel: String = SqlOptionModelHash
}
