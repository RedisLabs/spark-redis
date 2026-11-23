package com.redislabs.provider.redis.df.cluster

import com.redislabs.provider.redis.df.BinaryTtlWriteSuite
import com.redislabs.provider.redis.env.RedisClusterEnv

class BinaryTtlWriteClusterSuite extends BinaryTtlWriteSuite with RedisClusterEnv {
  override protected def clusterMode: Boolean = true
}
