package com.redislabs.provider.redis.df.standalone

import com.redislabs.provider.redis.df.BinaryTtlWriteSuite
import com.redislabs.provider.redis.env.RedisStandaloneEnv

class BinaryTtlWriteStandaloneSuite extends BinaryTtlWriteSuite with RedisStandaloneEnv {
  override protected def clusterMode: Boolean = false
}
