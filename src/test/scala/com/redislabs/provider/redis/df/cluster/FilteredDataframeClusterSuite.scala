package com.redislabs.provider.redis.df.cluster

import com.redislabs.provider.redis.df.{FilteredDataframeSuite, RedisDataframeSuite}
import com.redislabs.provider.redis.env.RedisClusterEnv

/**
  * @author The Viet Nguyen
  */
class FilteredDataframeClusterSuite extends FilteredDataframeSuite with RedisClusterEnv
