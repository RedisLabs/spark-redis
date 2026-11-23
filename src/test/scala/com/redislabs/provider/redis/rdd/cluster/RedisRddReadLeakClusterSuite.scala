package com.redislabs.provider.redis.rdd.cluster

import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.rdd.RedisRddReadLeakSuite

class RedisRddReadLeakClusterSuite extends RedisRddReadLeakSuite with RedisClusterEnv
