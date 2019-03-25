package org.apache.spark.sql.redis.stream.cluster

import com.redislabs.provider.redis.env.RedisClusterEnv
import org.apache.spark.sql.redis.stream.RedisStreamSourceSuite

class RedisStreamSourceClusterSuite extends RedisStreamSourceSuite with RedisClusterEnv
