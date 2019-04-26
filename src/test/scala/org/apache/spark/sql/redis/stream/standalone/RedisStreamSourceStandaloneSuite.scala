package org.apache.spark.sql.redis.stream.standalone

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import org.apache.spark.sql.redis.stream.RedisStreamSourceSuite

class RedisStreamSourceStandaloneSuite extends RedisStreamSourceSuite with RedisStandaloneEnv
