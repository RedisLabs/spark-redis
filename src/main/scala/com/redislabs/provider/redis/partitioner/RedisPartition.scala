package com.redislabs.provider.redis.partitioner

import com.redislabs.provider.redis.RedisConfig
import org.apache.spark.Partition


case class RedisPartition(index: Int,
                          redisConfig: RedisConfig,
                          slots: (Int, Int)) extends Partition
