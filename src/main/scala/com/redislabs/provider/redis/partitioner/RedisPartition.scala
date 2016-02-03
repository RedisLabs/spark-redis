package com.redislabs.provider.redis.partitioner

import org.apache.spark.Partition
import com.redislabs.provider._

case class RedisPartition(index: Int,
                          redisConfig: RedisConfig,
                          slots: (Int, Int)) extends Partition
