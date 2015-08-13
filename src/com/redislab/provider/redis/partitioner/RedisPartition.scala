package com.redislab.provider.redis.partitioner

import java.net.InetAddress
import java.util
import org.apache.spark.Partition
import com.redislab.provider._

case class RedisPartition(index: Int,
                          redisConfig: RedisConfig,
                          slots: (Int, Int)) extends Partition