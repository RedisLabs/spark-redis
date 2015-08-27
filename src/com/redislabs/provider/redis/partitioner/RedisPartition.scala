package com.redislabs.provider.redis.partitioner

import java.net.InetAddress
import java.util
import org.apache.spark.Partition
import com.redislabs.provider._

case class RedisPartition(index: Int,
                          redisConfig: RedisConfig,
                          slots: (Int, Int)) extends Partition