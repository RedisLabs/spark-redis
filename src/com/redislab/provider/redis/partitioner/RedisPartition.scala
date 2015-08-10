package com.redislab.provider.redis.partitioner

import java.net.InetAddress
import java.util
import org.apache.spark.Partition

case class RedisPartition(index: Int,
                          node: (InetAddress, Int, Int, Int)) extends Partition