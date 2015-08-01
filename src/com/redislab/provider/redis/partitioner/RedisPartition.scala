package com.redislab.provider.redis.partitioner

import java.net.InetAddress
import java.util
import org.apache.spark.Partition

case class RedisPartition(index: Int,
                          addr: (InetAddress, Int),
                          slots: util.HashSet[Int]) extends Partition