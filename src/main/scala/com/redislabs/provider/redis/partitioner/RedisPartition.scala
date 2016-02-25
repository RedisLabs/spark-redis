package com.redislabs.provider.redis.partitioner

import com.redislabs.provider.redis.ClusterInfo
import org.apache.spark.Partition


case class RedisPartition(index: Int,
                          clusterInfo: ClusterInfo,
                          slots: (Int, Int)) extends Partition
