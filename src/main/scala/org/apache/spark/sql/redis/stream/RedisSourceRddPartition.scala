package org.apache.spark.sql.redis.stream

import org.apache.spark.Partition

/**
  * @author The Viet Nguyen
  */
case class RedisSourceRddPartition(index: Int) extends Partition
