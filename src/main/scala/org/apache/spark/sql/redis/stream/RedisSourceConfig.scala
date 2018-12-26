package org.apache.spark.sql.redis.stream

import org.apache.spark.sql.redis.StreamOptionStreamKeys

/**
  * @author The Viet Nguyen
  */
case class RedisSourceConfig(consumerConfigs: Seq[RedisConsumerConfig])

object RedisSourceConfig {

  def fromMap(config: Map[String, String]): RedisSourceConfig = {
    val streamKey = config.getOrElse(StreamOptionStreamKeys,
      throw new IllegalArgumentException(s"Please specify '$StreamOptionStreamKeys'"))
    val consumerConfigs = Seq(RedisConsumerConfig(streamKey, "group55", "consumer-123", 100, 500))
    RedisSourceConfig(consumerConfigs)
  }
}

case class RedisConsumerConfig(streamKey: String, groupName: String, consumerName: String,
                               batchSize: Int, block: Int)
