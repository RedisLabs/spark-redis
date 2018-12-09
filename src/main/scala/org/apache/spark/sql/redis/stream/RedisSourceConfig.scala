package org.apache.spark.sql.redis.stream

import org.apache.spark.sql.redis.StreamOptionStreamKey

/**
  * @author The Viet Nguyen
  */
case class RedisSourceConfig(consumerConfigs: Seq[RedisConsumerConfig], batchSize: Int, block: Int)

object RedisSourceConfig {

  def fromMap(config: Map[String, String]): RedisSourceConfig = {
    val streamKey = config.getOrElse(StreamOptionStreamKey,
      throw new IllegalArgumentException("Please specify 'stream.key'"))
    val consumerConfigs = Seq(RedisConsumerConfig(streamKey, "group55", "consumer-123", 100, 500))
    RedisSourceConfig(consumerConfigs, 100, 500)
  }
}

case class RedisConsumerConfig(streamKey: String, groupName: String, consumerName: String,
                               batchSize: Int, block: Int)
