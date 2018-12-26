package org.apache.spark.sql.redis.stream

import org.apache.spark.sql
import org.apache.spark.sql.redis.StreamOptionStreamKeys

/**
  * @author The Viet Nguyen
  */
case class RedisSourceConfig(consumerConfigs: Seq[RedisConsumerConfig])

object RedisSourceConfig {

  def fromMap(config: Map[String, String]): RedisSourceConfig = {
    val parallelism = config.get(sql.redis.StreamOptionParallelism).map(_.toInt).getOrElse(1)
    val streamKeys = config.getOrElse(StreamOptionStreamKeys,
      throw new IllegalArgumentException(s"Please specify '$StreamOptionStreamKeys'"))
    val consumerConfigs = streamKeys.split(",").flatMap { streamKey =>
      (1 to parallelism).map { consumerIndex =>
        RedisConsumerConfig(streamKey, "group55", s"consumer-$consumerIndex", 100, 500)
      }
    }
    RedisSourceConfig(consumerConfigs)
  }
}

case class RedisConsumerConfig(streamKey: String, groupName: String, consumerName: String,
                               batchSize: Int, block: Int)
