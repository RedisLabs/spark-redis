package org.apache.spark.sql.redis.stream

import org.apache.spark.sql
import org.apache.spark.sql.redis.{StreamOptionConsumerPrefix, StreamOptionGroupName, StreamOptionStreamKeys}

/**
  * @author The Viet Nguyen
  */
case class RedisSourceConfig(consumerConfigs: Seq[RedisConsumerConfig])

object RedisSourceConfig {

  def fromMap(config: Map[String, String]): RedisSourceConfig = {
    val groupName = config.getOrElse(StreamOptionGroupName, "group55")
    val consumerPrefix = config.getOrElse(StreamOptionConsumerPrefix, "consumer")
    val parallelism = config.get(sql.redis.StreamOptionParallelism).map(_.toInt).getOrElse(1)
    val streamKeys = config.getOrElse(StreamOptionStreamKeys,
      throw new IllegalArgumentException(s"Please specify '$StreamOptionStreamKeys'"))
    val consumerConfigs = streamKeys.split(",").flatMap { streamKey =>
      (1 to parallelism).map { consumerIndex =>
        RedisConsumerConfig(streamKey, s"$groupName", s"$consumerPrefix-$consumerIndex", 100, 500)
      }
    }
    RedisSourceConfig(consumerConfigs)
  }
}

case class RedisConsumerConfig(streamKey: String, groupName: String, consumerName: String,
                               batchSize: Int, block: Int)
