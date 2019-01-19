package org.apache.spark.sql.redis.stream

import org.apache.spark.sql
import org.apache.spark.sql.redis._

/**
  * @author The Viet Nguyen
  */
case class RedisSourceConfig(consumerConfigs: Seq[RedisConsumerConfig],
                             start: Option[RedisSourceOffset])

object RedisSourceConfig {

  def fromMap(config: Map[String, String]): RedisSourceConfig = {
    val streamKeys = config.getOrElse(StreamOptionStreamKeys,
      throw new IllegalArgumentException(s"Please specify '$StreamOptionStreamKeys'"))
    val start = config.get(StreamOptionStreamOffsets).map(RedisSourceOffset.fromJson)
    val parallelism = config.get(sql.redis.StreamOptionParallelism).map(_.toInt).getOrElse(1)
    val groupName = config.getOrElse(StreamOptionGroupName, "spark-source")
    val consumerPrefix = config.getOrElse(StreamOptionConsumerPrefix, "consumer")
    val batchSize = config.get(StreamOptionReadBatchSize).map(_.toInt).getOrElse(StreamOptionReadBatchSizeDefault)
    val block = config.get(StreamOptionReadBlock).map(_.toInt).getOrElse(StreamOptionReadBlockDefault)
    val consumerConfigs = streamKeys.split(",").flatMap { streamKey =>
      (1 to parallelism).map { consumerIndex =>
        RedisConsumerConfig(streamKey, s"$groupName", s"$consumerPrefix-$consumerIndex", batchSize, block)
      }
    }
    RedisSourceConfig(consumerConfigs, start)
  }
}

case class RedisConsumerConfig(streamKey: String, groupName: String, consumerName: String,
                               batchSize: Int, block: Int)
