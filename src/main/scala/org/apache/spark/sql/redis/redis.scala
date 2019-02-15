package org.apache.spark.sql

/**
  * @author The Viet Nguyen
  */
package object redis {

  val RedisFormat = "org.apache.spark.sql.redis"

  val SqlOptionFilterKeysByType = "filter.keys.by.type"
  val SqlOptionNumPartitions = "partitions.number"
  /**
    * Default read operation number of partitions.
    */
  val SqlOptionNumPartitionsDefault = 3
  val SqlOptionTableName = "table"
  val SqlOptionKeysPattern = "keys.pattern"
  val SqlOptionModel = "model"
  val SqlOptionModelBinary = "binary"
  val SqlOptionModelHash = "hash"
  val SqlOptionInferSchema = "infer.schema"
  val SqlOptionKeyColumn = "key.column"
  val SqlOptionTTL = "ttl"

  val SqlOptionMaxPipelineSize = "max.pipeline.size"
  val SqlOptionScanCount = "scan.count"

  val SqlOptionIteratorGroupingSize = "iterator.grouping.size"
  val SqlOptionIteratorGroupingSizeDefault = 1000

  val StreamOptionStreamKeys = "stream.keys"
  val StreamOptionStreamOffsets = "stream.offsets"
  val StreamOptionReadBatchSize = "stream.read.batch.size"
  val StreamOptionReadBatchSizeDefault = 100
  val StreamOptionReadBlock = "stream.read.block"
  val StreamOptionReadBlockDefault = 500
  val StreamOptionParallelism = "stream.parallelism"
  val StreamOptionGroupName = "stream.group.name"
  val StreamOptionConsumerPrefix = "stream.consumer.prefix"
}
