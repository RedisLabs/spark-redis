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
  val SqlOptionModelBlock = "block"
  val SqlOptionInferSchema = "infer.schema"
  val SqlOptionKeyColumn = "key.column"
  val SqlOptionTTL = "ttl"
  val SqlOptionLogInfoVerbose = "log.info.verbose"

  val SqlOptionMaxPipelineSize = "max.pipeline.size"
  val SqlOptionScanCount = "scan.count"

  val SqlOptionIteratorGroupingSize = "iterator.grouping.size"
  val SqlOptionIteratorGroupingSizeDefault = 1000

  val SqlOptionBlockSize = "model.block.size"
  val SqlOptionBlockSizeDefault = 1000
}
