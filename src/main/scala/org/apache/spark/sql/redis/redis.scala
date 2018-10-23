package org.apache.spark.sql

/**
  * @author The Viet Nguyen
  */
package object redis {

  val RedisFormat = "org.apache.spark.sql.redis"
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
}
