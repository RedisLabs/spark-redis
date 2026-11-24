package org.apache.spark.sql

/**
  * @author The Viet Nguyen
  */
package object redis {

  val RedisFormat = "org.apache.spark.sql.redis"

  val RddWriteIteratorGroupingSize = "rdd.write.iterator.grouping.size"
  val RddWriteIteratorGroupingSizeDefault = 1000

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
  /**
    * What a hash-model write does to a hash already stored under the row's key. `merge` (the default) keeps the
    * fields the new row doesn't have. `replace` makes the stored hash exactly the new row. The binary model stores
    * a row as one value, so it always replaces.
    */
  val SqlOptionWriteMode = "write.mode"
  val SqlOptionWriteModeMerge = "merge"
  val SqlOptionWriteModeReplace = "replace"

  /**
    * Classes, besides the ones spark-redis writes itself, that a read may find in a Java-serialized binary-model row.
    * A comma-separated list of class names and patterns, see [[ClassAllowlist]].
    */
  val SqlOptionDeserializationAllowedClasses = "deserialization.allowed.classes"

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
