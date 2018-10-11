package org.apache.spark.sql

/**
  * @author The Viet Nguyen
  */
package object redis {

  val RedisFormat = "org.apache.spark.sql.redis"
  val SqlOptionNumPartitions = "numPartitions"
  /**
    * Default read operation number of partitions.
    */
  val SqlOptionNumPartitionsDefault = 3
  val SqlOptionModel = "model"
  val SqlOptionModelBinary = "binary"
  val SqlOptionModelHash = "hash"
  val SqlOptionInferSchema = "inferSchema"
  val SqlOptionKeyColumn = "keyColumn"
  val SqlOptionTTL = "ttl"
}
