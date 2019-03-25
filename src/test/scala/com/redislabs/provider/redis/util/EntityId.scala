package com.redislabs.provider.redis.util

import org.apache.spark.sql.types._

/**
  * @author The Viet Nguyen
  */
case class EntityId(_id: String, name: String)

object EntityId {

  val schema = StructType(Array(
    StructField("_id", StringType),
    StructField("name", StringType)
  ))
}
