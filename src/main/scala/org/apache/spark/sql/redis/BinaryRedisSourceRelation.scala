package org.apache.spark.sql.redis

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/**
  * @author The Viet Nguyen
  */
class BinaryRedisSourceRelation(override val sqlContext: SQLContext,
                                parameters: Map[String, String],
                                userSpecifiedSchema: Option[StructType])
  extends RedisSourceRelation(sqlContext, parameters, userSpecifiedSchema) {

  override def rowToBytes(value: Row): Array[Byte] =
    SerializationUtils.serialize(value)

  override def bytesToRow(value: Array[Byte]): Row =
    SerializationUtils.deserialize(value)
}
