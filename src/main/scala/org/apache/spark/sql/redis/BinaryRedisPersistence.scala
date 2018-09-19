package org.apache.spark.sql.redis

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.{Pipeline, Response}

/**
  * @author The Viet Nguyen
  */
class BinaryRedisPersistence extends RedisPersistence[Array[Byte]] {

  override def save(pipeline: Pipeline, key: Array[Byte], value: Array[Byte]): Unit =
    pipeline.set(key, value)

  override def load(pipeline: Pipeline, key: Array[Byte]): Response[Array[Byte]] =
    pipeline.get(key)

  override def encodeRow(value: Row): Array[Byte] =
    SerializationUtils.serialize(value)

  override def decodeRow(value: Array[Byte], schema: StructType): Row =
    SerializationUtils.deserialize(value)
}
