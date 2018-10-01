package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets.UTF_8

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.Pipeline

/**
  * @author The Viet Nguyen
  */
class BinaryRedisPersistence extends RedisPersistence[Array[Byte]] {

  override def save(pipeline: Pipeline, key: String, value: Array[Byte]): Unit =
    pipeline.set(key.getBytes(UTF_8), value)

  override def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit =
    pipeline.get(key.getBytes(UTF_8))

  override def encodeRow(value: Row): Array[Byte] =
    SerializationUtils.serialize(value)

  override def decodeRow(value: Array[Byte], schema: => StructType, inferSchema: Boolean): Row =
    SerializationUtils.deserialize(value)
}
