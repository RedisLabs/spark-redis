package org.apache.spark.sql.redis

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.{Pipeline, Response}

/**
  * @author The Viet Nguyen
  */
trait RedisPersistence[T] extends Serializable {

  def save(pipeline: Pipeline, key: Array[Byte], value: T): Unit

  def load(pipeline: Pipeline, key: Array[Byte]): Response[T]

  def encodeRow(value: Row): T

  def decodeRow(value: T, schema: StructType): Row
}

object RedisPersistence {

  private val providers =
    Map(SqlOptionModelBinary -> new BinaryRedisPersistence(),
      SqlOptionModelHash -> new HashRedisPersistence())

  def apply(model: String): RedisPersistence[Any] =
    providers.getOrElse(model, providers.getOrElse(SqlOptionModelHash,
      throw new IllegalStateException("Default persistence mode wasn't set")))
      .asInstanceOf[RedisPersistence[Any]]
}
