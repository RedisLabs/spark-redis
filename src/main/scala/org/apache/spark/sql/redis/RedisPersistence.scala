package org.apache.spark.sql.redis

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.Pipeline

/**
  * @author The Viet Nguyen
  */
trait RedisPersistence[T] extends Serializable {

  def save(pipeline: Pipeline, key: String, value: T, ttl: Int): Unit

  def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit

  def encodeRow(key: (String, String), value: Row): T

  def decodeRow(key: (String, String), value: T, schema: => StructType, inferSchema: Boolean,
                requiredColumns: Seq[String]): Row
}

object RedisPersistence {

  private val providers =
    Map(SqlOptionModelBinary -> new BinaryRedisPersistence(),
      SqlOptionModelHash -> new HashRedisPersistence())

  def apply(model: String): RedisPersistence[Any] = {
    // use hash model by default
    providers.getOrElse(model, providers(SqlOptionModelHash))
      .asInstanceOf[RedisPersistence[Any]]
  }
}
