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

  /**
    * Encode dataframe row before storing it in Redis.
    *
    * @param keyName field name that should be encoded in special way, e.g. in Redis keys.
    * @param value   row to encode.
    * @return encoded row
    */
  def encodeRow(keyName: String, value: Row): T

  /**
    * Decode dataframe row stored in Redis.
    *
    * @param keyMap          extracted name/value of key column from Redis key
    * @param value           encoded row
    * @param schema          row schema
    * @param requiredColumns required columns to decode
    * @return decoded row
    */
  def decodeRow(keyMap: (String, String), value: T, schema: StructType,
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
