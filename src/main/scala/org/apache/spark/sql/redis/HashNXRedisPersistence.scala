package org.apache.spark.sql.redis

import redis.clients.jedis.Pipeline

class HashNXRedisPersistence extends HashRedisPersistence {

  override def save(pipeline: Pipeline, key: String, value: Any, ttl: Int): Unit = {
    val javaValue = value.asInstanceOf[Map[String, String]]
    javaValue.keySet.foreach( field => pipeline.hsetnx(key, field, javaValue(field)))
    if (ttl > 0) {
      pipeline.expire(key, ttl)
    }
  }

}
