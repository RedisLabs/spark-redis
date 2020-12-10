package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets.UTF_8

import redis.clients.jedis.Pipeline
import redis.clients.jedis.params.SetParams

class BinaryNXRedisPersistence extends BinaryRedisPersistence {

  override def save(pipeline: Pipeline, key: String, value: Array[Byte], ttl: Int): Unit = {
    val keyBytes = key.getBytes(UTF_8)
    val setParameters = SetParams.setParams()
      .nx()
      .ex(ttl)
    pipeline.set(keyBytes, value, setParameters)
  }

}
