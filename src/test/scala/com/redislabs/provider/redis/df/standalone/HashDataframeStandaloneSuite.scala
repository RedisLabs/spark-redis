package com.redislabs.provider.redis.df.standalone

import com.redislabs.provider.redis.df.HashDataframeSuite
import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashDataframeStandaloneSuite extends HashDataframeSuite with RedisStandaloneEnv {

  override def saveMap(tableName: String, key: String, value: Map[String, Array[Byte]]): Unit = {
    val host = redisConfig.initialHost
    withConnection(host.connect()) { conn =>
      conn.hmset(
        (tableName + ":" + key).getBytes(StandardCharsets.UTF_8),
        value.map { elem => elem._1.getBytes(StandardCharsets.UTF_8) -> elem._2 }.asJava
      )
    }
  }
}
