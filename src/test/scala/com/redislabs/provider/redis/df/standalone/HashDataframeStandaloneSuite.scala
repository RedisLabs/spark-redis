package com.redislabs.provider.redis.df.standalone

import com.redislabs.provider.redis.df.HashDataframeSuite
import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashDataframeStandaloneSuite extends HashDataframeSuite with RedisStandaloneEnv {

  override def saveMap(tableName: String, key: String, value: Map[String, String]): Unit = {
    val host = redisConfig.initialHost
    withConnection(host.connect()) { conn =>
      conn.hmset(tableName + ":" + key, value.asJava)
    }
  }
}
