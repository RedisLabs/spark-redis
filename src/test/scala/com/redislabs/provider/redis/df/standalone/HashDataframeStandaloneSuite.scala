package com.redislabs.provider.redis.df.standalone

import com.redislabs.provider.redis.df.HashDataframeSuite
import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.util.ConnectionUtils

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashDataframeStandaloneSuite extends HashDataframeSuite with RedisStandaloneEnv {

  override def saveHash(tableName: String, key: String, value: Map[String, String]): Unit = {
    ConnectionUtils.withConnection(redisConfig.initialHost) { conn =>
      conn.hmset(tableName + ":" + key, value.asJava)
    }
  }
}
