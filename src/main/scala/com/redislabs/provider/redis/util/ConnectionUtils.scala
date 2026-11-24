package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.RedisConfig
import redis.clients.jedis.Jedis

/**
  * @author The Viet Nguyen
  */
object ConnectionUtils {

  def withConnection[A](conn: Jedis)(body: Jedis => A): A = {
    try {
      body(conn)
    } finally {
      conn.close()
    }
  }

  def withConnection[A](streamKey: String)(body: Jedis => A)(implicit redisConfig: RedisConfig): A = {
    withConnection(redisConfig.connectionForKey(streamKey)){
      body
    }
  }
}
