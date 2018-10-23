package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.RedisEndpoint
import redis.clients.jedis.Jedis

/**
  * @author The Viet Nguyen
  */
object ConnectionUtils {

  def withConnection[A](endpoint: RedisEndpoint)(body: Jedis => A): A = {
    val conn = endpoint.connect()
    val res = body(conn)
    conn.close()
    res
  }
}
