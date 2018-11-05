package com.redislabs.provider.redis.util

import redis.clients.jedis.Jedis

/**
  * @author The Viet Nguyen
  */
object ConnectionUtils {

  def withConnection[A](conn: Jedis)(body: Jedis => A): A = {
    val res = body(conn)
    conn.close()
    res
  }
}
