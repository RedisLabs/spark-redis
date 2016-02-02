package com.redislabs.provider.redis

import redis.clients.jedis.Jedis

class JedisFactory() extends Serializable {
  def create(connectionParams: RedisConnectionParameters): Jedis = {
    val client = new Jedis(connectionParams.host, connectionParams.port)
    if (connectionParams.password != null) client.auth(connectionParams.password)
    client
  }
}

object JedisFactory extends Serializable {
  var instance = new JedisFactory()
}

class RedisConnectionParameters(val host: String, val port: Int = 6379, val password: String = null) extends Serializable {

}
