package com.redislabs.provider.redis

import redis.clients.jedis.Jedis

class JedisFactory(val connectionParams: RedisConnectionParameters) extends Serializable {
  def create(): Jedis = {
    val client = new Jedis(connectionParams.host, connectionParams.port)
    if (connectionParams.password != null) client.auth(connectionParams.password)
    client
  }
}

class RedisConnectionParameters(val host: String, val port: Int, val password: String) extends Serializable {

}
