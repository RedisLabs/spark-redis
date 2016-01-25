package com.redislabs.provider.redis

import redis.clients.jedis.Jedis

class JedisFactory (val host_port_password: (String, Int, String)) {
  def create() :Jedis = {
    val client = new Jedis(host_port_password._1, host_port_password._2)
    if(host_port_password._3 != null) client.auth(host_port_password._3)
    client
  }
}
