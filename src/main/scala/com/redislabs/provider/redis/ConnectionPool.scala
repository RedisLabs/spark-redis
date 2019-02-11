package com.redislabs.provider.redis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import redis.clients.jedis.exceptions.JedisConnectionException
import java.util.concurrent.ConcurrentHashMap

import com.redislabs.provider.redis.util.Logging

import scala.collection.JavaConversions._


object ConnectionPool extends Logging {
  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()

  def connect(re: RedisEndpoint): Jedis = {
    val pool = pools.getOrElseUpdate(re,
      {
        val poolConfig: JedisPoolConfig = new JedisPoolConfig()
        poolConfig.setMaxTotal(250)
        poolConfig.setMaxIdle(32)
        poolConfig.setTestOnBorrow(false)
        poolConfig.setTestOnReturn(false)
        poolConfig.setTestWhileIdle(false)
        poolConfig.setMinEvictableIdleTimeMillis(60000)
        poolConfig.setTimeBetweenEvictionRunsMillis(30000)
        poolConfig.setNumTestsPerEvictionRun(-1)
        new JedisPool(poolConfig, re.host, re.port, re.timeout, re.auth, re.dbNum)
      }
    )
    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pool.getResource
      }
      catch {
        case e: JedisConnectionException if e.getCause.toString.contains("ERR max number of clients reached") =>
          logWarn("Max number of clients reached. Will wait and try again.")
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        case e: Exception => throw e
      }
    }
    conn
  }
}

