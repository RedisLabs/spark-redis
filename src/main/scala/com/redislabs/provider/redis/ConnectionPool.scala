package com.redislabs.provider.redis

import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{ClientSetInfoConfig, DefaultJedisClientConfig, HostAndPort, Jedis, JedisPool,
  JedisPoolConfig}

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._


object ConnectionPool {
  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()

  def connect(re: RedisEndpoint): Jedis = {
    val pool = pools.getOrElseUpdate(re,
      {
        val poolConfig: JedisPoolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(250)
        poolConfig.setMaxIdle(32)
        poolConfig.setTestOnBorrow(false)
        poolConfig.setTestOnReturn(false)
        poolConfig.setTestWhileIdle(false)
        poolConfig.setSoftMinEvictableIdleTime(Duration.ofMinutes(1))
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30))
        poolConfig.setNumTestsPerEvictionRun(-1)

        val clientSetInfo = ClientSetInfoConfig.withLibNameSuffix(RedisClientLibraryInfo.libNameSuffix)
        val clientConfig = DefaultJedisClientConfig.builder()
          .user(re.user)
          .password(re.auth)
          .database(re.dbNum)
          .connectionTimeoutMillis(re.timeout)
          .socketTimeoutMillis(re.timeout)
          .ssl(re.ssl)
          .clientSetInfoConfig(clientSetInfo)
          .build()

        val hostAndPort = new HostAndPort(re.host, re.port)
        new JedisPool(poolConfig, hostAndPort, clientConfig)
      }
    )
    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pool.getResource
      }
      catch {
        case e: JedisConnectionException if e.getCause.toString.
          contains("ERR max number of clients reached") => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }
}

