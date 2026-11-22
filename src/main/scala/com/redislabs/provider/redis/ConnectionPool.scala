package com.redislabs.provider.redis

import com.redislabs.provider.redis.util.Logging
import org.apache.commons.pool2.PooledObject
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisDataException}
import redis.clients.jedis.{Jedis, JedisFactory, JedisPool, JedisPoolConfig, Protocol}

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._


object ConnectionPool extends Logging {
  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()

  /**
    * Custom JedisFactory that sends CLIENT SETINFO commands when a new connection is created.
    * This ensures CLIENT SETINFO is sent only once per connection (at creation time),
    * not every time a connection is borrowed from the pool.
    */
  private class SparkRedisJedisFactory(
      host: String,
      port: Int,
      connectionTimeout: Int,
      soTimeout: Int,
      user: String,
      password: String,
      database: Int,
      ssl: Boolean
  ) extends JedisFactory(
      host, port, connectionTimeout, soTimeout, user, password, database, null, ssl, null, null, null
  ) {

    override def makeObject(): PooledObject[Jedis] = {
      val pooledObject = super.makeObject()
      try {
        val jedis = pooledObject.getObject
        jedis.sendCommand(Protocol.Command.CLIENT, "SETINFO", "LIB-NAME", RedisClientLibraryInfo.libName)
        jedis.sendCommand(Protocol.Command.CLIENT, "SETINFO", "LIB-VER", RedisClientLibraryInfo.libVersion)
      } catch {
        // An error reply means the server refused the command (pre-7.2, or CLIENT|SETINFO denied)
        case e: JedisDataException =>
          logDebug(s"CLIENT SETINFO not supported (requires Redis 7.2+): ${e.getMessage}")
      }
      pooledObject
    }
  }

  def connect(re: RedisEndpoint): Jedis = {

    val pool = pools.asScala.getOrElseUpdate(re,
      {
        val poolConfig: JedisPoolConfig = new JedisPoolConfig()
        poolConfig.setMaxTotal(250)
        poolConfig.setMaxIdle(32)
        poolConfig.setTestOnBorrow(false)
        poolConfig.setTestOnReturn(false)
        poolConfig.setTestWhileIdle(false)
        poolConfig.setSoftMinEvictableIdleDuration(Duration.ofMinutes(1))
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30))
        poolConfig.setNumTestsPerEvictionRun(-1)

        val factory = new SparkRedisJedisFactory(
          re.host, re.port, re.timeout, re.timeout, re.user, re.auth, re.dbNum, re.ssl
        )
        new JedisPool(poolConfig, factory)
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
          contains("ERR max number of clients reached") =>
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        case e: Exception => throw e
      }
    }
    conn
  }
}

