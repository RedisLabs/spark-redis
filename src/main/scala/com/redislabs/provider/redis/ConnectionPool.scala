package com.redislabs.provider.redis

import com.redislabs.provider.redis.util.Logging
import org.apache.commons.pool2.PooledObject
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisDataException}
import redis.clients.jedis.{Jedis, JedisFactory, JedisPool, JedisPoolConfig, Protocol}

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap


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

  /**
    * Returns the pool for this endpoint, creating it on first use.
    *
    * `computeIfAbsent` runs `createPool` at most once per endpoint, even when several tasks in the
    * same executor reach a new endpoint at once. Scala's `getOrElseUpdate` on a wrapped map does
    * not: it runs the factory in every racing caller and keeps one result, and the pools it drops
    * are never closed (issue #90). The `get` first keeps the common case lock-free, because on
    * Java 8 `computeIfAbsent` locks the bin even when the key is present.
    */
  private[redis] def poolFor(re: RedisEndpoint): JedisPool = {
    val pool = pools.get(re)
    if (pool != null) pool else pools.computeIfAbsent(re, (endpoint: RedisEndpoint) => createPool(endpoint))
  }

  private def createPool(re: RedisEndpoint): JedisPool = {
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(250)
    poolConfig.setMaxIdle(32)
    poolConfig.setTestOnBorrow(false)
    poolConfig.setTestOnReturn(false)
    poolConfig.setTestWhileIdle(false)
    poolConfig.setSoftMinEvictableIdleTime(Duration.ofMinutes(1))
    poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30))
    poolConfig.setNumTestsPerEvictionRun(-1)

    val factory = new SparkRedisJedisFactory(
      re.host, re.port, re.timeout, re.timeout, re.user, re.auth, re.dbNum, re.ssl
    )
    new JedisPool(poolConfig, factory)
  }

  def connect(re: RedisEndpoint): Jedis = {
    val pool = poolFor(re)
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

