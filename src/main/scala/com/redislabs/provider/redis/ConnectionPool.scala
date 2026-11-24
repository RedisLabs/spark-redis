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
    *
    * A non-null clientName is set by Jedis itself (CLIENT SETNAME, after AUTH and SELECT) on every new connection.
    * A refused SETNAME fails the connection, as a refused AUTH does: the name was asked for, so it is not optional.
    */
  private class SparkRedisJedisFactory(
      host: String,
      port: Int,
      connectionTimeout: Int,
      soTimeout: Int,
      user: String,
      password: String,
      database: Int,
      clientName: String,
      ssl: Boolean
  ) extends JedisFactory(
      host, port, connectionTimeout, soTimeout, user, password, database, clientName, ssl, null, null, null
  ) {

    override def makeObject(): PooledObject[Jedis] = {
      val pooledObject = super.makeObject()
      val jedis = pooledObject.getObject
      try {
        jedis.sendCommand(Protocol.Command.CLIENT, "SETINFO", "LIB-NAME", RedisClientLibraryInfo.libName)
        jedis.sendCommand(Protocol.Command.CLIENT, "SETINFO", "LIB-VER", RedisClientLibraryInfo.libVersion)
      } catch {
        // Without AUTH, SELECT or a client name, this is the first reply the connection reads, so a server at
        // maxclients answers it with its refusal, and has closed the connection: fail it, so `connect` retries.
        case e: JedisDataException if isMaxClients(e) =>
          jedis.close()
          throw e
        // Any other error reply means the server refused the command (pre-7.2, or CLIENT|SETINFO denied)
        case e: JedisDataException =>
          logDebug(s"CLIENT SETINFO not supported (requires Redis 7.2+): ${e.getMessage}")
      }
      pooledObject
    }
  }

  /** How long [[connect]] keeps retrying a server that refuses new connections at its `maxclients`. */
  private[redis] val MaxClientsRetryMillis = 60000L

  /** The longest wait between two attempts: the waits double from 8 ms up to this. */
  private val MaxClientsRetrySleepMillis = 512L

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
      re.host, re.port, re.timeout, re.timeout, re.user, re.auth, re.dbNum,
      RedisConfig.clientNameOrNull(re.clientName), re.ssl
    )
    new JedisPool(poolConfig, factory)
  }

  /**
    * A connection from the endpoint's pool. A server at its `maxclients` refuses new connections; they are retried,
    * waiting longer each time, for up to [[MaxClientsRetryMillis]].
    *
    * @throws JedisConnectionException if the server still refuses new connections after that, or can't be reached
    */
  def connect(re: RedisEndpoint): Jedis = {
    val pool = poolFor(re)
    retryWhileMaxClients(s"${re.host}:${re.port}", MaxClientsRetryMillis)(pool.getResource)
  }

  /**
    * Runs `attempt` until it doesn't fail because the server is at its `maxclients`, or until `maxWaitMillis` have
    * passed, then throws the last such failure, wrapped in one that says how long it retried.
    */
  private[redis] def retryWhileMaxClients[A](server: String, maxWaitMillis: Long,
                                             sleep: Long => Unit = Thread.sleep,
                                             nanoTime: () => Long = () => System.nanoTime())(attempt: => A): A = {
    val start = nanoTime()
    var sleepMillis = 4L
    var result: Option[A] = None
    while (result.isEmpty) {
      try {
        result = Some(attempt)
      } catch {
        case e: JedisConnectionException if isMaxClients(e) =>
          sleepMillis = math.min(sleepMillis * 2, MaxClientsRetrySleepMillis)
          val waitedMillis = (nanoTime() - start) / 1000000
          if (waitedMillis + sleepMillis > maxWaitMillis) {
            val refusal = maxClientsRefusal(e).getOrElse("")
            throw new JedisConnectionException(s"Redis at $server refused new connections for " +
              s"${waitedMillis / 1000} s: $refusal. Raise the server's maxclients, or run fewer tasks at once.", e)
          }
          sleep(sleepMillis)
      }
    }
    result.get
  }

  /**
    * True if `e`, or an exception that caused it, is the server's refusal of a connection at its `maxclients`:
    * `ERR max number of clients reached`, or on a cluster node `ERR max number of clients + cluster connections
    * reached`.
    */
  private[redis] def isMaxClients(e: Throwable): Boolean = maxClientsRefusal(e).isDefined

  /** The server's refusal at its `maxclients`, if `e` or an exception that caused it is one. */
  private def maxClientsRefusal(e: Throwable): Option[String] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).collectFirst {
      case d: JedisDataException if String.valueOf(d.getMessage).contains("max number of clients") => d.getMessage
    }
}

