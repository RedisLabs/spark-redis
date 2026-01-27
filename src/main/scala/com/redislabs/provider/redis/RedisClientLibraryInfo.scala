package com.redislabs.provider.redis

import redis.clients.jedis.Jedis

/**
  * Provides library identification information for Redis CLIENT SETINFO command.
  * 
  * This allows Redis server to identify the client library and version for
  * monitoring and debugging purposes.
  */
object RedisClientLibraryInfo {

  private val DownstreamDriverName = "jedis"
  private val UpstreamDriverName = "spark-redis"

  /**
    * spark-redis version extracted from the package manifest.
    * Falls back to "unknown" if not available (e.g., running from IDE).
    */
  val sparkRedisVersion: String = {
    val pkg = RedisClientLibraryInfo.getClass.getPackage
    val ver = if (pkg != null) pkg.getImplementationVersion else null
    if (ver != null) ver else "unknown"
  }

  /**
    * Jedis version extracted from the Jedis package manifest.
    * Falls back to "unknown" if not available.
    */
  val jedisVersion: String = {
    val pkg = classOf[Jedis].getPackage
    val ver = if (pkg != null) pkg.getImplementationVersion else null
    if (ver != null) ver else "unknown"
  }

  /**
    * Library name in format: jedis(spark-redis_v{spark-redis-version})
    * Example: jedis(spark-redis_v3.1.0-SNAPSHOT)
    */
  val libName: String = s"$DownstreamDriverName(${UpstreamDriverName}_v$sparkRedisVersion)"

  /**
    * Library version (Jedis version).
    * Example: 3.9.0
    */
  val libVersion: String = jedisVersion
}

