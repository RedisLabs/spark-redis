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
    * Empty if not available (e.g., running from IDE).
    */
  val sparkRedisVersion: String = {
    val pkg = RedisClientLibraryInfo.getClass.getPackage
    val ver = if (pkg != null) pkg.getImplementationVersion else null
    if (ver != null) ver else ""
  }

  /**
    * Jedis version extracted from the Jedis package manifest.
    * Empty if not available.
    */
  val jedisVersion: String = {
    val pkg = classOf[Jedis].getPackage
    val ver = if (pkg != null) pkg.getImplementationVersion else null
    if (ver != null) ver else ""
  }

  /**
    * Library name in format: jedis(spark-redis_v{spark-redis-version}),
    * or jedis(spark-redis) if the version is not available.
    * Example: jedis(spark-redis_v3.1.0-SNAPSHOT)
    */
  val libName: String = {
    val versionSuffix = if (sparkRedisVersion.isEmpty) "" else s"_v$sparkRedisVersion"
    s"$DownstreamDriverName($UpstreamDriverName$versionSuffix)"
  }

  /**
    * Library version (Jedis version). Empty if not available.
    * Example: 3.9.0
    */
  val libVersion: String = jedisVersion
}
