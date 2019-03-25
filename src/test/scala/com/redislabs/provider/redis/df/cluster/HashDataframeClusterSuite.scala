package com.redislabs.provider.redis.df.cluster

import com.redislabs.provider.redis.df.HashDataframeSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashDataframeClusterSuite extends HashDataframeSuite with RedisClusterEnv {

  override def saveMap(tableName: String, key: String, value: Map[String, String]): Unit = {
    val host = redisConfig.initialHost
    val hostAndPort = new HostAndPort(host.host, host.port)
    val conn = new JedisCluster(hostAndPort)
    conn.hmset(tableName + ":" + key, value.asJava)
    conn.close()
  }
}
