package com.redislabs.provider.redis.df.cluster

import java.nio.charset.StandardCharsets.UTF_8

import com.redislabs.provider.redis.df.BinaryDataframeSuite
import com.redislabs.provider.redis.env.RedisClusterEnv
import org.apache.spark.sql.redis.RedisSourceRelation.dataKey
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
  * @author The Viet Nguyen
  */
class BinaryDataframeClusterSuite extends BinaryDataframeSuite with RedisClusterEnv {

  override def saveMap(tableName: String, key: String, value: Map[String, String]): Unit = {
    val host = redisConfig.initialHost
    val hostAndPort = new HostAndPort(host.host, host.port)
    val conn = new JedisCluster(hostAndPort)
    conn.set(dataKey(tableName, key).getBytes(UTF_8), serialize(value))
    conn.close()
  }
}
