package com.redislabs.provider.redis.cluster

import com.redislabs.provider.redis.RedisConfigTopologySuite
import com.redislabs.provider.redis.env.RedisClusterEnv

class RedisConfigTopologyClusterSuite extends RedisConfigTopologySuite with RedisClusterEnv {
  override protected def topologyCommands: Seq[Seq[String]] = Seq(Seq("INFO"), Seq("CLUSTER", "SLOTS"))
}
