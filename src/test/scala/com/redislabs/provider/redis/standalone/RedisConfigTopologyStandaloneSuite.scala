package com.redislabs.provider.redis.standalone

import com.redislabs.provider.redis.RedisConfigTopologySuite
import com.redislabs.provider.redis.env.RedisStandaloneEnv

class RedisConfigTopologyStandaloneSuite extends RedisConfigTopologySuite with RedisStandaloneEnv {
  override protected def topologyCommands: Seq[Seq[String]] = Seq(Seq("INFO"), Seq("INFO", "REPLICATION"))
}
