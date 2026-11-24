package com.redislabs.provider.redis

import com.redislabs.provider.redis.env.Env
import com.redislabs.provider.redis.util.MonitoredCommands
import org.scalatest.{FunSuite, Matchers}

/** A [[RedisConfig]] reads the server's topology once, with the commands the configuration docs list. */
trait RedisConfigTopologySuite extends FunSuite with Matchers with Env with MonitoredCommands {

  /** The topology commands a new config sends, each once. */
  protected def topologyCommands: Seq[Seq[String]]

  test("a new config reads the topology once") {
    val commands = monitored(new RedisConfig(redisConfig.initialHost))
    commands.filter(c => c.name == "INFO" || c.name == "CLUSTER").map(_.args.map(_.toUpperCase)) should
      contain theSameElementsAs topologyCommands
  }

  test("a random node is always one of the primaries") {
    val primaries = redisConfig.hosts.toSet
    (1 to 200).map(_ => redisConfig.getRandomNode()).foreach(node => primaries should contain(node))
  }
}
