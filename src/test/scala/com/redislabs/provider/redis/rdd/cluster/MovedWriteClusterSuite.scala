package com.redislabs.provider.redis.rdd.cluster

import java.util.UUID

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.ClusterSlots
import com.redislabs.provider.redis.util.ClusterSlots.{moveSlot, withNode}
import org.scalatest.Matchers
import redis.clients.jedis.util.JedisClusterCRC16

/**
  * A slot moves to another primary after a `RedisConfig` has read the cluster topology, and the connector then
  * writes keys in that slot with the same `RedisConfig`. The cluster answers the old owner with `MOVED`, and every
  * key must still be stored (the #347 family: writes lost without an error).
  *
  * The test moves the slot with [[ClusterSlots]], the way `redis-cli --cluster reshard` does, and moves it back in
  * a `finally` block, so the suites that run next in this JVM see the original layout.
  */
class MovedWriteClusterSuite extends SparkRedisSuite with RedisClusterEnv with Matchers {

  private val HashTag = "{moved-write}"
  private val Slot = JedisClusterCRC16.getSlot(HashTag)
  private val KeyCount = 100
  private val SeedKeyCount = 5

  test("toRedisKV stores every key after its slot moved to another primary") {
    val all = ClusterSlots.primaries(redisHost, redisPort)
    val source = all.find(_.owns(Slot)).getOrElse(fail(s"no primary owns slot $Slot"))
    val target = all.find(_.id != source.id).getOrElse(fail("the cluster has only one primary"))

    val run = UUID.randomUUID().toString.replace("-", "")
    val seedKeys = (1 to SeedKeyCount).map(i => s"$HashTag:$run:seed:$i")
    val kvs = (1 to KeyCount).map(i => (s"$HashTag:$run:key:$i", s"value-$i"))

    // The connector reads the topology here, while the slot is still on `source`.
    val staleConfig = new RedisConfig(RedisEndpoint(redisHost, redisPort))
    staleConfig.getHost(kvs.head._1).endpoint.port shouldBe source.port

    withNode(source)(conn => seedKeys.foreach(key => conn.set(key, "seed")))
    try {
      moveSlot(Slot, source, target, all)
      withClue("setup: the seed keys moved with the slot:") {
        withNode(target)(conn => seedKeys.map(conn.get)) shouldBe seedKeys.map(_ => "seed")
      }

      sc.toRedisKV(sc.parallelize(kvs, 4))(staleConfig, ReadWriteConfig.fromSparkConf(sc.getConf))

      val stored = withNode(target)(conn => kvs.map { case (key, _) => key -> conn.get(key) }.toMap)
      val missing = kvs.count { case (key, value) => stored(key) != value }
      withClue(s"$missing of $KeyCount keys not stored after slot $Slot moved:") {
        stored shouldBe kvs.toMap
      }
    } finally {
      moveSlot(Slot, source, target, all) // finishes the move if it stopped part way; a no-op if it finished
      moveSlot(Slot, target, source, all)
      withNode(source)(conn => conn.del((seedKeys ++ kvs.map(_._1)): _*))
    }
  }
}
