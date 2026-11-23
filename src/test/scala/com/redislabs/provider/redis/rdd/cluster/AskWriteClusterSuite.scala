package com.redislabs.provider.redis.rdd.cluster

import java.util.UUID

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.ClusterSlots
import com.redislabs.provider.redis.util.ClusterSlots.withNode
import org.scalatest.Matchers
import redis.clients.jedis.util.JedisClusterCRC16

/**
  * The connector writes keys in a slot that is being migrated to another primary. The old owner still owns the
  * slot, but it answers `ASK` for keys it doesn't hold, and every key must still be stored. The companion of
  * [[MovedWriteClusterSuite]], which covers `MOVED` after a migration ends.
  *
  * The slot is put back in a `finally` block, so the suites that run next in this JVM see the original layout.
  */
class AskWriteClusterSuite extends SparkRedisSuite with RedisClusterEnv with Matchers {

  private val HashTag = "{ask-write}"
  private val Slot = JedisClusterCRC16.getSlot(HashTag)
  private val KeyCount = 100

  test("toRedisKV stores every key while its slot is being migrated to another primary") {
    val all = ClusterSlots.primaries(redisHost, redisPort)
    val source = all.find(_.owns(Slot)).getOrElse(fail(s"no primary owns slot $Slot"))
    val target = all.find(_.id != source.id).getOrElse(fail("the cluster has only one primary"))

    val run = UUID.randomUUID().toString.replace("-", "")
    val kvs = (1 to KeyCount).map(i => (s"$HashTag:$run:key:$i", s"value-$i"))

    val config = new RedisConfig(RedisEndpoint(redisHost, redisPort))
    config.getHost(kvs.head._1).endpoint.port shouldBe source.port

    try {
      // Mid-migration: `source` still owns the slot, and answers ASK for the new keys.
      ClusterSlots.beginMove(Slot, source, target)
      sc.toRedisKV(sc.parallelize(kvs, 4))(config, ReadWriteConfig.fromSparkConf(sc.getConf))
      ClusterSlots.finishMove(Slot, source, target, all)

      val stored = withNode(target)(conn => kvs.map { case (key, _) => key -> conn.get(key) }.toMap)
      val missing = kvs.count { case (key, value) => stored(key) != value }
      withClue(s"$missing of $KeyCount keys not stored while slot $Slot was migrating:") {
        stored shouldBe kvs.toMap
      }
    } finally {
      ClusterSlots.moveSlot(Slot, source, target, all) // finishes the move if it stopped part way
      ClusterSlots.moveSlot(Slot, target, source, all)
      withNode(source)(conn => conn.del(kvs.map(_._1): _*))
    }
  }
}
