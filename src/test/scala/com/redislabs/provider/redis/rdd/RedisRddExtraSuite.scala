package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import org.scalatest.Matchers
import com.redislabs.provider.redis._

import scala.collection.JavaConverters._

/**
  * More RDD tests
  */
trait RedisRddExtraSuite extends SparkRedisSuite with Keys with Matchers {

  implicit val redisConfig: RedisConfig

  test("toRedisByteLISTs") {
    val list1 = Seq("a1", "b1", "c1")
    val list2 = Seq("a2", "b2", "c2")
    val keyValues = Seq(
      ("binary-list1", list1),
      ("binary-list2", list2)
    )
    val keyValueBytes = keyValues.map { case (k, list) => (k.getBytes, list.map(_.getBytes())) }
    val rdd = sc.parallelize(keyValueBytes)
    sc.toRedisByteLISTs(rdd)

    verifyList("binary-list1", list1)
    verifyList("binary-list2", list2)
  }

  test("toRedisLISTs") {
    val list1 = Seq("a1", "b1", "c1")
    val list2 = Seq("a2", "b2", "c2")
    val keyValues = Seq(
      ("list1", list1),
      ("list2", list2)
    )
    val rdd = sc.parallelize(keyValues)
    sc.toRedisLISTs(rdd)

    verifyList("list1", list1)
    verifyList("list2", list2)
  }

  test("toRedisHASHes") {
    val map1 = Map("k1" -> "v1", "k2" -> "v2")
    val map2 = Map("k3" -> "v3", "k4" -> "v4")
    val hashes = Seq(
      ("hash1", map1),
      ("hash2", map2)
    )
    val rdd = sc.parallelize(hashes)
    sc.toRedisHASHes(rdd)

    verifyHash("hash1", map1)
    verifyHash("hash2", map2)
  }

  test("toRedisByteHASHes") {
    val map1 = Map("k1" -> "v1", "k2" -> "v2")
    val map2 = Map("k3" -> "v3", "k4" -> "v4")
    val hashes = Seq(
      ("hash1", map1),
      ("hash2", map2)
    )
    val hashesBytes = hashes.map { case (k, hash) => (k.getBytes, hash.map { case (mapKey, mapVal) => (mapKey.getBytes, mapVal.getBytes) }) }
    val rdd = sc.parallelize(hashesBytes)
    sc.toRedisByteHASHes(rdd)

    verifyHash("hash1", map1)
    verifyHash("hash2", map2)
  }

  def verifyList(list: String, vals: Seq[String]): Unit = {
    withConnection(redisConfig.getHost(list).endpoint.connect()) { conn =>
      conn.lrange(list, 0, vals.size).asScala should be(vals.toList)
    }
  }

  def verifyHash(hash: String, vals: Map[String, String]): Unit = {
    withConnection(redisConfig.getHost(hash).endpoint.connect()) { conn =>
      conn.hgetAll(hash).asScala should be(vals)
    }
  }

}
