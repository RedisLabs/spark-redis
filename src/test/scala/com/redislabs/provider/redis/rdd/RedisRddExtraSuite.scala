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

  def verifyList(list: String, vals: Seq[String]): Unit = {
    withConnection(redisConfig.getHost(list).endpoint.connect()) { conn =>
      conn.lrange(list, 0, vals.size).asScala should be(vals.toList)
    }
  }

}
