package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import org.scalatest.Matchers
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.util.TestUtils
import redis.clients.jedis.exceptions.JedisConnectionException

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

  test("toRedisByteKV") {
    val binaryKeyValue1 = ("binary-key1".getBytes, "binary-value1".getBytes)
    val binaryKeyValue2 = ("binary-key2".getBytes, "binary-value2".getBytes)
    val keyValueBytes = Seq(binaryKeyValue1, binaryKeyValue2)
    val rdd = sc.parallelize(keyValueBytes)
    sc.toRedisByteKV(rdd)

    verifyBytes(binaryKeyValue1._1, binaryKeyValue1._2)
    verifyBytes(binaryKeyValue2._1, binaryKeyValue2._2)
  }

  test("toRedisByteKV with ttl") {
    val key = "binary-key-ttl".getBytes
    val value = "binary-value-ttl".getBytes
    val ttl = 1000
    sc.toRedisByteKV(sc.parallelize(Seq((key, value))), ttl)

    verifyBytes(key, value)
    withConnection(redisConfig.getHost(key).endpoint.connect()) { conn =>
      val actualTtl: Long = conn.ttl(key)
      actualTtl should be > 0L
      actualTtl should be <= ttl.toLong
    }
  }

  // grouping size 2 splits a 5-element partition into batches of 2, 2 and 1;
  // pipeline size 1 forces a sync after every command
  private val smallBatchConfig = ReadWriteConfig(ReadWriteConfig.ScanCountDefault, 1, 2)

  test("toRedisKV writes every key when a partition spans several batches") {
    val prefix = TestUtils.generateRandomKey()
    val kvs = (1 to 5).map(i => (s"$prefix:kv$i", s"value$i"))
    sc.toRedisKV(sc.parallelize(kvs, 1))(redisConfig, smallBatchConfig)

    kvs.foreach { case (key, value) => verifyKV(key, value) }
  }

  test("toRedisKV with ttl sets an expiry on every key") {
    val prefix = TestUtils.generateRandomKey()
    val kvs = (1 to 5).map(i => (s"$prefix:ttl$i", s"value$i"))
    val ttl = 100
    sc.toRedisKV(sc.parallelize(kvs, 1), ttl)(redisConfig, smallBatchConfig)

    kvs.foreach { case (key, value) =>
      verifyKV(key, value)
      withConnection(redisConfig.getHost(key).endpoint.connect()) { conn =>
        val remaining: Long = conn.ttl(key)
        remaining should be > 0L
        remaining should be <= ttl.toLong
      }
    }
  }

  test("connection fails with incorrect user/pass") {
    assertThrows[JedisConnectionException] {
      new RedisConfig(RedisEndpoint(
        host = redisHost,
        port = redisPort,
        user = user,
        auth = "wrong_password"))
    }
  }

  test("connection with correct user/pass") {
    val userConfig = new RedisConfig(RedisEndpoint(
      host = redisHost,
      port = redisPort,
      user = user,
      auth = userPassword))

    val someKey = TestUtils.generateRandomKey()
    val jedis = userConfig.connectionForKey(someKey)
    jedis.set(someKey, "123")
    jedis.get(someKey) should be("123")

    // test RDD operation
    sc.fromRedisKeyPattern(someKey)(redisConfig = userConfig)
      .collect()(0) should be(someKey)
  }

  def verifyList(list: String, vals: Seq[String]): Unit = {
    withConnection(redisConfig.getHost(list).endpoint.connect()) { conn =>
      conn.lrange(list, 0, vals.size).asScala should be(vals.toList)
    }
  }

  def verifyKV(key: String, value: String): Unit = {
    withConnection(redisConfig.getHost(key).endpoint.connect()) { conn =>
      conn.get(key) should be(value)
    }
  }

  def verifyHash(hash: String, vals: Map[String, String]): Unit = {
    withConnection(redisConfig.getHost(hash).endpoint.connect()) { conn =>
      conn.hgetAll(hash).asScala should be(vals)
    }
  }

  def verifyBytes(key: Array[Byte], value: Array[Byte]): Unit = {
    withConnection(redisConfig.getHost(key).endpoint.connect()) { conn =>
      conn.get(key) should be(value)
    }
  }
}
