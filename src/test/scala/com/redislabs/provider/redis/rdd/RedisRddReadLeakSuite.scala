package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.TestUtils
import com.redislabs.provider.redis.util.TestUtils.interceptSparkErr
import org.apache.spark.SparkException
import org.apache.spark.sql.redis.{RedisFormat, RedisSourceRelation}
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Millis, Span}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException

/**
  * Reads that fail must return their connections to the pool.
  *
  * The RDD reads run as an ACL user that isn't allowed to run the read commands, so Redis rejects them with NOPERM.
  * Each failing read is repeated, and the number of active connections in the pool must not grow.
  */
// scalastyle:off multiple.string.literals
trait RedisRddReadLeakSuite extends SparkRedisSuite with Keys with Matchers {

  implicit val redisConfig: RedisConfig

  private val noReadUser = "spark-redis-no-read"
  private val noInfoUser = "spark-redis-no-info"
  private val testUserPassword = "leak-test-pw"
  private val readCommands = Seq("get", "hgetall", "smembers", "lrange", "zrange", "zrangebyscore", "scan")
  private val repetitions = 3

  // when a task fails, Spark cancels the other tasks of the job, but they keep their connection until their current
  // command returns; a leaked connection never goes back to the pool
  implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    forEachMaster { conn =>
      val common = Seq("reset", "on", s">$testUserPassword", "~*", "+@all")
      conn.aclSetUser(noReadUser, common ++ readCommands.map(c => s"-$c"): _*)
      conn.aclSetUser(noInfoUser, common :+ "-info": _*)
    }
  }

  override def afterAll(): Unit = {
    forEachMaster { conn =>
      conn.aclDelUser(noReadUser)
      conn.aclDelUser(noInfoUser)
    }
    super.afterAll()
  }

  /** ACL users are per node, so they are created on every master of a cluster */
  private def forEachMaster(f: Jedis => Unit): Unit =
    redisConfig.hosts.foreach(node => withConnection(node.endpoint.connect())(f))

  private def endpointFor(user: String): RedisEndpoint =
    redisConfig.initialHost.copy(user = user, auth = testUserPassword)

  private lazy val noReadConfig: RedisConfig = new RedisConfig(endpointFor(noReadUser))

  private def activeConnections(endpoints: Seq[RedisEndpoint]): Int =
    endpoints.map(ConnectionPool.poolFor(_).getNumActive).sum

  private def assertReadFailsWithoutLeak(read: => Any): Unit = {
    val endpoints = noReadConfig.hosts.map(_.endpoint).toSeq
    val before = activeConnections(endpoints)
    (1 to repetitions).foreach { _ =>
      val e = interceptSparkErr[SparkException](read)
      e.getMessage should include("NOPERM")
    }
    eventually {
      activeConnections(endpoints) shouldBe before
    }
  }

  private val keys = Array.fill(20)(TestUtils.generateRandomKey())

  test("fromRedisKV returns its connections when the read fails") {
    assertReadFailsWithoutLeak(sc.fromRedisKV(keys)(redisConfig = noReadConfig).collect())
  }

  test("fromRedisHash returns its connections when the read fails") {
    assertReadFailsWithoutLeak(sc.fromRedisHash(keys)(redisConfig = noReadConfig).collect())
  }

  test("fromRedisSet returns its connections when the read fails") {
    assertReadFailsWithoutLeak(sc.fromRedisSet(keys)(redisConfig = noReadConfig).collect())
  }

  test("fromRedisList returns its connections when the read fails") {
    assertReadFailsWithoutLeak(sc.fromRedisList(keys)(redisConfig = noReadConfig).collect())
  }

  test("fromRedisZSet and fromRedisZSetWithScore return their connections when the read fails") {
    assertReadFailsWithoutLeak(sc.fromRedisZSet(keys)(redisConfig = noReadConfig).collect())
    assertReadFailsWithoutLeak(sc.fromRedisZSetWithScore(keys)(redisConfig = noReadConfig).collect())
  }

  test("fromRedisZRangeByScore returns its connections when the read fails") {
    assertReadFailsWithoutLeak(sc.fromRedisZRangeByScore(keys, 0, 10)(redisConfig = noReadConfig).collect())
    assertReadFailsWithoutLeak(sc.fromRedisZRangeByScoreWithScore(keys, 0, 10)(redisConfig = noReadConfig).collect())
  }

  test("fromRedisKeyPattern returns its connections when SCAN fails") {
    assertReadFailsWithoutLeak(sc.fromRedisKeyPattern("*")(redisConfig = noReadConfig).collect())
  }

  test("creating a RedisConfig returns the connection when INFO fails") {
    val endpoint = endpointFor(noInfoUser)
    val before = activeConnections(Seq(endpoint))
    (1 to repetitions).foreach { _ =>
      val e = intercept[JedisDataException](new RedisConfig(endpoint))
      e.getMessage should include("NOPERM")
    }
    activeConnections(Seq(endpoint)) shouldBe before
  }

  test("reading a DataFrame without a stored schema returns the connection") {
    val table = TestUtils.generateTableName("no-schema")
    val endpoints = Seq(redisConfig.getHost(RedisSourceRelation.schemaKey(table)).endpoint)
    val before = activeConnections(endpoints)
    (1 to repetitions).foreach { _ =>
      val e = intercept[IllegalStateException](spark.read.format(RedisFormat).option("table", table).load())
      e.getMessage should include("Unable to read dataframe schema")
    }
    activeConnections(endpoints) shouldBe before
  }
}
