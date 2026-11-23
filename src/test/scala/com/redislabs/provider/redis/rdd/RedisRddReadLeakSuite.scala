package com.redislabs.provider.redis.rdd

import java.util.concurrent.ConcurrentHashMap

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.TestUtils
import com.redislabs.provider.redis.util.TestUtils.interceptSparkErr
import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd,
  SparkListenerTaskStart}
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
  /** The commands the RDD reads run: sets are read with SSCAN, and score ranges with ZRANGE ... BYSCORE. */
  private val readCommands = Seq("get", "hgetall", "sscan", "lrange", "zrange", "scan")
  private val repetitions = 3

  implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10000, Millis)))

  /**
    * The jobs and tasks of this suite that started and haven't ended yet.
    *
    * When a task fails, Spark fails the job and cancels its other tasks, but those run on, holding their connection,
    * until their current command returns. `SparkStatusTracker`'s running-task count doesn't include them, so it can't
    * tell when they are done. Spark reports every task start of a job before the job's end, so once no job and no
    * task is left here, no task of a failed read can still hold a connection.
    */
  private object RunningWork extends SparkListener {
    private val jobs = ConcurrentHashMap.newKeySet[Int]()
    private val tasks = ConcurrentHashMap.newKeySet[Long]()

    override def onJobStart(event: SparkListenerJobStart): Unit = jobs.add(event.jobId)
    override def onJobEnd(event: SparkListenerJobEnd): Unit = jobs.remove(event.jobId)
    override def onTaskStart(event: SparkListenerTaskStart): Unit = tasks.add(event.taskInfo.taskId)
    override def onTaskEnd(event: SparkListenerTaskEnd): Unit = tasks.remove(event.taskInfo.taskId)

    def awaitNone(): Unit = eventually {
      withClue(s"jobs still running: $jobs, tasks still running: $tasks") {
        (jobs.isEmpty && tasks.isEmpty) shouldBe true
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.addSparkListener(RunningWork)
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

  /**
    * Counts the connections only when no task of this suite is running, before and after the reads. A cancelled task
    * returns its connection when it ends, so any connection still active then has leaked.
    */
  private def assertReadFailsWithoutLeak(read: => Any): Unit = {
    val endpoints = noReadConfig.hosts.map(_.endpoint).toSeq
    RunningWork.awaitNone()
    val before = activeConnections(endpoints)
    (1 to repetitions).foreach { _ =>
      val e = interceptSparkErr[SparkException](read)
      e.getMessage should include("NOPERM")
    }
    RunningWork.awaitNone()
    activeConnections(endpoints) shouldBe before
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
    // a timeout that no other suite uses gives the read a pool of its own (pools are per endpoint), so connections that
    // other suites are still returning to the shared pool don't change the count
    val timeout = 2345
    val isolatedConfig = new RedisConfig(redisConfig.initialHost.copy(timeout = timeout))
    val endpoints = Seq(isolatedConfig.getHost(RedisSourceRelation.schemaKey(table)).endpoint)
    val before = activeConnections(endpoints)
    (1 to repetitions).foreach { _ =>
      val e = intercept[IllegalStateException] {
        spark.read.format(RedisFormat).option("table", table).option("timeout", timeout).load()
      }
      e.getMessage should include("Unable to read dataframe schema")
    }
    activeConnections(endpoints) shouldBe before
  }
}
