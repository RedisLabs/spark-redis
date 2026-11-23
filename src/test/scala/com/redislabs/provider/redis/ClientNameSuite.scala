package com.redislabs.provider.redis

import com.redislabs.provider.redis.RedisConfig.{ClientNameConfKey, ClientNameOption}
import com.redislabs.provider.redis.df.RedisDataframeSuite
import com.redislabs.provider.redis.util.TestUtils.{generateRandomKey, generateTableName}
import redis.clients.jedis.Jedis

/**
  * Every connection the connector opens carries the configured client name, as CLIENT LIST shows (#398).
  *
  * The suite's SparkSession sets `spark.redis.client.name` to [[SessionClientName]]. Each test records the newest
  * client id on every Redis node, runs a job, and then reads the names of the connections opened since: the ones
  * the job left idle in the connector's pools. Pools are keyed by endpoint and live for the whole test JVM, so a job
  * that reuses an existing pool opens nothing new. Each test therefore uses an endpoint no earlier test has used.
  * The tests run in the order written; see the notes on the first two.
  */
trait ClientNameSuite extends RedisDataframeSuite {

  val SessionClientName: String = uniqueName("session")

  private def uniqueName(role: String): String = s"spark-redis-$role-${generateRandomKey().take(8)}"

  override def beforeAll(): Unit = {
    conf.set(ClientNameConfKey, SessionClientName)
    super.beforeAll()
  }

  /** Every node the connector talks to: the standalone server, or each node of the cluster. */
  private lazy val servers: Seq[(String, Int)] =
    redisConfig.nodes.map(n => (n.endpoint.host, n.endpoint.port)).distinct.toSeq

  /** A plain Jedis connection, outside the connector's pools, so it does not affect what it observes. */
  private def withObserver[A](server: (String, Int))(body: Jedis => A): A = {
    val conn = new Jedis(server._1, server._2)
    try {
      val endpoint = redisConfig.initialHost
      Option(endpoint.auth).foreach(password => conn.auth(Option(endpoint.user).getOrElse("default"), password))
      body(conn)
    } finally {
      conn.close()
    }
  }

  /** CLIENT LIST as one field -> value map per client connection, leaving out replication links. */
  private def clients(conn: Jedis): Seq[Map[String, String]] =
    conn.clientList().split("\n").map(_.trim).filter(_.nonEmpty).toSeq.map { line =>
      line.split(" ").map { field =>
        val kv = field.split("=", 2)
        kv(0) -> (if (kv.length > 1) kv(1) else "")
      }.toMap
    }.filterNot(c => c.getOrElse("flags", "").exists(f => f == 'S' || f == 'M'))

  /** Runs body, then returns the name of each connection it opened that is still open, on every node. */
  private def namesOfConnectionsOpenedBy(body: => Unit): Seq[String] = {
    val newestBefore = servers.map { s =>
      s -> withObserver(s)(conn => clients(conn).map(_("id").toLong).max)
    }.toMap
    body
    servers.flatMap { s =>
      withObserver(s) { conn =>
        val self = conn.clientId().longValue()
        clients(conn)
          .filter { c =>
            val id = c("id").toLong
            id > newestBefore(s) && id != self
          }
          .map(_("name"))
      }
    }
  }

  /** Writes and reads back string keys with the RDD API, by default with the config it builds from the session. */
  private def writeAndReadRdd(config: RedisConfig = RedisConfig.fromSparkConf(sc.getConf)): Unit = {
    val readWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)
    val prefix = generateRandomKey()
    val kvs = (1 to 50).map(i => (s"$prefix:$i", i.toString))
    sc.toRedisKV(sc.parallelize(kvs, 4))(config, readWriteConfig)
    sc.fromRedisKV(s"$prefix:*")(config, readWriteConfig).collect() should contain theSameElementsAs kvs
  }

  // This test must run before anything opens connections named SessionClientName. If the source wrongly used the
  // Spark setting, its connections would then be new ones in a new pool and show up here under the session name.
  test("the client.name option overrides spark.redis.client.name for that DataFrame source") {
    val sourceName = uniqueName("source")
    val table = generateTableName("clientName")
    val names = namesOfConnectionsOpenedBy {
      writeDf(table, Map(ClientNameOption -> sourceName))
      loadAndVerifyDf(table, Map(ClientNameOption -> sourceName))
    }
    names should not be empty
    all(names) shouldBe sourceName
  }

  // The first use of SessionClientName in this JVM, so the RDD API's pool for it starts empty here.
  test("spark.redis.client.name names every connection the RDD API and a DataFrame source open") {
    val table = generateTableName("clientName")
    val names = namesOfConnectionsOpenedBy {
      writeAndReadRdd()
      writeDf(table)
      loadAndVerifyDf(table)
    }
    names should not be empty
    all(names) shouldBe SessionClientName
  }

  test("without a client name, connections are left unnamed") {
    // The session's settings without the name, and with a timeout no other suite uses: the endpoint without a name
    // that every other suite in this JVM shares would hand out their idle connections instead of opening new ones.
    val unnamed = RedisConfig.fromSparkConf(sc.getConf.remove(ClientNameConfKey).set("spark.redis.timeout", "2468"))
    unnamed.initialHost.clientName shouldBe null
    val names = namesOfConnectionsOpenedBy {
      writeAndReadRdd(unnamed)
    }
    names should not be empty
    all(names) shouldBe ""
  }

  test("an empty client.name option leaves that DataFrame source's connections unnamed") {
    // As above, a timeout of its own gives the source a pool of its own.
    val table = generateTableName("clientName")
    val options = Map(ClientNameOption -> "", "timeout" -> "2469")
    val names = namesOfConnectionsOpenedBy {
      writeDf(table, options)
      loadAndVerifyDf(table, options)
    }
    names should not be empty
    all(names) shouldBe ""
  }
}
