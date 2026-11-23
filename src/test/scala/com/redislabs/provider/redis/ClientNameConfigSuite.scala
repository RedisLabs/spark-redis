package com.redislabs.provider.redis

import com.redislabs.provider.redis.RedisConfig.{ClientNameConfKey, ClientNameOption}
import org.apache.spark.SparkConf
import org.scalatest.{FunSuite, Matchers}

/**
  * How `spark.redis.client.name` and the `client.name` source option become [[RedisEndpoint.clientName]] (#398).
  * No Redis server is needed: building an endpoint does not connect (building a RedisConfig does, to find the
  * nodes). The suites that extend [[ClientNameSuite]] check that the name reaches every connection.
  */
class ClientNameConfigSuite extends FunSuite with Matchers {

  private def conf(clientName: Option[String]): SparkConf = {
    val c = new SparkConf(false).set("spark.redis.host", "127.0.0.1")
    clientName.foreach(c.set(ClientNameConfKey, _))
    c
  }

  test("spark.redis.client.name sets the endpoint's client name") {
    new RedisEndpoint(conf(Some("etl-job"))).clientName shouldBe "etl-job"
  }

  test("without spark.redis.client.name the endpoint has no client name") {
    new RedisEndpoint(conf(None)).clientName shouldBe null
    new RedisEndpoint(conf(None), Map.empty[String, String]).clientName shouldBe null
    RedisEndpoint().clientName shouldBe null
  }

  test("the client.name option overrides spark.redis.client.name") {
    new RedisEndpoint(conf(Some("etl-job")), Map(ClientNameOption -> "orders-source")).clientName shouldBe
      "orders-source"
    new RedisEndpoint(conf(None), Map(ClientNameOption -> "orders-source")).clientName shouldBe "orders-source"
  }

  test("a source without the client.name option uses spark.redis.client.name") {
    new RedisEndpoint(conf(Some("etl-job")), Map("table" -> "person")).clientName shouldBe "etl-job"
  }

  test("an empty client name means no name, and an empty client.name option clears the Spark setting") {
    new RedisEndpoint(conf(Some(""))).clientName shouldBe null
    new RedisEndpoint(conf(Some("etl-job")), Map(ClientNameOption -> "")).clientName shouldBe null
    // So the endpoint, and the connection pool keyed by it, is the same as with no setting at all.
    new RedisEndpoint(conf(Some(""))) shouldBe new RedisEndpoint(conf(None))
  }

  test("endpoints that differ only by client name are different endpoints") {
    // ConnectionPool keys its pools by endpoint, so each name gets its own pool of named connections.
    RedisEndpoint(clientName = "a") should not be RedisEndpoint(clientName = "b")
    RedisEndpoint(clientName = "a") should not be RedisEndpoint()
  }

  test("a client name Redis would refuse is rejected when the endpoint is built") {
    // CLIENT SETNAME refuses spaces, newlines and anything outside printable ASCII.
    Seq("etl job", "etl\njob", "etl\tjob", "café").foreach { bad =>
      val fromConf = intercept[IllegalArgumentException](new RedisEndpoint(conf(Some(bad))))
      fromConf.getMessage should include(ClientNameConfKey)
      val fromOption = intercept[IllegalArgumentException](
        new RedisEndpoint(conf(None), Map(ClientNameOption -> bad)))
      fromOption.getMessage should include(ClientNameOption)
      intercept[IllegalArgumentException](RedisEndpoint(clientName = bad))
    }
  }

  test("every printable ASCII character other than space is accepted") {
    val allowed = ('!' to '~').mkString
    RedisEndpoint(clientName = allowed).clientName shouldBe allowed
  }

  test("maskPassword keeps the client name") {
    RedisEndpoint(auth = "passwd", clientName = "etl-job").maskPassword().clientName shouldBe "etl-job"
  }
}
