package com.redislabs.provider.redis

import com.redislabs.provider.redis.RedisConfig.{AuthConfKey, AuthOption, PasswordConfKey, PasswordOption}
import org.apache.spark.SparkConf
import org.scalatest.{FunSuite, Matchers}

/**
  * How `spark.redis.password`, the deprecated `spark.redis.auth`, and the `password` and deprecated `auth` source
  * options become [[RedisEndpoint.auth]], and that an endpoint's toString doesn't show it (GHSA-wmm3-vmjq-7mmc).
  * No Redis server is needed: building an endpoint does not connect.
  * [[org.apache.spark.sql.redis.PasswordRedactionSuite]] checks that Spark redacts the new names.
  */
class PasswordConfigSuite extends FunSuite with Matchers {

  private def conf(settings: (String, String)*): SparkConf = new SparkConf(false).setAll(settings)

  private val noOptions = Map.empty[String, String]

  test("spark.redis.password sets the endpoint's password") {
    new RedisEndpoint(conf(PasswordConfKey -> "new")).auth shouldBe "new"
    new RedisEndpoint(conf(PasswordConfKey -> "new"), noOptions).auth shouldBe "new"
  }

  test("the deprecated spark.redis.auth still sets it") {
    new RedisEndpoint(conf(AuthConfKey -> "old")).auth shouldBe "old"
    new RedisEndpoint(conf(AuthConfKey -> "old"), noOptions).auth shouldBe "old"
  }

  test("spark.redis.password wins over spark.redis.auth") {
    new RedisEndpoint(conf(PasswordConfKey -> "new", AuthConfKey -> "old")).auth shouldBe "new"
    new RedisEndpoint(conf(PasswordConfKey -> "new", AuthConfKey -> "old"), noOptions).auth shouldBe "new"
  }

  test("without either setting the endpoint sends no AUTH") {
    new RedisEndpoint(conf()).auth shouldBe null
    new RedisEndpoint(conf(), noOptions).auth shouldBe null
  }

  test("the password option and the deprecated auth option override both Spark settings") {
    Seq(conf(), conf(PasswordConfKey -> "conf"), conf(AuthConfKey -> "conf")).foreach { c =>
      new RedisEndpoint(c, Map(PasswordOption -> "option")).auth shouldBe "option"
      new RedisEndpoint(c, Map(AuthOption -> "option")).auth shouldBe "option"
    }
  }

  test("the password option wins over the auth option") {
    new RedisEndpoint(conf(), Map(PasswordOption -> "new", AuthOption -> "old")).auth shouldBe "new"
  }

  test("an endpoint's toString shows every field but the password") {
    val endpoint = RedisEndpoint(host = "redis.example.com", port = 6380, user = "alice", auth = "s3cr3t",
      clientName = "etl-job")
    endpoint.toString shouldBe "RedisEndpoint(redis.example.com,6380,alice,*********(redacted),0,2000,false,etl-job)"
    // Classes that hold an endpoint print it with its toString, as RedisSourceRelation's log of its initial host does.
    RedisNode(endpoint, 0, 16383, 0, 1).toString should not include "s3cr3t"
  }

  test("an endpoint without a password shows null for it") {
    RedisEndpoint(host = "h", port = 1).toString shouldBe "RedisEndpoint(h,1,null,null,0,2000,false,null)"
  }

  test("toString doesn't change the endpoint") {
    val endpoint = RedisEndpoint(auth = "s3cr3t")
    endpoint.toString
    endpoint.auth shouldBe "s3cr3t"
    endpoint shouldBe RedisEndpoint(auth = "s3cr3t")
  }
}
