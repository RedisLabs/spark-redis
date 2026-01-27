package com.redislabs.provider.redis

import org.scalatest.{FunSuite, Matchers}

/**
  * Unit tests for RedisClientLibraryInfo object.
  * These tests validate the library identification information used by CLIENT SETINFO.
  */
class RedisClientLibraryInfoTest extends FunSuite with Matchers {

  test("sparkRedisVersion should return a non-null value") {
    RedisClientLibraryInfo.sparkRedisVersion should not be null
  }

  test("sparkRedisVersion should not be empty") {
    RedisClientLibraryInfo.sparkRedisVersion should not be empty
  }

  test("jedisVersion should return a non-null value") {
    RedisClientLibraryInfo.jedisVersion should not be null
  }

  test("jedisVersion should not be empty") {
    RedisClientLibraryInfo.jedisVersion should not be empty
  }

  test("libName should contain spark-redis identifier") {
    RedisClientLibraryInfo.libName should include("spark-redis")
  }

  test("libName should have expected format with jedis prefix") {
    // Format: jedis(spark-redis_v{version})
    RedisClientLibraryInfo.libName should startWith("jedis(spark-redis_v")
    RedisClientLibraryInfo.libName should endWith(")")
  }

  test("libVersion should match jedisVersion") {
    RedisClientLibraryInfo.libVersion shouldBe RedisClientLibraryInfo.jedisVersion
  }

  test("libVersion should not be empty") {
    RedisClientLibraryInfo.libVersion should not be empty
  }
}

