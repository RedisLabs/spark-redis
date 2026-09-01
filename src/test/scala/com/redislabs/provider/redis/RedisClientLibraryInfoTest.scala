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

  test("jedisVersion should return a non-null value") {
    RedisClientLibraryInfo.jedisVersion should not be null
  }

  test("libVersion should return a non-null value") {
    RedisClientLibraryInfo.libVersion should not be null
  }

  test("a missing version is empty rather than the literal 'unknown'") {
    RedisClientLibraryInfo.sparkRedisVersion should not be "unknown"
    RedisClientLibraryInfo.jedisVersion should not be "unknown"
    RedisClientLibraryInfo.libVersion should not be "unknown"
  }

  test("libName is empty in the version substring when the version is missing") {
    if (RedisClientLibraryInfo.sparkRedisVersion.isEmpty) {
      RedisClientLibraryInfo.libName shouldBe "jedis(spark-redis)"
    } else {
      RedisClientLibraryInfo.libName shouldBe
        s"jedis(spark-redis_v${RedisClientLibraryInfo.sparkRedisVersion})"
    }
  }

  test("libName never leaves a stray '_v' separator") {
    RedisClientLibraryInfo.libName should not include "_v)"
  }

  test("libName should contain spark-redis identifier") {
    RedisClientLibraryInfo.libName should include("spark-redis")
  }

  test("libName should have expected format with jedis prefix") {
    // Format: jedis(spark-redis) or jedis(spark-redis_v{version})
    RedisClientLibraryInfo.libName should startWith("jedis(spark-redis")
    RedisClientLibraryInfo.libName should endWith(")")
  }

  test("libVersion should match jedisVersion") {
    RedisClientLibraryInfo.libVersion shouldBe RedisClientLibraryInfo.jedisVersion
  }
}
