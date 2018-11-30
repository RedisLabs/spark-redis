package com.redislabs.provider.redis.util

import org.scalatest.{FunSuite, Matchers}

/**
  * @author The Viet Nguyen
  */
class JsonUtilsTest extends FunSuite with Matchers {

  test("testToJson") {
    val json = JsonUtils.toJson(Map("key" -> "value"))
    json shouldBe """{"key":"value"}"""
  }
}
