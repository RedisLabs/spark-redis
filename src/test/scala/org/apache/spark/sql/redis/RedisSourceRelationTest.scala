package org.apache.spark.sql.redis

import org.scalatest.{FunSuite, Matchers}

/**
  * @author The Viet Nguyen
  */
class RedisSourceRelationTest extends FunSuite with Matchers {

  test("redis key extractor with table name") {
    val key = RedisSourceRelation.tableKey("table", "table:key")
    key shouldBe "key"
  }

  test("redis key extractor with keys pattern") {
    val key = RedisSourceRelation.tableKey("table:*", "table:key")
    key shouldBe "key"
  }

  test("redis key extractor with empty pattern") {
    val key = RedisSourceRelation.tableKey("", "key")
    key shouldBe "key"
  }
}
