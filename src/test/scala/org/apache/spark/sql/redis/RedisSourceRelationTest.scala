package org.apache.spark.sql.redis

import org.scalatest.{FunSuite, Matchers}

/**
  * @author The Viet Nguyen
  */
class RedisSourceRelationTest extends FunSuite with Matchers {

  test("redis key extractor with prefix pattern") {
    val key = RedisSourceRelation.tableKey("table*", "tablekey")
    key shouldBe "key"
  }

  test("redis key extractor with other patterns") {
    val key = RedisSourceRelation.tableKey("*table", "key")
    key shouldBe "key"
  }
}
