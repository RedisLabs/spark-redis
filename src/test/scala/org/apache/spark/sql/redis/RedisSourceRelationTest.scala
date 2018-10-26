package org.apache.spark.sql.redis

import org.scalatest.{FunSuite, Matchers}

/**
  * @author The Viet Nguyen
  */
class RedisSourceRelationTest extends FunSuite with Matchers {

  test("redis key extractor") {
    val key = RedisSourceRelation.tableKey("table", "table:key")
    key shouldBe "key"
  }
}
