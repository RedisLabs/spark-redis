package org.apache.spark.sql.redis.stream

import org.scalatest.{FunSuite, Matchers}

/**
  * @author The Viet Nguyen
  */
class RedisConsumerOffsetTest extends FunSuite with Matchers {

  test("testFromJson") {
    val offset = RedisSourceOffset.fromJson(
      """
        |{
        |  "offsets":{
        |    "mystream": {
        |      "groupName": "group55",
        |      "offset": "1543674099961-0"
        |    }
        |  }
        |}
        |""".stripMargin)
    offset shouldBe RedisSourceOffset(Map("mystream" ->
      RedisConsumerOffset("group55", "1543674099961-0")))
  }
}
