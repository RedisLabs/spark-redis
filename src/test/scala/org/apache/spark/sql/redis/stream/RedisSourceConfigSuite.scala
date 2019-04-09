package org.apache.spark.sql.redis.stream

import org.apache.spark.sql.redis._
import org.scalatest.{FunSuite, Matchers}

/**
  * @author The Viet Nguyen
  */
class RedisSourceConfigSuite extends FunSuite with Matchers {

  val group: String = "group55"

  test("testFromMap") {
    val config = RedisSourceConfig.fromMap(Map(
      StreamOptionStreamKeys -> "mystream1,mystream2,mystream3",
      StreamOptionStreamOffsets ->
        s"""
          |{
          |  "offsets":{
          |    "mystream1": {
          |      "groupName": "$group",
          |      "offset": "0-10"
          |    },
          |    "mystream2": {
          |       "groupName": "$group",
          |       "offset": "0-7"
          |    }
          |  }
          |}
        """.stripMargin,
      StreamOptionParallelism -> "2",
      StreamOptionGroupName -> group,
      StreamOptionConsumerPrefix -> "consumer"
    ))
    config shouldBe RedisSourceConfig(
      Seq(
        RedisConsumerConfig("mystream1", group, "consumer-1", 100, 500),
        RedisConsumerConfig("mystream1", group, "consumer-2", 100, 500),
        RedisConsumerConfig("mystream2", group, "consumer-1", 100, 500),
        RedisConsumerConfig("mystream2", group, "consumer-2", 100, 500),
        RedisConsumerConfig("mystream3", group, "consumer-1", 100, 500),
        RedisConsumerConfig("mystream3", group, "consumer-2", 100, 500)
      ),
      Some(RedisSourceOffset(Map(
        "mystream1" -> RedisConsumerOffset(group, "0-10"),
        "mystream2" -> RedisConsumerOffset(group, "0-7")
      )))
    )
  }
}
