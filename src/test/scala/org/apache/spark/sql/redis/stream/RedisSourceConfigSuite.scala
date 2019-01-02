package org.apache.spark.sql.redis.stream

import org.apache.spark.sql.redis._
import org.scalatest.{FunSuite, Matchers}

/**
  * @author The Viet Nguyen
  */
class RedisSourceConfigSuite extends FunSuite with Matchers {

  test("testFromMap") {
    val config = RedisSourceConfig.fromMap(Map(
      StreamOptionStreamKeys -> "mystream1,mystream2,mystream3",
      StreamOptionStreamOffsets ->
        """
          |{
          |  "offsets":{
          |    "mystream1": {
          |      "groupName": "group55",
          |      "offset": "0-10"
          |    },
          |    "mystream2": {
          |       "groupName": "group55",
          |       "offset": "0-7"
          |    }
          |  }
          |}
        """.stripMargin,
      StreamOptionParallelism -> "2",
      StreamOptionGroupName -> "group55",
      StreamOptionConsumerPrefix -> "consumer"
    ))
    config shouldBe RedisSourceConfig(
      Seq(
        RedisConsumerConfig("mystream1", "group55", "consumer-1", 100, 500),
        RedisConsumerConfig("mystream1", "group55", "consumer-2", 100, 500),
        RedisConsumerConfig("mystream2", "group55", "consumer-1", 100, 500),
        RedisConsumerConfig("mystream2", "group55", "consumer-2", 100, 500),
        RedisConsumerConfig("mystream3", "group55", "consumer-1", 100, 500),
        RedisConsumerConfig("mystream3", "group55", "consumer-2", 100, 500)
      ),
      Some(RedisSourceOffset(Map(
        "mystream1" -> RedisConsumerOffset("group55", "0-10"),
        "mystream2" -> RedisConsumerOffset("group55", "0-7")
      )))
    )
  }
}
