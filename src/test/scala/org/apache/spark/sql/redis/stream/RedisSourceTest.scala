package org.apache.spark.sql.redis.stream

import org.scalatest.{FunSuite, Matchers, OptionValues}

/**
  * @author The Viet Nguyen
  */
class RedisSourceTest extends FunSuite with Matchers with OptionValues {

  test("testGetOffsetRanges") {
    val startOffsets = RedisSourceOffset(Map("mystream" -> RedisConsumerOffset("group55", "0-0")))
    val endOffsets = RedisSourceOffset(Map("mystream" -> RedisConsumerOffset("group55", "0-1")))
    val consumerConfig = RedisConsumerConfig("mystream", "group55", "consumer", 1000, 100)
    val consumerConfigs = Seq(consumerConfig)
    val offsetRanges = RedisSource.getOffsetRanges(Some(startOffsets), endOffsets, consumerConfigs)
    offsetRanges.head shouldBe RedisSourceOffsetRange(Some("0-0"), "0-1", consumerConfig)
  }
}
