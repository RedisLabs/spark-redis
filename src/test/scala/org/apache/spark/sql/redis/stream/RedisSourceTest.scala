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

  test("getOffsetRanges splits the range of a stream between its consumers") {
    val (streamKey, groupName) = ("mystream", "group55")
    val startOffsets = RedisSourceOffset(Map(streamKey -> RedisConsumerOffset(groupName, "100-0")))
    val endOffsets = RedisSourceOffset(Map(streamKey -> RedisConsumerOffset(groupName, "400-5")))
    val consumerConfigs = (1 to 3).map(i => RedisConsumerConfig(streamKey, groupName, s"consumer-$i", 1000, 100))
    val offsetRanges = RedisSource.getOffsetRanges(Some(startOffsets), endOffsets, consumerConfigs)
    offsetRanges shouldBe Seq(
      RedisSourceOffsetRange(Some("100-0"), "200-0", consumerConfigs(0)),
      RedisSourceOffsetRange(Some("200-0"), "300-0", consumerConfigs(1)),
      RedisSourceOffsetRange(Some("300-0"), "400-5", consumerConfigs(2))
    )
  }

  test("testSplitOffsetRange") {
    // split between the first entry of the range and the end
    RedisSource.splitOffsetRange(Some("0-0"), "400-0", 2, Some("300-7")) shouldBe
      Seq(Some("0-0") -> "350-0", Some("350-0") -> "400-0")
    // the ranges stay within (start, end]
    val (start, end) = ("5-3", "5-9")
    RedisSource.splitOffsetRange(Some(start), end, 3) shouldBe
      Seq(Some(start) -> start, Some(start) -> start, Some(start) -> end)
    // without start, the first consumer reads the whole range
    RedisSource.splitOffsetRange(None, end, 2) shouldBe Seq(None -> end, Some(end) -> end)
  }
}
