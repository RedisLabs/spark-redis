package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.util.JsonUtils
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

/**
  * @param offsets A map of offset by stream key
  * @author The Viet Nguyen
  */
case class RedisSourceOffset(offsets: Map[String, RedisConsumerOffset]) extends Offset {

  override def json(): String = JsonUtils.toJson(this)
}

object RedisSourceOffset {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def fromOffset(offset: Offset): RedisSourceOffset = {
    offset match {
      case o: RedisSourceOffset => o
      case so: SerializedOffset => fromJson(so.json)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to RedisSourceOffset")
    }

    fromJson(offset.json())
  }

  def fromJson(json: String): RedisSourceOffset = {
    try {
      Serialization.read[RedisSourceOffset](json)
    } catch {
      case e: Throwable =>
        val example = RedisSourceOffset(Map("my-stream" -> RedisConsumerOffset("redis-source", "1543674099961-0")))
        val jsonExample = Serialization.write(example)
        throw new RuntimeException(s"Unable to parse offset json. Example of valid json: $jsonExample", e)
    }
  }
}

case class RedisConsumerOffset(groupName: String, offset: String)

case class RedisSourceOffsetRange(start: Option[String], end: String, config: RedisConsumerConfig)
