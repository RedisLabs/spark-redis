package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.util.JsonUtils
import org.apache.spark.sql.execution.streaming.Offset
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

/**
  * @author The Viet Nguyen
  */
case class RedisSourceOffset(offsets: Map[String, RedisConsumerOffset]) extends Offset {

  override def json(): String = JsonUtils.toJson(this)
}

object RedisSourceOffset {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def fromJson(json: String): RedisSourceOffset = {
    Serialization.read[RedisSourceOffset](json)
  }
}

case class RedisConsumerOffset(groupName: String, offset: String)

case class RedisSourceOffsetRange(streamKey: String, groupName: String, start: Option[String],
                                  end: String)
