package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.util.JsonUtils
import org.apache.spark.sql.execution.streaming.Offset

/**
  * @author The Viet Nguyen
  */
case class RedisSourceOffset(offsets: Map[String, RedisConsumerOffset]) extends Offset {

  override def json(): String = JsonUtils.toJson(offsets)
}

case class RedisConsumerOffset(groupName: String, offset: String)

case class RedisSourceOffsetRange(streamKey: String, groupName: String, start: Option[String],
                                  end: String)
