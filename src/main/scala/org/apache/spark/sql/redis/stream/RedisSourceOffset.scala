package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.util.JsonUtils
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.redis.stream.RedisSourceOffset.RedisConsumerOffset

/**
  * @author The Viet Nguyen
  */
case class RedisSourceOffset(offsets: Map[String, RedisConsumerOffset]) extends Offset {

  override def json(): String = JsonUtils.toJson(offsets)
}

case object RedisSourceOffset {

  case class RedisConsumerOffset(groupName: String, offset: String)

  case object Smallest extends Offset {

    override def json(): String = "-"
  }

  case object Greatest extends Offset {

    override def json(): String = "+"
  }

}
