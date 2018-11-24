package org.apache.spark.sql.redis.stream

import org.apache.spark.sql.execution.streaming.Offset

/**
  * @author The Viet Nguyen
  */
case class RedisSourceOffset(offset: String) extends Offset {

  override def json(): String = offset
}

case object RedisSourceOffset {

  case object Smallest extends Offset {

    override def json(): String = "-"
  }

  case object Greatest extends Offset {

    override def json(): String = "+"
  }
}
