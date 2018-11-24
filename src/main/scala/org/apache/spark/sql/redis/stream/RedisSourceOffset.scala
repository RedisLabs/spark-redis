package org.apache.spark.sql.redis.stream

import org.apache.spark.sql.execution.streaming.Offset

/**
  * @author The Viet Nguyen
  */
class RedisSourceOffset(start: String, end: String) extends Offset {

  override def json(): String =
    s"""
       |{
       |  "start": "$start",
       |  "end": "$end"
       |}
    """.stripMargin
}

case object RedisSourceOffset {

  case object Smallest extends Offset {

    override def json(): String = "-"
  }

  case object Greatest extends Offset {

    override def json(): String = "+"
  }
}
