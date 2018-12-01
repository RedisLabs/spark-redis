package org.apache.spark.sql.redis.stream

/**
  * @author The Viet Nguyen
  */
case class RedisSourceOffsetRange(streamKey: String, groupName: String, start: Option[String],
                                  end: String)
