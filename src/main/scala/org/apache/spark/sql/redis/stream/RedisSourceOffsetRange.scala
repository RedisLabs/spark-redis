package org.apache.spark.sql.redis.stream

/**
  * @author The Viet Nguyen
  */
case class RedisSourceOffsetRange(streamKey: String, start: Option[String], end: String)
