package org.apache.spark.sql.redis.stream

import org.apache.spark.sql.execution.streaming.Offset
import redis.clients.jedis.EntryID

/**
  * @author The Viet Nguyen
  */
case class RedisSourceOffsetRange(prev: Option[Offset], end: Offset) {

  def startId: EntryID = prev match {
    case None => new EntryID(0, 0)
    case Some(offset) =>
      val previousEnd = new EntryID(offset.json())
      new EntryID(previousEnd.getTime, previousEnd.getSequence + 1)
  }

  def endId: EntryID = new EntryID(end.json())
}
