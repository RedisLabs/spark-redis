package com.redislabs.provider.redis.util

import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.{EntryID, Jedis}

/**
  * @author The Viet Nguyen
  */
object StreamUtils extends Logging {

  val EntryIdEarliest = new EntryID(0, 0)

  def createConsumerGroupIfNotExist(conn: Jedis, streamKey: String, groupName: String,
                                    offset: Option[EntryID]): Unit = {
    try {
      val actualOffset = offset.getOrElse(EntryIdEarliest)
      conn.xgroupCreate(streamKey, groupName, actualOffset, true)
    } catch {
      case e: Exception if StringUtils.contains(e.getMessage, "already exists") =>
        logInfo(s"Consumer group already exists: $groupName")
        offset.foreach { os =>
          logInfo(s"Reset consumer group to: $os")
          resetConsumerGroup(conn, streamKey, groupName, os)
        }
    }
  }

  def resetConsumerGroup(conn: Jedis, streamKey: String, groupName: String,
                         offset: EntryID): Unit = {
    conn.xgroupSetID(streamKey, groupName, offset)
  }
}
