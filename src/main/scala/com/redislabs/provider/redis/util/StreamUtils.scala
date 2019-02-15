package com.redislabs.provider.redis.util

import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.{EntryID, Jedis}

/**
  * @author The Viet Nguyen
  */
object StreamUtils extends Logging {

  val EntryIdEarliest = new EntryID(0, 0)

  def createConsumerGroupIfNotExist(conn: Jedis, streamKey: String, groupName: String,
                                    offset: EntryID): Unit = {
    try {
      conn.xgroupCreate(streamKey, groupName, offset, true)
    } catch {
      case e: Exception if StringUtils.contains(e.getMessage, "already exists") =>
        logInfo(s"Consumer group already exists: $groupName")
    }
  }

  def resetConsumerGroup(conn: Jedis, streamKey: String, groupName: String,
                         offset: EntryID): Unit = {
    logInfo(s"Setting consumer group $groupName id to $offset")
    conn.xgroupSetID(streamKey, groupName, offset)
  }
}
