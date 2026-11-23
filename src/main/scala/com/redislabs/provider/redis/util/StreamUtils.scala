package com.redislabs.provider.redis.util

import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.{StreamEntryID, Jedis}

/**
  * @author The Viet Nguyen
  */
object StreamUtils extends Logging {

  val EntryIdEarliest = new StreamEntryID(0, 0)

  /**
    * The smallest entry id greater than `id`, to read a range that excludes its start (XRANGE includes both ends).
    */
  def nextEntryId(id: StreamEntryID): StreamEntryID = {
    if (id.getSequence == Long.MaxValue) {
      new StreamEntryID(id.getTime + 1, 0)
    } else {
      new StreamEntryID(id.getTime, id.getSequence + 1)
    }
  }

  def createConsumerGroupIfNotExist(conn: Jedis, streamKey: String, groupName: String,
                                    offset: StreamEntryID): Unit = {
    try {
      conn.xgroupCreate(streamKey, groupName, offset, true)
    } catch {
      case e: Exception if StringUtils.contains(e.getMessage, "already exists") =>
        logInfo(s"Consumer group already exists: $groupName")
    }
  }

  def resetConsumerGroup(conn: Jedis, streamKey: String, groupName: String,
                         offset: StreamEntryID): Unit = {
    logInfo(s"Setting consumer group $groupName id to $offset")
    conn.xgroupSetID(streamKey, groupName, offset)
  }
}
