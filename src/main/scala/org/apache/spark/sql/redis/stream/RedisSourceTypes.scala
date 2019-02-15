package org.apache.spark.sql.redis.stream

import java.util.{List => JList, Map => JMap}

import redis.clients.jedis.{EntryID, StreamEntry => JStreamEntry}

/**
  * @author The Viet Nguyen
  */
object RedisSourceTypes {

  type StreamEntry = (EntryID, JMap[String, String])
  type StreamEntryBatch = JMap.Entry[String, JList[JStreamEntry]]
  type StreamEntryBatches = JList[StreamEntryBatch]
}
