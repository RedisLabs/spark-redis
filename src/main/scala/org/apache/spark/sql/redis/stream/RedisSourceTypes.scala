package org.apache.spark.sql.redis.stream

import java.util.{List => JList, Map => JMap}

import redis.clients.jedis.{EntryID, StreamEntry}

/**
  * @author The Viet Nguyen
  */
object RedisSourceTypes {

  type EntryIdWithFields = (EntryID, JMap[String, String])
  type StreamEntryBatch = JMap.Entry[String, JList[StreamEntry]]
  type StreamEntryBatches = JList[StreamEntryBatch]
}
