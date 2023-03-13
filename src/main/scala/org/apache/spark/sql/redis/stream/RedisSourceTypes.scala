package org.apache.spark.sql.redis.stream

import java.util.{List => JList, Map => JMap}

import redis.clients.jedis.{StreamEntryID, StreamEntry => JStreamEntry}

/**
  * @author The Viet Nguyen
  */
object RedisSourceTypes {

  type StreamEntry = (StreamEntryID, JMap[String, Array[Byte]])
  type StreamEntryBatch = JMap.Entry[Array[Byte], JList[JStreamEntry]]
  type StreamEntryBatches = JList[StreamEntryBatch]
}
