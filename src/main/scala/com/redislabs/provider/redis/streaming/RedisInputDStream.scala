package com.redislabs.provider.redis.streaming

import com.redislabs.provider.redis.RedisConfig
import org.apache.curator.utils.ThreadUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import redis.clients.jedis._

import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

/**
  * Receives messages from Redis List
  */
class RedisInputDStream[T: ClassTag](_ssc: StreamingContext,
                                     keys: Array[String],
                                     storageLevel: StorageLevel,
                                     redisConfig: RedisConfig,
                                     streamType: Class[T])
  extends ReceiverInputDStream[T](_ssc) {
  def getReceiver(): Receiver[T] = {
    new RedisReceiver(keys, storageLevel, redisConfig, streamType)
  }
}


private class RedisReceiver[T: ClassTag](keys: Array[String],
                                         storageLevel: StorageLevel,
                                         redisConfig: RedisConfig,
                                         streamType: Class[T])
  extends Receiver[T](storageLevel) {

  def onStart() {
    val executorPool = ThreadUtils.newFixedThreadPool(keys.length, "BlockLists Streaming")
    try {
      /* start a executor for each interested List */
      keys.foreach{ key =>
        executorPool.submit(new MessageHandler(redisConfig.connectionForKey(key), key))
      }
    } finally {
      executorPool.shutdown()
    }
  }

  def onStop() {
  }

  private class MessageHandler(conn: Jedis, key: String) extends Runnable {
    def run() {
      try {
        while(!isStopped) {
          val response = conn.blpop(2, key)
          if (response == null || response.isEmpty) {
            // no-op
          } else if (classTag[T] == classTag[String]) {
            store(response.get(1).asInstanceOf[T])
          } else if (classTag[T] == classTag[(String, String)]) {
            store((response.get(0), response.get(1)).asInstanceOf[T])
          } else {
            throw new scala.Exception("Unknown Redis Streaming type")
          }
        }
      } catch {
        case NonFatal(e) =>
          restart("Error receiving data", e)
      } finally {
        onStop()
      }
    }
  }
}
