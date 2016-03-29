package com.redislabs.provider.redis.streaming

import com.redislabs.provider.redis.RedisConfig
import org.apache.curator.utils.ThreadUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import redis.clients.jedis._

import scala.util.control.NonFatal

class RedisInputDStream(_ssc: StreamingContext,
                        keys: Array[String],
                        redisConfig: RedisConfig,
                        storageLevel: StorageLevel)
  extends ReceiverInputDStream[(String, String)](_ssc) {
  def getReceiver(): Receiver[(String, String)] = {
    new RedisReceiver(keys, storageLevel, redisConfig)
  }
}

private class RedisReceiver(keys: Array[String],
                            storageLevel: StorageLevel,
                            redisConfig: RedisConfig)
  extends Receiver[(String, String)](storageLevel) {

  def onStart() {
    val executorPool = ThreadUtils.newFixedThreadPool(keys.length, "BlockLists Streamming")
    try {
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
      val keys: Array[String] = Array(key)
      try {
        while(!isStopped) {
          val res = conn.blpop(Integer.MAX_VALUE, keys:_*)
          res match {
            case null => "Really?"
            case _ => store((res.get(0), res.get(1)))
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

