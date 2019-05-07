package com.redislabs.provider.redis.streaming

import com.redislabs.provider.redis.{ReadWriteConfig, RedisConfig}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

/**
  * RedisStreamingContext extends StreamingContext's functionality with Redis
  *
  * @param ssc a spark StreamingContext
  */
class RedisStreamingContext(@transient val ssc: StreamingContext) extends Serializable {
  /**
    * @param keys         an Array[String] which consists all the Lists we want to listen to
    * @param storageLevel the receiver' storage tragedy of received data, default as MEMORY_AND_DISK_2
    * @return a stream of (listname, value)
    */
  def createRedisStream(keys: Array[String],
                        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
                       (implicit
                        redisConfig: RedisConfig = RedisConfig.fromSparkConf(ssc.sparkContext.getConf)):
  RedisInputDStream[(String, String)] = {
    new RedisInputDStream(ssc, keys, storageLevel, redisConfig, classOf[(String, String)])
  }

  /**
    * @param keys         an Array[String] which consists all the Lists we want to listen to
    * @param storageLevel the receiver' storage tragedy of received data, default as MEMORY_AND_DISK_2
    * @return a stream of (value)
    */
  def createRedisStreamWithoutListname(keys: Array[String],
                                       storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
                                      (implicit
                                       redisConf: RedisConfig = RedisConfig.fromSparkConf(ssc.sparkContext.getConf)):
  RedisInputDStream[String] = {
    new RedisInputDStream(ssc, keys, storageLevel, redisConf, classOf[String])
  }

  def createRedisXStream(consumersConfig: Seq[ConsumerConfig],
                         storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
                        (implicit
                         redisConfig: RedisConfig = RedisConfig.fromSparkConf(ssc.sparkContext.getConf)):
  InputDStream[StreamItem] = {
    val readWriteConfig = ReadWriteConfig.fromSparkConf(ssc.sparkContext.getConf)
    val receiver = new RedisStreamReceiver(consumersConfig, redisConfig, readWriteConfig, storageLevel)
    ssc.receiverStream(receiver)
  }
}

trait RedisStreamingFunctions {

  implicit def toRedisStreamingContext(ssc: StreamingContext): RedisStreamingContext = new RedisStreamingContext(ssc)

}

