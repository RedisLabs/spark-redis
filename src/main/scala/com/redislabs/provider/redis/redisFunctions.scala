package com.redislabs.provider.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

import redis.clients.util.{SafeEncoder, JedisClusterCRC16}
import com.redislabs.provider.redis.rdd._



//import com.redislabs.provider.redis.NodesInfo._


/**
  * RedisContext extends sparkContext's functionality with redis functions
  * @param sc a spark context
  */
class RedisContext(@transient val sc: SparkContext) extends Serializable {

  import com.redislabs.provider.redis.RedisContext._


  val redisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))

  /**
    * @param keyPattern
    * @param partitionNum number of partitions
    * @return RedisKeysRDD of simple Keys stored in redis server
    */
  def fromRedisKeyPattern(keyPattern: String = "*",
                          partitionNum: Int = 3)
                         (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
  RedisKeysRDD = {

    new RedisKeysRDD(sc, redisConfig, keyPattern, partitionNum);

  }

  /**
    * @param kvs Pair RDD of K/V
    */
  def toRedisKV(kvs: RDD[(String, String)])
     (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    kvs.foreachPartition(partition => setKVs(partition, redisConfig))
  }

  /**
    * @param kvs      Pair RDD of K/V
    * @param hashName target hash's name which hold all the kvs
    */
  def toRedisHASH(kvs: RDD[(String, String)],
                  hashName: String)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    kvs.foreachPartition(partition => setHash(hashName, partition, redisConfig))
  }

  /**
    * @param kvs      Pair RDD of K/V
    * @param zsetName target zset's name which hold all the kvs
    */
  def toRedisZSET(kvs: RDD[(String, String)],
                  zsetName: String)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    kvs.foreachPartition(partition => setZset(zsetName, partition, redisConfig))
  }

  /**
    * @param vs      RDD of values
    * @param setName target set's name which hold all the vs
    */
  def toRedisSET(vs: RDD[String],
                 setName: String)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    vs.foreachPartition(partition => setSet(setName, partition, redisConfig))
  }

  /**
    * @param vs       RDD of values
    * @param listName target list's name which hold all the vs
    */
  def toRedisLIST(vs: RDD[String],
                  listName: String)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    vs.foreachPartition(partition => setList(listName, partition, redisConfig))
  }

  /**
    * @param vs       RDD of values
    * @param listName target list's name which hold all the vs
    * @param listSize target list's size
    *                 save all the vs to listName(list type) in redis-server
    */
  def toRedisFixedLIST(vs: RDD[String],
                       listName: String,
                       listSize: Int = 0)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    vs.foreachPartition(partition => setFixedList(listName, listSize, partition, redisConfig))
  }

}



object RedisContext extends Serializable {
  /**
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to the target host
    */
  def setKVs(arr: Iterator[(String, String)], redisConfig: RedisConfig) {


    arr.map(kv => (redisConfig.findHost(kv._1), kv)).toArray.groupBy(_._1).
      mapValues(a => a.map(p => p._2)).foreach {
      x => {
        val jedis = new Jedis(x._1._1, x._1._2)
        val pipeline = jedis.pipelined
        x._2.foreach(x => pipeline.set(x._1, x._2))
        pipeline.sync
        jedis.close
      }
    }
  }


  /**
    * @param key
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to hashName(list type) to the target host
    */
  def setHash(key: String, arr: Iterator[(String, String)], redisConfig: RedisConfig) {


    val jedis = redisConfig.connecttionForKey(key)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.hset(key, x._1, x._2))
    pipeline.sync
    jedis.close
  }

  /**
    * @param key
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to zsetName(zset type) to the target host
    */
  def setZset(key: String, arr: Iterator[(String, String)], redisConfig: RedisConfig) {

    val jedis = redisConfig.connecttionForKey(key)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.zadd(key, x._2.toDouble, x._1))
    pipeline.sync
    jedis.close
  }

  /**
    * @param key
    * @param arr values which should be saved in the target host
    *            save all the values to setName(set type) to the target host
    */
  def setSet(key: String, arr: Iterator[String], redisConfig: RedisConfig) {


    val jedis = redisConfig.connecttionForKey(key)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.sadd(key, _))
    pipeline.sync
    jedis.close
  }

  /**
    * @param listName
    * @param arr values which should be saved in the target host
    *            save all the values to listName(list type) to the target host
    */
  def setList(listName: String, arr: Iterator[String], redisConfig: RedisConfig) {

    val jedis = redisConfig.connecttionForKey(listName)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.rpush(listName, _))
    pipeline.sync
    jedis.close
  }

  /**
    * @param key
    * @param listSize
    * @param arr values which should be saved in the target host
    *            save all the values to listName(list type) to the target host
    */
  def setFixedList(key: String, listSize: Int, arr: Iterator[String],
                   redisConfig: RedisConfig) {

    val jedis = redisConfig.connecttionForKey(key)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.lpush(key, _))
    if (listSize > 0) {
      pipeline.ltrim(key, 0, listSize - 1)
    }
    pipeline.sync
    jedis.close
  }


}

trait RedisFunctions {
  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}

