package com.redislab.provider.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{ HostAndPort, Jedis, JedisCluster }
import redis.clients.util.{ SafeEncoder, JedisClusterCRC16 }
import scala.collection.JavaConversions._
import com.redislab.provider.redis.rdd._
import com.redislab.provider.redis.SaveToRedis._
import com.redislab.provider.redis.NodesInfo._

class RedisContext(val sc: SparkContext) extends Serializable {
  
  def fromRedisKeyPattern(initialHost: (String, Int),
                          keyPattern: String = "*",
                          partitionNum: Int = 3) = {
    new RedisKeysRDD(sc, initialHost, keyPattern, partitionNum);
  }
  
  def toRedisKV(kvs: RDD[(String, String)],
                initialHost: (String, Int)) = {
    val hosts = getHosts(initialHost)
    kvs.map(kv => (findHost(hosts, kv._1), kv)).groupByKey.foreach(
        x => setKVs((x._1._1, x._1._2), x._2)
    )
  }
  def toRedisHASH(kvs: RDD[(String, String)],
                  hashName: String,
                  initialHost: (String, Int)) = {
    val host = getHost(hashName, initialHost)
    kvs.foreachPartition(partition => setHash(host, hashName, partition))
  }
  def toRedisZSET(kvs: RDD[(String, String)],
                  zsetName: String,
                  initialHost: (String, Int)) = {
    val host = getHost(zsetName, initialHost)
    kvs.foreachPartition(partition => setZset(host, zsetName, partition))
  }
  def toRedisSET(vs: RDD[String],
                 setName: String,
                 initialHost: (String, Int)) = {
    val host = getHost(setName, initialHost)
    vs.foreachPartition(partition => setSet(host, setName, partition))
  }
  def toRedisLIST(vs: RDD[String],
                  listName: String,
                  initialHost: (String, Int)) = {
    val host = getHost(listName, initialHost)
    vs.foreachPartition(partition => setList(host, listName, partition))
  }
}

object NodesInfo {
  def findHost(hosts: Array[(String, Int, Int, Int)], key: String) = {
      val slot = JedisClusterCRC16.getSlot(key)
      hosts.filter(host => {host._3 <= slot && host._4 >= slot})(0) 
  }
  def getHost(key: String, initialHost: (String, Int)) = {
    val slot = JedisClusterCRC16.getSlot(key);
    val hosts = getSlots(initialHost).filter(x => (x._3 == 0 && x._5 <= slot && x._6 >= slot)).map(x => (x._1, x._2))
    hosts(0)
  }
  def getHosts(initialHost: (String, Int)) = {
    getSlots(initialHost).filter(_._3 == 0).map(x => (x._1, x._2, x._5, x._6))
  }
  def getSlots(initialHost: (String, Int)) = {
    val j = new Jedis(initialHost._1, initialHost._2)
    j.clusterSlots().asInstanceOf[java.util.List[java.lang.Object]].flatMap {
      slotInfoObj =>
        {
          val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
          val sPos = slotInfo.get(0).toString.toInt
          val ePos = slotInfo.get(1).toString.toInt
          (0 until (slotInfo.size - 2)).map(i => {
            val node = slotInfo(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
            (SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]]),
             node.get(1).toString.toInt,
             i,
             slotInfo.size - 2,
             sPos,
             ePos)
          })
        }
    }.toArray
  }
  def getNodes(initialHost: (String, Int)) = {
    val j = new Jedis(initialHost._1, initialHost._2)
    j.clusterSlots().asInstanceOf[java.util.List[java.lang.Object]].flatMap {
      slotInfoObj =>
        {
          val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]].drop(2)
          val range = slotInfo.size
          (0 until range).map(i => {
            var node = slotInfo(i).asInstanceOf[java.util.List[java.lang.Object]]
            (SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]]),
             node.get(1).toString.toInt,
             i,
             range)
          })
        }
    }.distinct.toArray
  }
}

object SaveToRedis {
  def setKVs(host: (String, Int), arr: Iterable[(String, String)]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.set(x._1, x._2))
    pipeline.sync
  }
  def setHash(host: (String, Int), hashName: String, arr: Iterator[(String, String)]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.hset(hashName, x._1, x._2))
    pipeline.sync
  }
  def setZset(host: (String, Int), zsetName: String, arr: Iterator[(String, String)]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.zadd(zsetName, x._2.toDouble, x._1))
    pipeline.sync
  }
  def setSet(host: (String, Int), setName: String, arr: Iterator[String]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.sadd(setName, _))
    pipeline.sync
  }
  def setList(host: (String, Int), listName: String, arr: Iterator[String]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.rpush(listName, _))
    pipeline.sync
  }
}

trait RedisFunctions {
  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}

