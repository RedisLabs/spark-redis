package com.redislab.provider.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{ HostAndPort, Jedis, JedisCluster }
import redis.clients.util.{ SafeEncoder, JedisClusterCRC16 }
import scala.collection.JavaConversions._
import com.redislab.provider.redis.rdd._
//import com.redislab.provider.redis.SaveToRedis._
import com.redislab.provider.redis.NodesInfo._

class RedisContext(val sc: SparkContext) extends Serializable {
  
  def fromRRDD(initialHost: (String, Int),
               keys: RDD[String]) = {
    new RRDD(sc, initialHost, keys);
  }
  
  def fromRedisKeyPattern(initialHost: (String, Int),
                          keyPattern: String = "*") = {
    new RedisListRDD(sc, initialHost, keyPattern, "keypattern");
  }
  
  def fromRedisKV(initialHost: (String, Int),
                  keyPattern: String = "*") = {
    new RedisKVRDD(sc, initialHost, keyPattern, "kv");
  }
  def fromRedisHASH(initialHost: (String, Int),
                    keyPattern: String = "*") = {
    new RedisKVRDD(sc, initialHost, keyPattern, "hash");
  }
  def fromRedisZSET(initialHost: (String, Int),
                    keyPattern: String = "*") = {
    new RedisKVRDD(sc, initialHost, keyPattern, "zset");
  }
  
  def fromRedisSET(initialHost: (String, Int),
                   keyPattern: String = "*") = {
    new RedisListRDD(sc, initialHost, keyPattern, "set");
  }
  def fromRedisLIST(initialHost: (String, Int),
                    keyPattern: String = "*") = {
    new RedisListRDD(sc, initialHost, keyPattern, "list");
  }
  
  /*
  def toRedisKV(kvs: RDD[(String, String)],
                initialHost: (String, Int)) = {
    val hosts = getHosts((initialHost._1, initialHost._2))
    val ckv = kvs.map(x => (getIndex(hosts, x._1), (x._1, x._2))).groupByKey
    ckv.foreach {
      x => setKVs(x._1, hosts, x._2)
    }
  }
  
  def toRedisHASH(kvs: RDD[(String, String)],
                  hashName: String,
                  initialHost: (String, Int)) = {
    val hosts = getHosts((initialHost._1, initialHost._2))
    val index = getIndex(hosts, hashName)
    setHash(index, hosts, hashName, kvs.collect)
  }
  def toRedisZSET(kvs: RDD[(String, String)],
                  zsetName: String,
                  initialHost: (String, Int)) = {
    val hosts = getHosts((initialHost._1, initialHost._2))
    val index = getIndex(hosts, zsetName)
    setZset(index, hosts, zsetName, kvs.collect)
  }
  def toRedisSET(vs: RDD[String],
                 setName: String,
                 initialHost: (String, Int)) = {
    val hosts = getHosts((initialHost._1, initialHost._2))
    val index = getIndex(hosts, setName)
    setSet(index, hosts, setName, vs.collect)
  }
  def toRedisLIST(vs: RDD[String],
                  listName: String,
                  initialHost: (String, Int)) = {
    val hosts = getHosts((initialHost._1, initialHost._2))
    val index = getIndex(hosts, listName)
    setList(index, hosts, listName, vs.collect)
  }
  */
}

object NodesInfo {
  def getHosts(initialHost: (String, Int)) = {
    val j = new Jedis(initialHost._1, initialHost._2)
    val hosts = j.clusterSlots().asInstanceOf[java.util.List[java.lang.Object]].map {
      slotInfoObj =>
        {
          val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
          val sPos = slotInfo.get(0).toString.toInt
          val ePos = slotInfo.get(1).toString.toInt
          val hostInfos = slotInfo.get(2).asInstanceOf[java.util.List[java.lang.Object]]
          val hp = new HostAndPort(SafeEncoder.encode(hostInfos.get(0).asInstanceOf[Array[scala.Byte]]),
            hostInfos.get(1).toString.toInt);
          (hp, sPos, ePos)
        }
    }.groupBy(_._1).map {
      x =>
        {
          val groups = x._2;
          val slots = new java.util.HashSet[Int]()
          for (grou <- groups) {
            val sPos = grou._2
            val ePos = grou._3
            for (i <- (sPos to ePos))
              slots.add(i)
          }
          (x._1.getHost, x._1.getPort, slots)
        }
    }.toArray
    j.close()
    hosts
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
            var node = slotInfo(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
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

/*
object SaveToRedis {
  def getIndex(hosts: Array[(String, Int, java.util.HashSet[Int])], key: String) = {
    val slot = JedisClusterCRC16.getSlot(key);
    var index = 0
    while (!(hosts(index)._3.contains(slot))) index += 1;
    index
  }
  def setKVs(index: Int, hosts: Array[(String, Int, java.util.HashSet[Int])], arr: Iterable[(String, String)]) = {
    val jedis = new Jedis(hosts(index)._1, hosts(index)._2)
    arr.foreach(x => jedis.set(x._1, x._2))
  }
  def setHash(index: Int, hosts: Array[(String, Int, java.util.HashSet[Int])], hashName: String, arr: Array[(String, String)]) = {
    val jedis = new Jedis(hosts(index)._1, hosts(index)._2)
    arr.foreach(x => jedis.hset(hashName, x._1, x._2))
  }
  def setZset(index: Int, hosts: Array[(String, Int, java.util.HashSet[Int])], zsetName: String, arr: Array[(String, String)]) = {
    val jedis = new Jedis(hosts(index)._1, hosts(index)._2)
    arr.foreach(x => jedis.zadd(zsetName, x._2.toDouble, x._1))
  }
  def setSet(index: Int, hosts: Array[(String, Int, java.util.HashSet[Int])], setName: String, arr: Array[String]) = {
    val jedis = new Jedis(hosts(index)._1, hosts(index)._2)
    arr.foreach(jedis.sadd(setName, _))
  }
  def setList(index: Int, hosts: Array[(String, Int, java.util.HashSet[Int])], listName: String, arr: Array[String]) = {
    val jedis = new Jedis(hosts(index)._1, hosts(index)._2)
    arr.foreach(jedis.rpush(listName, _))
  }
}
*/

trait RedisFunctions {
  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}

