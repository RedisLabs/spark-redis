package com.redislab.provider.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{ HostAndPort, Jedis }
import redis.clients.util.SafeEncoder
import scala.collection.JavaConversions._
import com.redislab.provider.redis.rdd._

class RedisContext(val sc: SparkContext) extends Serializable {

  def getHosts(initialHost: (String, Int)) = {
    val j = new Jedis(initialHost._1, initialHost._2);
    val hosts = j.clusterSlots().asInstanceOf[java.util.List[java.lang.Object]].map {
      slotInfoObj =>
        {
          val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
          val sPos = slotInfo.get(0).toString.toInt
          val ePos = slotInfo.get(1).toString.toInt
          val hostInfos = slotInfo.get(2).asInstanceOf[java.util.List[java.lang.Object]]
          val hp = new HostAndPort(SafeEncoder.encode(hostInfos.get(0).asInstanceOf[Array[scala.Byte]]), hostInfos.get(1).toString.toInt);
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

  def fromRedisKV(initialHost: (String, Int),
                  keyPattern: String = "*",
                  makePartitioner: Boolean = true) = {
    new RedisKVRDD(sc, getHosts(initialHost), keyPattern, "kv", false);
  }
  def fromRedisHASH(initialHost: (String, Int),
                    keyPattern: String = "*",
                    makePartitioner: Boolean = true) = {
    new RedisKVRDD(sc, getHosts(initialHost), keyPattern, "hash", false);
  }
  def fromRedisZSET(initialHost: (String, Int),
                    keyPattern: String = "*",
                    makePartitioner: Boolean = true) = {
    new RedisKVRDD(sc, getHosts(initialHost), keyPattern, "zset", false);
  }
  def fromRedisSET(initialHost: (String, Int),
                   keyPattern: String = "*",
                   makePartitioner: Boolean = true) = {
    new RedisListRDD(sc, getHosts(initialHost), keyPattern, "set", false);
  }
  def fromRedisLIST(initialHost: (String, Int),
                    keyPattern: String = "*",
                    makePartitioner: Boolean = true) = {
    new RedisListRDD(sc, getHosts(initialHost), keyPattern, "list", false);
  }
}

trait RedisFunctions {

  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)

}
