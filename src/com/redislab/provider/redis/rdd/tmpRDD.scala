package com.redislab.provider.redis.rdd

import java.net.InetAddress
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark._
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._
import com.redislab.provider.redis.partitioner._

class RedisKVRDD(sc: SparkContext,
                 val redisNodes: Array[(String, Int, Int, Int)],
                 val keyPattern: String,
                 val rddType: String)
    extends RDD[(String, String)](sc, Seq.empty) with Logging with Keys{

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].node._1.getHostName)
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until redisNodes.size).map(i => {
      new RedisPartition(i, (InetAddress.getByName(redisNodes(i)._1), redisNodes(i)._2, redisNodes(i)._3, redisNodes(i)._4)).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val node = partition.node
    val jedis = new Jedis(node._1.getHostAddress, node._2)
    val index = node._3
    val range = node._4
    jedis.readonly()
    val keys = getKeys(jedis, keyPattern)
    rddType match {
      case "kv"   => keys.filter(k => (k.hashCode % range == index && jedis.`type`(k) == "string")).map(k => (k, jedis.get(k))).iterator;
      case "hash" => keys.filter(k => jedis.`type`(k) == "hash").flatMap(k => jedis.hgetAll(k)).iterator;
      case "zset" => keys.filter(k => jedis.`type`(k) == "zset").flatMap(k => jedis.zrangeWithScores(k, 0, -1)).map(tup => (tup.getElement, tup.getScore.toString)).iterator;
      case _      => Seq().iterator;
    }
  }
}

class RedisListRDD(sc: SparkContext,
                   val redisNodes: Array[(String, Int, Int, Int)],
                   val keyPattern: String,
                   val rddType: String)
    extends RDD[String](sc, Seq.empty) with Logging with Keys{

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].node._1.getHostName)
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until redisNodes.size).map(i => {
      new RedisPartition(i, (InetAddress.getByName(redisNodes(i)._1), redisNodes(i)._2, redisNodes(i)._3, redisNodes(i)._4)).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val node = partition.node
    val jedis = new Jedis(node._1.getHostAddress, node._2)
    val index = node._3
    val range = node._4
    val keys = getKeys(jedis, keyPattern)
    rddType match {
      case "set"  => keys.filter(k => jedis.`type`(k) == "set").flatMap(k => jedis.smembers(k)).iterator;
      case "list" => keys.filter(k => jedis.`type`(k) == "list").flatMap(k => jedis.lrange(k, 0, -1)).iterator;
      case _      => Seq().iterator;
    }
  }
}

trait Keys {
  private def isRedisRegex(key: String) = {
    def judge(key: String, escape: Boolean): Boolean = {
      if (key.length == 0)
        return false
      escape match {
        case true => judge(key.substring(1), false);
        case false => {
          key.charAt(0) match {
            case '*'  => true;
            case '?'  => true;
            case '['  => true;
            case '\\' => judge(key.substring(1), true);
            case _    => judge(key.substring(1), false);
          }
        }
      }
    }
    judge(key, false)
  }

  private def scanKeys(jedis: Jedis, params: ScanParams, cursor: String): util.ArrayList[String] = {
    def scankeys(jedis: Jedis, params: ScanParams, cursor: String, scanned: Boolean): util.ArrayList[String] = {
      val keys = new util.ArrayList[String]
      if (scanned && cursor == "0")
        return keys;
      val scan = jedis.scan(cursor, params)
      keys.addAll(scan.getResult)
      keys.addAll(scankeys(jedis, params, scan.getStringCursor, true))
      keys
    }
    scankeys(jedis, params, cursor, false)
  }

  def getKeys(jedis: Jedis, keyPattern: String) = {
    val keys = new util.ArrayList[String]()
    if (isRedisRegex(keyPattern)) {
      val params = new ScanParams().`match`(keyPattern)
      keys.addAll(scanKeys(jedis, params, "0"))
    }
    else {
      keys.add(keyPattern)
    }
    keys
  }
}
