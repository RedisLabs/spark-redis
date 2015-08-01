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
                 val redisHosts: Array[(String, Int, util.HashSet[Int])],
                 val keyPattern: String,
                 val rddType: String,
                 val makePartitioner: Boolean)
    extends RDD[(String, String)](sc, Seq.empty) with Logging {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].addr._1.getHostName)
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until redisHosts.size).map(i => {
      new RedisPartition(i, (InetAddress.getByName(redisHosts(i)._1), redisHosts(i)._2), redisHosts(i)._3).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val addr = partition.addr
    val jedis = new Jedis(addr._1.getHostAddress, addr._2)
    val keys = getKeys(jedis, keyPattern)
    rddType match {
      case "kv"   => keys.filter(k => jedis.`type`(k) == "string").map(k => (k, jedis.get(k))).iterator;
      case "hash" => keys.filter(k => jedis.`type`(k) == "hash").flatMap(k => jedis.hgetAll(k)).iterator;
      case "zset" => keys.filter(k => jedis.`type`(k) == "zset").flatMap(k => jedis.zrangeWithScores(k, 0, -1)).map(tup => (tup.getElement, tup.getScore.toString)).iterator;
      case _      => Seq().iterator;
    }
  }
  def save(key: String, tarType: String = rddType): Unit = {
    println(key)
    println(tarType)
    val jc = new JedisCluster(Set(new HostAndPort(redisHosts(0)._1, redisHosts(0)._2)), 5)    
    tarType match {
      case "hash" => toLocalIterator.foreach{
        x => {
        val xx = x.asInstanceOf[(String, String)];
        jc.hset(key, xx._1, xx._2)
      }};
      case "zset" => toLocalIterator.foreach{
        x => {
        val xx = x.asInstanceOf[(String, String)];
        jc.zadd(key, xx._2.toDouble, xx._1)
      }};
      case _      => Seq().iterator;
    }
    
  }

  def getKeys(jedis: Jedis, keyPattern: String) = {
    val params = new ScanParams().`match`(keyPattern)
    val keys = new util.HashSet[String]()
    var scan = jedis.scan("0", params)
    val f = scan.getResult
    keys.addAll(f)
    while (scan.getStringCursor != "0") {
      scan = jedis.scan(scan.getStringCursor, params)
      val f1 = scan.getResult
      keys.addAll(f1)
    }
    keys
  }
}

class RedisListRDD(sc: SparkContext,
                   val redisHosts: Array[(String, Int, util.HashSet[Int])],
                   val keyPattern: String,
                   val rddType: String,
                   val makePartitioner: Boolean)
    extends RDD[String](sc, Seq.empty) with Logging {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].addr._1.getHostName)
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until redisHosts.size).map(i => {
      new RedisPartition(i, (InetAddress.getByName(redisHosts(i)._1), redisHosts(i)._2), redisHosts(i)._3).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val addr = partition.addr
    val jedis = new Jedis(addr._1.getHostAddress, addr._2)
    val keys = getKeys(jedis, keyPattern)
    rddType match {
      case "set"  => keys.filter(k => jedis.`type`(k) == "set").flatMap(k => jedis.smembers(k)).iterator;
      case "list" => keys.filter(k => jedis.`type`(k) == "list").flatMap(k => jedis.lrange(k, 0, -1)).iterator;
      case _      => Seq().iterator;
    }
  }

  def save(key: String, tarType: String = rddType): Unit = {
    val jc = new JedisCluster(Set(new HostAndPort(redisHosts(0)._1, redisHosts(0)._2)), 5)
    tarType match {
      case "set"  => toLocalIterator.foreach(jc.sadd(key, _));
      case "list" => toLocalIterator.foreach(jc.rpush(key, _));
      case _      => Seq().iterator;
    }
  }

  def getKeys(jedis: Jedis, keyPattern: String) = {
    val params = new ScanParams().`match`(keyPattern)
    val keys = new util.HashSet[String]()
    var scan = jedis.scan("0", params)
    val f = scan.getResult
    keys.addAll(f)
    while (scan.getStringCursor != "0") {
      scan = jedis.scan(scan.getStringCursor, params)
      val f1 = scan.getResult
      keys.addAll(f1)
    }
    keys
  }
}