package com.redislabs.provider.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis
import redis.clients.util.{SafeEncoder, JedisClusterCRC16}
import scala.collection.JavaConversions._
import com.redislabs.provider.redis.rdd._
import com.redislabs.provider.redis.SaveToRedis._
import com.redislabs.provider.redis.NodesInfo._

class RedisContext(val sc: SparkContext) extends Serializable {

  /**
   * @param initialHost any addr and port of a cluster or a single server
   * @param keyPattern
   * @param partitionNum number of partitions
   * @return RedisKeysRDD of simple Keys stored in redis server
   */
  def fromRedisKeyPattern(initialHost: (String, Int),
                          keyPattern: String = "*",
                          partitionNum: Int = 3) = {
    new RedisKeysRDD(sc, initialHost, keyPattern, partitionNum);
  }

  /**
   * @param kvs Pair RDD of K/V
   * @param initialHost any addr and port of a cluster or a single server
   * save all the kvs to redis-server
   */
  def toRedisKV(kvs: RDD[(String, String)],
                initialHost: (String, Int)) = {
    val hosts = getHosts(initialHost)
    kvs.map(kv => (findHost(hosts, kv._1), kv)).groupByKey.foreach(
        x => setKVs((x._1._1, x._1._2), x._2)
    )
  }
  /**
   * @param kvs Pair RDD of K/V
   * @param hashName target hash's name which hold all the kvs
   * @param initialHost any addr and port of a cluster or a single server
   * save all the kvs to hashName(hash type) in redis-server
   */
  def toRedisHASH(kvs: RDD[(String, String)],
                  hashName: String,
                  initialHost: (String, Int)) = {
    val host = getHost(hashName, initialHost)
    kvs.foreachPartition(partition => setHash(host, hashName, partition))
  }
  /**
   * @param kvs Pair RDD of K/V
   * @param zsetName target zset's name which hold all the kvs
   * @param initialHost any addr and port of a cluster or a single server
   * save all the kvs to zsetName(zset type) in redis-server
   */
  def toRedisZSET(kvs: RDD[(String, String)],
                  zsetName: String,
                  initialHost: (String, Int)) = {
    val host = getHost(zsetName, initialHost)
    kvs.foreachPartition(partition => setZset(host, zsetName, partition))
  }
  /**
   * @param vs RDD of values
   * @param setName target set's name which hold all the vs
   * @param initialHost any addr and port of a cluster or a single server
   * save all the vs to setName(set type) in redis-server
   */
  def toRedisSET(vs: RDD[String],
                 setName: String,
                 initialHost: (String, Int)) = {
    val host = getHost(setName, initialHost)
    vs.foreachPartition(partition => setSet(host, setName, partition))
  }
  /**
   * @param vs RDD of values
   * @param listName target list's name which hold all the vs
   * @param initialHost any addr and port of a cluster or a single server
   * save all the vs to listName(list type) in redis-server
   */
  def toRedisLIST(vs: RDD[String],
                  listName: String,
                  initialHost: (String, Int)) = {
    val host = getHost(listName, initialHost)
    vs.foreachPartition(partition => setList(host, listName, partition))
  }
}

object NodesInfo {

  /**
   * @param initialHost any addr and port of a cluster or a single server
   * @return true if the target server is in cluster mode
   */
  private def clusterEnable(initialHost: (String, Int)) : Boolean = {
    new Jedis(initialHost._1, initialHost._2).info("cluster").contains("1")
  }

  /**
   * @param hosts list of hosts(addr, port, startSlot, endSlot)
   * @param key
   * @return host whose slots should involve key
   */
  def findHost(hosts: Array[(String, Int, Int, Int)], key: String) = {
      val slot = JedisClusterCRC16.getSlot(key)
      hosts.filter(host => {host._3 <= slot && host._4 >= slot})(0)
  }
  /**
   * @param key
   * @param initialHost any addr and port of a cluster or a single server
   * @return host whose slots should involve key
   */
  def getHost(key: String, initialHost: (String, Int)) = {
    val slot = JedisClusterCRC16.getSlot(key);
    val hosts = getSlots(initialHost).filter(x => (x._3 == 0 && x._5 <= slot && x._6 >= slot)).map(x => (x._1, x._2))
    hosts(0)
  }
  /**
   * @param initialHost any addr and port of a cluster or a single server
   * @return list of hosts(addr, port, startSlot, endSlot)
   */
  def getHosts(initialHost: (String, Int)) = {
    getSlots(initialHost).filter(_._3 == 0).map(x => (x._1, x._2, x._5, x._6))
  }

  /**
   * @param initialHost any addr and port of a single server
   * @return list of nodes(addr, port, index, range, startSlot, endSlot)
   */
  private def getNonClusterSlots(initialHost: (String, Int)) = {
    getNonClusterNodes(initialHost).map(x=> (x._1, x._2, x._3, x._4, 0, 16383)).toArray
  }
  /**
   * @param initialHost any addr and port of a cluster server
   * @return list of nodes(addr, port, index, range, startSlot, endSlot)
   */
  private def getClusterSlots(initialHost: (String, Int)) = {
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
  /**
   * @param initialHost any addr and port of a cluster or a single server
   * @return list of nodes(addr, port, index, range, startSlot, endSlot)
   */
  def getSlots(initialHost: (String, Int)) = {
    if (clusterEnable(initialHost))
      getClusterSlots(initialHost)
    else
      getNonClusterSlots(initialHost)
  }


  /**
   * @param initialHost any addr and port of a single server
   * @return list of nodes(addr, port, index, range)
   */
  private def getNonClusterNodes(initialHost: (String, Int)) = {
    var master = initialHost
    var replinfo = new Jedis(initialHost._1, initialHost._2).info("Replication").split("\n")
    if (replinfo.filter(_.contains("role:slave")).length != 0){
      val host = replinfo.filter(_.contains("master_host:"))(0).trim.substring(12)
      val port = replinfo.filter(_.contains("master_port:"))(0).trim.substring(12).toInt
      master = (host, port)
      val j = new Jedis(host, port)
      replinfo = j.info("Replication").split("\n")
    }
    val slaves = replinfo.filter(x => (x.contains("slave") && x.contains("online"))).map(rl => {
      val content = rl.substring(rl.indexOf(':') + 1).split(",")
      val ip = content(0)
      val port = content(1)
      (ip.substring(ip.indexOf('=')+1).toString, port.substring(port.indexOf('=')+1).toInt)
    })
    val nodes = master +: slaves
    val range = nodes.size
    (0 until range).map(i => (nodes(i)._1, nodes(i)._2, i, range)).toArray
  }
  /**
   * @param initialHost any addr and port of a cluster server
   * @return list of nodes(addr, port, index, range)
   */
  private def getClusterNodes(initialHost: (String, Int)) = {
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

  /**
   * @param initialHost any addr and port of a cluster or a single server
   * @return list of nodes(addr, port, index, range)
   */
  def getNodes(initialHost: (String, Int)) = {
    if (clusterEnable(initialHost))
      getClusterNodes(initialHost)
    else
      getNonClusterNodes(initialHost)
  }
}

object SaveToRedis {
  /**
   * @param host addr and port of a target host
   * @param arr k/vs which should be saved in the target host
   * save all the k/vs to the target host
   */
  def setKVs(host: (String, Int), arr: Iterable[(String, String)]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.set(x._1, x._2))
    pipeline.sync
  }
  /**
   * @param host addr and port of a target host
   * @param hashName
   * @param arr k/vs which should be saved in the target host
   * save all the k/vs to hashName(list type) to the target host
   */
  def setHash(host: (String, Int), hashName: String, arr: Iterator[(String, String)]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.hset(hashName, x._1, x._2))
    pipeline.sync
  }
  /**
   * @param host addr and port of a target host
   * @param zsetName
   * @param arr k/vs which should be saved in the target host
   * save all the k/vs to zsetName(zset type) to the target host
   */
  def setZset(host: (String, Int), zsetName: String, arr: Iterator[(String, String)]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.zadd(zsetName, x._2.toDouble, x._1))
    pipeline.sync
  }
  /**
   * @param host addr and port of a target host
   * @param setName
   * @param arr values which should be saved in the target host
   * save all the values to setName(set type) to the target host
   */
  def setSet(host: (String, Int), setName: String, arr: Iterator[String]) = {
    val jedis = new Jedis(host._1, host._2)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.sadd(setName, _))
    pipeline.sync
  }
  /**
   * @param host addr and port of a target host
   * @param listName
   * @param arr values which should be saved in the target host
   * save all the values to listName(list type) to the target host
   */
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

