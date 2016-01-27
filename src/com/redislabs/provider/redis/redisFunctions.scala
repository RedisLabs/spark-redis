package com.redislabs.provider.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.util.{SafeEncoder, JedisClusterCRC16}
import scala.collection.JavaConversions._
import com.redislabs.provider.redis.rdd._
import com.redislabs.provider.redis.SaveToRedis._
import com.redislabs.provider.redis.NodesInfo._

class RedisContext(val sc: SparkContext) extends Serializable {

  /**
   * @param initialHost any node of a cluster or a single server
   * @param keyPattern
   * @param partitionNum number of partitions
   * @return RedisKeysRDD of simple Keys stored in redis server
   */
  def fromRedisKeyPattern(initialHost: RedisConnectionParameters,
                          keyPattern: String = "*",
                          partitionNum: Int = 3) = {
    new RedisKeysRDD(sc, initialHost, keyPattern, partitionNum)
  }

  /**
   * @param kvs Pair RDD of K/V
   * @param initialHost any node of a cluster or a single server
   *                    save all the kvs to redis-server
   */
  def toRedisKV(kvs: RDD[(String, String)],
                initialHost: RedisConnectionParameters) = {
    kvs.foreachPartition(partition => setKVs(initialHost, partition))
  }

  /**
   * @param kvs Pair RDD of K/V
   * @param hashName target hash's name which hold all the kvs
   * @param initialHost any node of a cluster or a single server
   *                    save all the kvs to hashName(hash type) in redis-server
   */
  def toRedisHASH(kvs: RDD[(String, String)],
                  hashName: String,
                  initialHost: RedisConnectionParameters) = {
    val host = getHost(hashName, initialHost)
    kvs.foreachPartition(partition => setHash(host, hashName, partition))
  }

  /**
   * @param kvs Pair RDD of K/V
   * @param zsetName target zset's name which hold all the kvs
   * @param initialHost any addr and port of a cluster or a single server
   *                    save all the kvs to zsetName(zset type) in redis-server
   */
  def toRedisZSET(kvs: RDD[(String, String)],
                  zsetName: String,
                  initialHost: RedisConnectionParameters) = {
    val host = getHost(zsetName, initialHost)
    kvs.foreachPartition(partition => setZset(host, zsetName, partition))
  }

  /**
   * @param vs RDD of values
   * @param setName target set's name which hold all the vs
   * @param initialHost any node of a cluster or a single server
   *                    save all the vs to setName(set type) in redis-server
   */
  def toRedisSET(vs: RDD[String],
                 setName: String,
                 initialHost: RedisConnectionParameters) = {
    val host = getHost(setName, initialHost)
    vs.foreachPartition(partition => setSet(host, setName, partition))
  }

  /**
   * @param vs RDD of values
   * @param listName target list's name which hold all the vs
   * @param initialHost any node of a cluster or a single server
   *                    save all the vs to listName(list type) in redis-server
   */
  def toRedisLIST(vs: RDD[String],
                  listName: String,
                  initialHost: RedisConnectionParameters) = {
    val host = getHost(listName, initialHost)
    vs.foreachPartition(partition => setList(host, listName, partition))
  }

  /**
   * @param vs RDD of values
   * @param listName target list's name which hold all the vs
   * @param initialHost any node of a cluster or a single server
   * @param listSize target list's size
   *                 save all the vs to listName(list type) in redis-server
   */
  def toRedisFixedLIST(vs: RDD[String],
                       listName: String,
                       initialHost: RedisConnectionParameters,
                       listSize: Int = 0) = {
    val host = getHost(listName, initialHost)
    vs.foreachPartition(partition => setFixedList(host, listName, listSize, partition))
  }

}

object NodesInfo {

  /**
   * @param initialHost any node of a cluster or a single server
   * @return true if the target server is in cluster mode
   */
  private def clusterEnable(initialHost: RedisConnectionParameters): Boolean = {
    val jedis = JedisFactory.instance.create(initialHost)
    val res = jedis.info("cluster").contains("1")
    jedis.close
    res
  }

  /**
   * @param hosts list of hosts(RedisConnectionParameters, startSlot, endSlot)
   * @param key
   * @return host whose slots should involve key
   */
  def findHost(hosts: Array[(RedisConnectionParameters, Int, Int)], key: String) = {
    val slot = JedisClusterCRC16.getSlot(key)
    hosts.filter(host => {
      host._2 <= slot && host._3 >= slot
    })(0)
  }

  /**
   * @param key
   * @param redisConnectionParameters any node of a cluster or a single server
   * @return host whose slots should involve key
   */
  def getHost(key: String, redisConnectionParameters: RedisConnectionParameters) = {
    val slot = JedisClusterCRC16.getSlot(key)
    val hosts = getSlots(redisConnectionParameters).filter(x => x._2 == 0 && x._4 <= slot && x._5 >= slot).map(x => x._1)
    hosts(0)
  }

  /**
   * @param initialHost any node of a cluster or a single server
   * @return list of hosts(RedisConnectionParameters, startSlot, endSlot)
   */
  def getHosts(initialHost: RedisConnectionParameters) = {
    getSlots(initialHost).filter(_._2 == 0).map(x => (x._1, x._4, x._5))
  }

  /**
   * @param initialHost any node of a single server
   * @return list of nodes(RedisConnectionParameters, index, range, startSlot, endSlot)
   */
  private def getNonClusterSlots(initialHost: RedisConnectionParameters) = {
    getNonClusterNodes(initialHost).map(x => (x._1, x._2, x._3, 0, 16383)).toArray
  }

  /**
   * @param initialHost any node of a cluster server
   * @return list of nodes(RedisConnectionParameters, index, range, startSlot, endSlot)
   */
  private def getClusterSlots(initialHost: RedisConnectionParameters) = {
    val jedis = JedisFactory.instance.create(initialHost)
    val res = jedis.clusterSlots().flatMap {
      slotInfoObj => {
        val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
        val sPos = slotInfo.get(0).toString.toInt
        val ePos = slotInfo.get(1).toString.toInt
        (0 until (slotInfo.size - 2)).map(i => {
          val node = slotInfo(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
          (new RedisConnectionParameters(
            SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]]),
            node.get(1).toString.toInt,
            initialHost.password),
            i,
            slotInfo.size - 2,
            sPos,
            ePos)
        })
      }
    }.toArray
    jedis.close()
    res
  }

  /**
   * @param initialHost any node of a cluster or a single server
   * @return list of nodes(RedisConnectionParameters, index, range, startSlot, endSlot)
   */
  def getSlots(initialHost: RedisConnectionParameters) = {
    if (clusterEnable(initialHost))
      getClusterSlots(initialHost)
    else
      getNonClusterSlots(initialHost)
  }


  /**
   * @param initialHost any node of a single server
   * @return list of nodes(RedisConnectionParameters, index, range)
   */
  private def getNonClusterNodes(initialHost: RedisConnectionParameters) = {
    var master = initialHost
    val jedis = JedisFactory.instance.create(initialHost)
    var replinfo = jedis.info("Replication").split("\n")
    jedis.close
    if (replinfo.exists(_.contains("role:slave"))) {
      val host = replinfo.filter(_.contains("master_host:"))(0).trim.substring(12)
      val port = replinfo.filter(_.contains("master_port:"))(0).trim.substring(12).toInt
      master = new RedisConnectionParameters(host, port, initialHost.password)
      val jedis = JedisFactory.instance.create(master)
      replinfo = jedis.info("Replication").split("\n")
      jedis.close
    }
    val slaves = replinfo.filter(x => x.contains("slave") && x.contains("online")).map(rl => {
      val content = rl.substring(rl.indexOf(':') + 1).split(",")
      val ip = content(0)
      val port = content(1)
      new RedisConnectionParameters(ip.substring(ip.indexOf('=') + 1),
        port.substring(port.indexOf('=') + 1).toInt,
        initialHost.password)
    })
    val nodes = master +: slaves
    val range = nodes.size
    (0 until range).map(i => (nodes(i), i, range)).toArray
  }

  /**
   * @param initialHost any node of a cluster server
   * @return list of nodes(RedisConnectionParameters, index, range)
   */
  private def getClusterNodes(initialHost: RedisConnectionParameters) = {
    val jedis = JedisFactory.instance.create(initialHost)
    val res = jedis.clusterSlots().flatMap {
      slotInfoObj => {
        val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]].drop(2)
        val range = slotInfo.size
        (0 until range).map(i => {
          val node = slotInfo(i).asInstanceOf[java.util.List[java.lang.Object]]
          (new RedisConnectionParameters(SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]]),
            node.get(1).toString.toInt,
            initialHost.password),
            i,
            range)
        })
      }
    }.distinct.toArray
    jedis.close()
    res
  }

  /**
   * @param initialHost any node of a cluster or a single server
   * @return list of nodes(RedisConnectionParameters, index, range)
   */
  def getNodes(initialHost: RedisConnectionParameters) = {
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
   *            save all the k/vs to the target host
   */
  def setKVs(host: RedisConnectionParameters, arr: Iterator[(String, String)]) = {
    val hosts = getHosts(host)
    arr.map(kv => (findHost(hosts, kv._1), kv)).toArray.groupBy(_._1).mapValues(a => a.map(p => p._2)).foreach {
      x => {
        val jedis = JedisFactory.instance.create(x._1._1)
        val pipeline = jedis.pipelined
        x._2.foreach(x => pipeline.set(x._1, x._2))
        pipeline.sync
      }
    }
  }

  /**
   * @param host addr and port of a target host
   * @param hashName
   * @param arr k/vs which should be saved in the target host
   *            save all the k/vs to hashName(list type) to the target host
   */
  def setHash(host: RedisConnectionParameters, hashName: String, arr: Iterator[(String, String)]) = {
    val jedis = JedisFactory.instance.create(host)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.hset(hashName, x._1, x._2))
    pipeline.sync
  }

  /**
   * @param host addr and port of a target host
   * @param zsetName
   * @param arr k/vs which should be saved in the target host
   *            save all the k/vs to zsetName(zset type) to the target host
   */
  def setZset(host: RedisConnectionParameters, zsetName: String, arr: Iterator[(String, String)]) = {
    val jedis = JedisFactory.instance.create(host)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.zadd(zsetName, x._2.toDouble, x._1))
    pipeline.sync
  }

  /**
   * @param host addr and port of a target host
   * @param setName
   * @param arr values which should be saved in the target host
   *            save all the values to setName(set type) to the target host
   */
  def setSet(host: RedisConnectionParameters, setName: String, arr: Iterator[String]) = {
    val jedis = JedisFactory.instance.create(host)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.sadd(setName, _))
    pipeline.sync
  }

  /**
   * @param host addr and port of a target host
   * @param listName
   * @param arr values which should be saved in the target host
   *            save all the values to listName(list type) to the target host
   */
  def setList(host: RedisConnectionParameters, listName: String, arr: Iterator[String]) = {
    val jedis = JedisFactory.instance.create(host)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.rpush(listName, _))
    pipeline.sync
  }

  /**
   * @param host addr and port of a target host
   * @param listName
   * @param listSize
   * @param arr values which should be saved in the target host
   *            save all the values to listName(list type) to the target host
   */
  def setFixedList(host: RedisConnectionParameters, listName: String, listSize: Int, arr: Iterator[String]) = {
    val jedis = JedisFactory.instance.create(host)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.lpush(listName, _))
    if (listSize > 0) {
      pipeline.ltrim(listName, 0, listSize - 1)
    }
    pipeline.sync
  }

}

trait RedisFunctions {
  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}

