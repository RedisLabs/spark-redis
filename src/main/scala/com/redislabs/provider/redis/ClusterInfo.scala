package com.redislabs.provider.redis

import redis.clients.jedis.Jedis
import redis.clients.util.{SafeEncoder, JedisClusterCRC16}
import scala.collection.JavaConversions._



class Endpoint(host: String, port: Int)  extends Serializable
/**
  * ClusterInfo holds the state of the cluster nodes, and uses consistent hashing to map
  * keys to nodes
  */
class ClusterInfo(initialHost: (String, Int)) extends  Serializable {


  val hosts = getHosts(initialHost)
  val slots = getSlots(initialHost)


  def getRandomNode() : (String, Int, Int, Int) = {

    val rnd = scala.util.Random.nextInt().abs % hosts.length
    hosts(rnd)
  }
  /**
    * @param initialHost any addr and port of a cluster or a single server
    * @return true if the target server is in cluster mode
    */
  private def clusterEnabled(initialHost: (String, Int)): Boolean = {
    val jedis = new Jedis(initialHost._1, initialHost._2)
    val res = jedis.info("cluster").contains("1")
    jedis.close
    res
  }

  /**
    * @param key
    * @return host whose slots should involve key
    */
  def findHost(key: String): (String, Int, Int, Int) = {
    val slot = JedisClusterCRC16.getSlot(key)
    hosts.filter(host => {
      host._3 <= slot && host._4 >= slot
    })(0)
  }

  /**
    * @param key
    * @return host whose slots should involve key
    */
  def getHost(key: String): (String, Int) = {
    val slot = JedisClusterCRC16.getSlot(key)
    val hosts = slots.filter(x => x._3 == 0 && x._5 <= slot && x._6 >= slot).
      map(x => (x._1, x._2))
    hosts(0)
  }

  /**
    * @param initialHost any addr and port of a cluster or a single server
    * @return list of hosts(addr, port, startSlot, endSlot)
    */
  private def getHosts(initialHost: (String, Int)): Array[(String, Int, Int, Int)] = {
    getSlots(initialHost).filter(_._3 == 0).map(x => (x._1, x._2, x._5, x._6))
  }

  /**
    * @param initialHost any addr and port of a single server
    * @return list of nodes(addr, port, index, range, startSlot, endSlot)
    */
  private def getNonClusterSlots(initialHost: (String, Int)):
  Array[(String, Int, Int, Int, Int, Int)] = {
    getNonClusterNodes(initialHost).map(x => (x._1, x._2, x._3, x._4, 0, 16383)).toArray
  }

  /**
    * @param initialHost any addr and port of a cluster server
    * @return list of nodes(addr, port, index, range, startSlot, endSlot)
    */
  private def getClusterSlots(initialHost: (String, Int)):
  Array[(String, Int, Int, Int, Int, Int)] = {
    val jedis = new Jedis(initialHost._1, initialHost._2)
    val res = jedis.clusterSlots().flatMap {
      slotInfoObj => {
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
    jedis.close()
    res
  }

  /**
    * @param initialHost any addr and port of a cluster or a single server
    * @return list of nodes(addr, port, index, range, startSlot, endSlot)
    */
  def getSlots(initialHost: (String, Int)): Array[(String, Int, Int, Int, Int, Int)] = {
    if (clusterEnabled(initialHost)) {
      getClusterSlots(initialHost)
    } else {
      getNonClusterSlots(initialHost)
    }
  }


  /** Get all the nodes from a non cluster setup, i.e. master/slaves or single master.
    * If the initial host is a master we return the node and its slaves. If it
    *
    * @param initialHost any addr and port of a single server
    * @return list of nodes(addr, port, index, range)
    */
  private def getNonClusterNodes(initialHost: (String, Int)): Array[(String, Int, Int, Int)] = {
    val master = initialHost
    val jedis = new Jedis(initialHost._1, initialHost._2)
    val replinfo = jedis.info("Replication").split("\n")
    jedis.close

    // If  this node is a slave, we need to extract the slaves from its master
    if (replinfo.filter(_.contains("role:slave")).length != 0) {
      val host = replinfo.filter(_.contains("master_host:"))(0).trim.substring(12)
      val port = replinfo.filter(_.contains("master_port:"))(0).trim.substring(12).toInt

      //simply re-enter this function witht he master host/port
      getNonClusterNodes(initialHost = (host, port))

    } else {
      //this is a master - take its slaves

      val slaves = replinfo.filter(x => (x.contains("slave") && x.contains("online"))).map(rl => {
        val content = rl.substring(rl.indexOf(':') + 1).split(",")
        val ip = content(0)
        val port = content(1)
        (ip.substring(ip.indexOf('=') + 1), port.substring(port.indexOf('=') + 1).toInt)
      })

      val nodes = master +: slaves
      val range = nodes.size
      (0 until range).map(i => (nodes(i)._1, nodes(i)._2, i, range)).toArray
    }
  }

  /**
    * @param initialHost any addr and port of a cluster server
    * @return list of nodes(addr, port, index, range)
    */
  private def getClusterNodes(initialHost: (String, Int)): Array[(String, Int, Int, Int)] = {
    val jedis = new Jedis(initialHost._1, initialHost._2)
    val res = jedis.clusterSlots().asInstanceOf[java.util.List[java.lang.Object]].flatMap {
      slotInfoObj => {
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
    jedis.close()
    res
  }

  /**
    * @param initialHost any addr and port of a cluster or a single server
    * @return list of nodes(addr, port, index, range)
    */
  private def getNodes(initialHost: (String, Int)): Array[(String, Int, Int, Int)] = {
    if (clusterEnabled(initialHost)) {
      getClusterNodes(initialHost)
    } else {
      getNonClusterNodes(initialHost)
    }
  }

}
