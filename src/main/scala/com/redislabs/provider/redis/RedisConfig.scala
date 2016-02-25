package com.redislabs.provider.redis

import java.net.URI

import org.apache.spark.SparkConf
import redis.clients.jedis.{JedisFactory, Jedis}
import redis.clients.util.{JedisURIHelper, SafeEncoder, JedisClusterCRC16}
import scala.collection.JavaConversions._


/**
  * RedisEndpoint represents a redis connection endpoint info: host, port, auth password and db number
  * @param host the redis host or ip
  * @param port the redis port
  * @param auth the authentication password
  * @param dbNum database number (should be avoided in general)
  */
class RedisEndpoint(val host: String, val port: Int, val auth: String = "", val dbNum: Int = 0 )
  extends Serializable {


  /**
    * Constructor from spark config. set params with redis.host, redis.port, redis.auth and redis.db
    * @param conf spark context config
    */
  def this(conf: SparkConf) {
      this(
        conf.get("redis.host", "localhost"),
        conf.get("redis.port", "6379").toInt,
        conf.get("redis.auth", ""),
        conf.get("redis.db", "0").toInt
      )
  }


  /**
    * Constructor with Jedis URI
    * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
    */

  def this(uri: URI) {
    this(uri.getHost, uri.getPort, JedisURIHelper.getPassword(uri), JedisURIHelper.getDBIndex(uri))
  }

  /**
    * Constructor with Jedis URI from String
    * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
    */
  def this(uri :String) {
    this(URI.create(uri))
  }


  /**
    * Connect tries to open a connection to the redis endpoint,
    * optionally authenticating and selecting a db
 *
    * @return a new Jedis instance
    */
  def connect():Jedis = {
    val client = new Jedis(this.host, this.port)

    // if a password was set - auth
    if (!Option(auth).getOrElse("").isEmpty) {
      client.auth(auth)
    }

    // if a db num was set, select it
    if (dbNum > 0) {
      client.select(dbNum)
    }

    client

  }
}

class Node

/**
  * RedisConfig holds the state of the cluster nodes, and uses consistent hashing to map
  * keys to nodes
  */
class RedisConfig(val initialHost: RedisEndpoint) extends  Serializable {


  val currentAddr = initialHost.host

  val hosts = getHosts(initialHost)
  val slots = getSlots(initialHost)

  def getAuth: String = {
    initialHost.auth
  }

  def getDB :Int = {
    initialHost.dbNum
  }

  def getRandomNode() : (String, Int, Int, Int) = {

    val rnd = scala.util.Random.nextInt().abs % hosts.length
    hosts(rnd)
  }

  def getNodesBySlots(sPos: Int, ePos: Int): Array[(String, Int, Int, Int, Int, Int)] = {
    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int) =
      if (sPos1 <= sPos2) ePos1 >= sPos2 else ePos2 >= sPos1

    val node = getRandomNode
    slots.filter(node => inter(sPos, ePos, node._5, node._6)).
      filter(_._3 == 0) //master only now
  }

  /** Get a jedis connection for a given key */
  def connecttionForKey(key: String): Jedis = {
    val host = getHost(key)
    host.connect
  }

  /**
    * @param initialHost any addr and port of a cluster or a single server
    * @return true if the target server is in cluster mode
    */
  private def clusterEnabled(initialHost: RedisEndpoint): Boolean = {
    val conn = initialHost.connect()
    val res = conn.info("cluster").contains("1")
    conn.close
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
  def getHost(key: String): RedisEndpoint = {
    val slot = JedisClusterCRC16.getSlot(key)
    val hosts = slots.filter(x => x._3 == 0 && x._5 <= slot && x._6 >= slot).
      map(x => (x._1, x._2))
    new RedisEndpoint(hosts(0)._1, hosts(0)._2, initialHost.auth, initialHost.dbNum)
  }

  /**
    * @param initialHost any addr and port of a cluster or a single server
    * @return list of hosts(addr, port, startSlot, endSlot)
    */
  private def getHosts(initialHost: RedisEndpoint): Array[(String, Int, Int, Int)] = {
    getSlots(initialHost).filter(_._3 == 0).map(x => (x._1, x._2, x._5, x._6))
  }

  /**
    * @param initialHost any addr and port of a single server
    * @return list of nodes(addr, port, index, range, startSlot, endSlot)
    */
  private def getNonClusterSlots(initialHost: RedisEndpoint):
  Array[(String, Int, Int, Int, Int, Int)] = {
    getNonClusterNodes(initialHost).map(x => (x._1, x._2, x._3, x._4, 0, 16383)).toArray
  }

  /**
    * @param initialHost any addr and port of a cluster server
    * @return list of nodes(addr, port, index, range, startSlot, endSlot)
    */
  private def getClusterSlots(initialHost: RedisEndpoint):
  Array[(String, Int, Int, Int, Int, Int)] = {
    val conn = initialHost.connect()
    val slots = conn.clusterSlots()
    val res = conn.clusterSlots().flatMap {
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
    conn.close()
    res
  }

  /**
    * @param initialHost any addr and port of a cluster or a single server
    * @return list of nodes(addr, port, index, range, startSlot, endSlot)
    */
  def getSlots(initialHost: RedisEndpoint): Array[(String, Int, Int, Int, Int, Int)] = {
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
  private def getNonClusterNodes(initialHost: RedisEndpoint): Array[(String, Int, Int, Int)] = {
    val master = (initialHost.host, initialHost.port)
    val conn = initialHost.connect()

    val replinfo = conn.info("Replication").split("\n")
    conn.close

    // If  this node is a slave, we need to extract the slaves from its master
    if (replinfo.filter(_.contains("role:slave")).length != 0) {
      val host = replinfo.filter(_.contains("master_host:"))(0).trim.substring(12)
      val port = replinfo.filter(_.contains("master_port:"))(0).trim.substring(12).toInt

      //simply re-enter this function witht he master host/port
      getNonClusterNodes(initialHost = new RedisEndpoint(host, port,
        initialHost.auth, initialHost.dbNum))

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
  private def getClusterNodes(initialHost: RedisEndpoint): Array[(String, Int, Int, Int)] = {
    val conn = initialHost.connect()
    val res = conn.clusterSlots().asInstanceOf[java.util.List[java.lang.Object]].flatMap {
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
    conn.close()
    res
  }

  /**
    * @param initialHost any addr and port of a cluster or a single server
    * @return list of nodes(addr, port, index, range)
    */
  private def getNodes(initialHost: RedisEndpoint): Array[(String, Int, Int, Int)] = {
    if (clusterEnabled(initialHost)) {
      getClusterNodes(initialHost)
    } else {
      getNonClusterNodes(initialHost)
    }
  }

}
