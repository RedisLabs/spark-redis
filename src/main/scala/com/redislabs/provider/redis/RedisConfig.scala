package com.redislabs.provider.redis

import java.net.URI

import org.apache.spark.SparkConf
import redis.clients.jedis.util.{JedisClusterCRC16, JedisURIHelper, SafeEncoder}
import redis.clients.jedis.{Jedis, Protocol}

import scala.collection.JavaConversions._


/**
  * RedisEndpoint represents a redis connection endpoint info: host, port, auth password
  * db number, timeout and ssl mode
  *
  * @param host  the redis host or ip
  * @param port  the redis port
  * @param auth  the authentication password
  * @param dbNum database number (should be avoided in general)
  * @param ssl true to enable SSL connection. Defaults to false
  */
case class RedisEndpoint(host: String = Protocol.DEFAULT_HOST,
                         port: Int = Protocol.DEFAULT_PORT,
                         auth: String = null,
                         dbNum: Int = Protocol.DEFAULT_DATABASE,
                         timeout: Int = Protocol.DEFAULT_TIMEOUT,
                         ssl: Boolean = false)
  extends Serializable {

  /**
    * Constructor from spark config. set params with spark.redis.host, spark.redis.port, spark.redis.auth, spark.redis.db and spark.redis.ssl
    *
    * @param conf spark context config
    */
  def this(conf: SparkConf) {
    this(
      conf.get("spark.redis.host", Protocol.DEFAULT_HOST),
      conf.getInt("spark.redis.port", Protocol.DEFAULT_PORT),
      conf.get("spark.redis.auth", null),
      conf.getInt("spark.redis.db", Protocol.DEFAULT_DATABASE),
      conf.getInt("spark.redis.timeout", Protocol.DEFAULT_TIMEOUT),
      conf.getBoolean("spark.redis.ssl", false)
    )
  }


  /**
   * Constructor from spark config and parameters.
   *
   * @param conf spark context config
   * @param parameters source specific parameters
   */
  def this(conf: SparkConf, parameters: Map[String, String]) {
    this(
      parameters.getOrElse("host", conf.get("spark.redis.host", Protocol.DEFAULT_HOST)),
      parameters.getOrElse("port", conf.get("spark.redis.port", Protocol.DEFAULT_PORT.toString)).toInt,
      parameters.getOrElse("auth", conf.get("spark.redis.auth", null)),
      parameters.getOrElse("dbNum", conf.get("spark.redis.db", Protocol.DEFAULT_DATABASE.toString)).toInt,
      parameters.getOrElse("timeout", conf.get("spark.redis.timeout", Protocol.DEFAULT_TIMEOUT.toString)).toInt,
      parameters.getOrElse("ssl", conf.get("spark.redis.ssl", false.toString)).toBoolean)
  }


  /**
    * Constructor with Jedis URI
    *
    * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]. Use "rediss://" scheme for redis SSL
    */
  def this(uri: URI) {
    this(uri.getHost, uri.getPort, JedisURIHelper.getPassword(uri), JedisURIHelper.getDBIndex(uri),
      Protocol.DEFAULT_TIMEOUT, uri.getScheme == RedisSslScheme)
  }

  /**
    * Constructor with Jedis URI from String
    *
    * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]. Use "rediss://" scheme for redis SSL
    */
  def this(uri: String) {
    this(URI.create(uri))
  }

  /**
    * Connect tries to open a connection to the redis endpoint,
    * optionally authenticating and selecting a db
    *
    * @return a new Jedis instance
    */
  def connect(): Jedis = {
    ConnectionPool.connect(this)
  }

  /**
    * @return config with masked password. Used for logging.
    */
  def maskPassword(): RedisEndpoint = {
    this.copy(auth = "")
  }
}

case class RedisNode(endpoint: RedisEndpoint,
                     startSlot: Int,
                     endSlot: Int,
                     idx: Int,
                     total: Int) {
  def connect(): Jedis = {
    endpoint.connect()
  }
}

/**
  * Tuning options for read and write operations.
  */
case class ReadWriteConfig(scanCount: Int, maxPipelineSize: Int, rddWriteIteratorGroupingSize: Int)

object ReadWriteConfig {
  /** maximum number of commands per pipeline **/
  val MaxPipelineSizeConfKey = "spark.redis.max.pipeline.size"
  val MaxPipelineSizeDefault = 100

  /** count option of SCAN command **/
  val ScanCountConfKey = "spark.redis.scan.count"
  val ScanCountDefault = 100

  /** Iterator grouping size for writing RDD **/
  val RddWriteIteratorGroupingSizeKey = "spark.redis.rdd.write.iterator.grouping.size"
  val RddWriteIteratorGroupingSizeDefault = 1000

  val Default: ReadWriteConfig = ReadWriteConfig(ScanCountDefault, MaxPipelineSizeDefault, RddWriteIteratorGroupingSizeDefault)

  def fromSparkConf(conf: SparkConf): ReadWriteConfig = {
    ReadWriteConfig(
      conf.getInt(ScanCountConfKey, ScanCountDefault),
      conf.getInt(MaxPipelineSizeConfKey, MaxPipelineSizeDefault),
      conf.getInt(RddWriteIteratorGroupingSizeKey, RddWriteIteratorGroupingSizeDefault)
    )
  }
}

object RedisConfig {

  /**
    * create redis config from spark config
    */
  def fromSparkConf(conf: SparkConf): RedisConfig = {
    new RedisConfig(new RedisEndpoint(conf))
  }

  def fromSparkConfAndParameters(conf: SparkConf, parameters: Map[String, String]): RedisConfig = {
    new RedisConfig(new RedisEndpoint(conf, parameters))
  }
}

/**
  * RedisConfig holds the state of the cluster nodes, and uses consistent hashing to map
  * keys to nodes
  */
class RedisConfig(val initialHost: RedisEndpoint) extends Serializable {

  val initialAddr: String = initialHost.host

  val hosts: Array[RedisNode] = getHosts(initialHost)
  val nodes: Array[RedisNode] = getNodes(initialHost)

  /**
    * @return initialHost's auth
    */
  def getAuth: String = {
    initialHost.auth
  }

  /**
    * @return selected db number
    */
  def getDB: Int = {
    initialHost.dbNum
  }

  def getRandomNode(): RedisNode = {
    val rnd = scala.util.Random.nextInt().abs % hosts.length
    hosts(rnd)
  }

  /**
    * @param sPos start slot number
    * @param ePos end slot number
    * @return a list of RedisNode whose slots union [sPos, ePos] is not null
    */
  def getNodesBySlots(sPos: Int, ePos: Int): Array[RedisNode] = {
    /* This function judges if [sPos1, ePos1] union [sPos2, ePos2] is not null */
    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int) =
      if (sPos1 <= sPos2) ePos1 >= sPos2 else ePos2 >= sPos1

    nodes.filter(node => inter(sPos, ePos, node.startSlot, node.endSlot)).
      filter(_.idx == 0) //master only now
  }

  /**
    * *IMPORTANT* Please remember to close after using
    *
    * @param key
    * @return jedis that is a connection for a given key
    */
  def connectionForKey(key: String): Jedis = {
    getHost(key).connect()
  }

  /**
    * *IMPORTANT* Please remember to close after using
    *
    * @param key
    * @return jedis is a connection for a given key
    */
  def connectionForKey(key: Array[Byte]): Jedis = {
    getHost(key).connect()
  }

  /**
    * @param initialHost any redis endpoint of a cluster or a single server
    * @return true if the target server is in cluster mode
    */
  private def clusterEnabled(initialHost: RedisEndpoint): Boolean = {
    val conn = initialHost.connect()
    val info = conn.info.split("\n")
    val version = info.filter(_.contains("redis_version:"))(0)
    val clusterEnable = info.filter(_.contains("cluster_enabled:"))
    val mainVersion = version.substring(14, version.indexOf(".")).toInt
    val res = mainVersion > 2 && clusterEnable.length > 0 && clusterEnable(0).contains("1")
    conn.close()
    res
  }

  /**
    * @param key
    * @return host whose slots should involve key
    */
  def getHost(key: String): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    getHostBySlot(slot)
  }

  /**
    * @param key
    * @return host whose slots should involve key
    */
  def getHost(key: Array[Byte]): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    getHostBySlot(slot)
  }

  private def getHostBySlot(slot: Int): RedisNode = {
    hosts.filter { host =>
      host.startSlot <= slot && host.endSlot >= slot
    }(0)
  }


  /**
    * @param initialHost any redis endpoint of a cluster or a single server
    * @return list of host nodes
    */
  private def getHosts(initialHost: RedisEndpoint): Array[RedisNode] = {
    getNodes(initialHost).filter(_.idx == 0)
  }

  /**
    * @param initialHost any redis endpoint of a single server
    * @return list of nodes
    */
  private def getNonClusterNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    val master = (initialHost.host, initialHost.port)
    val conn = initialHost.connect()

    val replinfo = conn.info("Replication").split("\n")
    conn.close()

    // If  this node is a slave, we need to extract the slaves from its master
    if (replinfo.exists(_.contains("role:slave"))) {
      val host = replinfo.filter(_.contains("master_host:"))(0).trim.substring(12)
      val port = replinfo.filter(_.contains("master_port:"))(0).trim.substring(12).toInt

      //simply re-enter this function witht he master host/port
      getNonClusterNodes(initialHost = new RedisEndpoint(host, port,
        initialHost.auth, initialHost.dbNum, ssl = initialHost.ssl))

    } else {
      //this is a master - take its slaves

      val slaves = replinfo.filter(x => x.contains("slave") && x.contains("online")).map(rl => {
        val content = rl.substring(rl.indexOf(':') + 1).split(",")
        val ip = content(0)
        val port = content(1)
        (ip.substring(ip.indexOf('=') + 1), port.substring(port.indexOf('=') + 1).toInt)
      })

      val nodes = master +: slaves
      val range = nodes.length
      (0 until range).map(i =>
        RedisNode(RedisEndpoint(nodes(i)._1, nodes(i)._2, initialHost.auth, initialHost.dbNum,
          initialHost.timeout, initialHost.ssl),
          0, 16383, i, range)).toArray
    }
  }

  /**
    * @param initialHost any redis endpoint of a cluster server
    * @return list of nodes
    */
  private def getClusterNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    val conn = initialHost.connect()
    val res = conn.clusterSlots().flatMap {
      slotInfoObj => {
        val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
        val sPos = slotInfo.get(0).toString.toInt
        val ePos = slotInfo.get(1).toString.toInt
        /*
         * We will get all the nodes with the slots range [sPos, ePos],
         * and create RedisNode for each nodes, the total field of all
         * RedisNode are the number of the nodes whose slots range is
         * as above, and the idx field is just an index for each node
         * which will be used for adding support for slaves and so on.
         * And the idx of a master is always 0, we rely on this fact to
         * filter master.
         */
        (0 until (slotInfo.size - 2)).map(i => {
          val node = slotInfo(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
          val host = SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]])
          val port = node.get(1).toString.toInt
          RedisNode(RedisEndpoint(host, port, initialHost.auth, initialHost.dbNum,
            initialHost.timeout, initialHost.ssl),
            sPos,
            ePos,
            i,
            slotInfo.size - 2)
        })
      }
    }.toArray
    conn.close()
    res
  }

  /**
    * @param initialHost any redis endpoint of a cluster or a single server
    * @return list of nodes
    */
  def getNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    if (clusterEnabled(initialHost)) {
      getClusterNodes(initialHost)
    } else {
      getNonClusterNodes(initialHost)
    }
  }
}
