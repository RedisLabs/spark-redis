package com.redislabs.provider.redis

import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean

import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.Logging
import org.apache.spark.SparkConf
import redis.clients.jedis.util.{JedisClusterCRC16, JedisURIHelper, SafeEncoder}
import redis.clients.jedis.{Jedis, Protocol}

import scala.collection.JavaConverters._


/**
  * RedisEndpoint represents a redis connection endpoint info: host, port, auth password
  * db number, timeout and ssl mode
  *
  * @param host  the redis host or ip
  * @param port  the redis port
  * @param user  the authentication username
  * @param auth  the authentication password, or null to send no `AUTH`. Set from `spark.redis.password` (or the
  *              deprecated `spark.redis.auth`). [[toString]] never shows it.
  * @param dbNum database number (should be avoided in general)
  * @param ssl true to enable SSL connection. Defaults to false
  * @param clientName the name every connection to this endpoint sets with CLIENT SETNAME, or null (the default)
  *                   to leave connections unnamed. An empty name also leaves them unnamed.
  */
case class RedisEndpoint(host: String = Protocol.DEFAULT_HOST,
                         port: Int = Protocol.DEFAULT_PORT,
                         user: String = null,
                         auth: String = null,
                         dbNum: Int = Protocol.DEFAULT_DATABASE,
                         timeout: Int = Protocol.DEFAULT_TIMEOUT,
                         ssl: Boolean = false,
                         clientName: String = null)
  extends Serializable {

  // Fail on the driver, naming the bad value, rather than on the first connection with Redis's own error.
  require(clientName == null || RedisConfig.isValidClientName(clientName),
    s"Invalid Redis client name '$clientName' (${RedisConfig.ClientNameConfKey} or the " +
      s"${RedisConfig.ClientNameOption} option): use printable ASCII characters only, with no spaces")

  /**
    * Constructor from spark config. set params with spark.redis.host, spark.redis.port, spark.redis.user,
    * spark.redis.password (or the deprecated spark.redis.auth), spark.redis.db, spark.redis.timeout, spark.redis.ssl
    * and spark.redis.client.name
    *
    * @param conf spark context config
    */
  def this(conf: SparkConf) {
    this(
      conf.get("spark.redis.host", Protocol.DEFAULT_HOST),
      conf.getInt("spark.redis.port", Protocol.DEFAULT_PORT),
      conf.get("spark.redis.user", null),
      RedisConfig.passwordOrNull(conf),
      conf.getInt("spark.redis.db", Protocol.DEFAULT_DATABASE),
      conf.getInt("spark.redis.timeout", Protocol.DEFAULT_TIMEOUT),
      conf.getBoolean("spark.redis.ssl", false),
      RedisConfig.clientNameOrNull(conf.get(RedisConfig.ClientNameConfKey, null))
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
      parameters.getOrElse("user", conf.get("spark.redis.user", null)),
      RedisConfig.passwordOrNull(conf, parameters),
      parameters.getOrElse("dbNum", conf.get("spark.redis.db", Protocol.DEFAULT_DATABASE.toString)).toInt,
      parameters.getOrElse("timeout", conf.get("spark.redis.timeout", Protocol.DEFAULT_TIMEOUT.toString)).toInt,
      parameters.getOrElse("ssl", conf.get("spark.redis.ssl", false.toString)).toBoolean,
      RedisConfig.clientNameOrNull(parameters.getOrElse(RedisConfig.ClientNameOption,
        conf.get(RedisConfig.ClientNameConfKey, null))))
  }


  /**
    * Constructor with Jedis URI
    *
    * @param uri connection URI in the form of redis://$user:$password@$host:$port/[dbnum]. Use "rediss://" scheme for redis SSL
    */
  def this(uri: URI) {
    this(uri.getHost, uri.getPort, JedisURIHelper.getUser(uri), JedisURIHelper.getPassword(uri), JedisURIHelper.getDBIndex(uri),
      Protocol.DEFAULT_TIMEOUT, uri.getScheme == RedisSslScheme)
  }

  /**
    * Constructor with Jedis URI from String
    *
    * @param uri connection URI in the form of redis://$user:$password@$host:$port/[dbnum]. Use "rediss://" scheme for redis SSL
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
    * @return the endpoint with an empty password. Logging doesn't need it: [[toString]] already hides the password.
    */
  def maskPassword(): RedisEndpoint = {
    this.copy(auth = "")
  }

  /**
    * The case class's own format, with the password replaced: endpoints are logged, and end up in error messages and
    * in the toString of the classes that hold them, such as [[RedisNode]].
    */
  override def toString: String = {
    val shown = if (auth == null) this else copy(auth = RedisConfig.RedactedPassword)
    shown.productIterator.mkString(productPrefix + "(", ",", ")")
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

object RedisConfig extends Logging {

  /**
    * Spark setting: the password sent with AUTH. Spark hides its value in the Spark UI, the History Server and event
    * logs, because the name matches the default `spark.redaction.regex`.
    */
  val PasswordConfKey = "spark.redis.password"

  /**
    * Deprecated name of [[PasswordConfKey]], still read when [[PasswordConfKey]] is unset. Spark's default
    * `spark.redaction.regex` doesn't match it, so its value shows in clear text in the Spark UI, the History Server and
    * event logs (GHSA-wmm3-vmjq-7mmc).
    */
  val AuthConfKey = "spark.redis.auth"

  /** DataFrame and stream option that overrides [[PasswordConfKey]]. Spark redacts it where it shows options. */
  val PasswordOption = "password"

  /** Deprecated name of [[PasswordOption]], still read when [[PasswordOption]] is unset. Spark doesn't redact it. */
  val AuthOption = "auth"

  /** What [[RedisEndpoint.toString]] shows instead of a password: the text Spark itself puts in place of a secret. */
  private[redis] val RedactedPassword = "*********(redacted)"

  // Endpoints are built for every RDD call and every DataFrame, so each deprecation warning is logged once per JVM.
  private val authConfWarned = new AtomicBoolean(false)
  private val authOptionWarned = new AtomicBoolean(false)

  /** The password in the Spark settings: [[PasswordConfKey]], else [[AuthConfKey]], else null for no AUTH. */
  private[redis] def passwordOrNull(conf: SparkConf): String = {
    if (conf.contains(AuthConfKey) && authConfWarned.compareAndSet(false, true)) {
      val fix = if (conf.contains(PasswordConfKey)) {
        s"$PasswordConfKey is set too and is used, so remove $AuthConfKey"
      } else {
        s"set $PasswordConfKey instead"
      }
      logWarning(s"$AuthConfKey is deprecated and Spark does not redact it, so the Redis password shows in clear " +
        s"text in the Spark UI, the History Server and event logs. To hide it, $fix. Consider rotating the password.")
    }
    conf.getOption(PasswordConfKey).orElse(conf.getOption(AuthConfKey)).orNull
  }

  /**
    * The password for a DataFrame or stream: the [[PasswordOption]] option, else the [[AuthOption]] option, else the
    * Spark settings.
    */
  private[redis] def passwordOrNull(conf: SparkConf, parameters: Map[String, String]): String = {
    if (parameters.contains(AuthOption) && authOptionWarned.compareAndSet(false, true)) {
      val fix = if (parameters.contains(PasswordOption)) {
        s"the '$PasswordOption' option is set too and is used, so remove '$AuthOption'"
      } else {
        s"use the '$PasswordOption' option instead"
      }
      logWarning(s"The '$AuthOption' option is deprecated and Spark does not redact it, so the Redis password can " +
        s"show in clear text in query plans, in the Spark UI's SQL tab and in event logs. To hide it, $fix.")
    }
    val fromConf = passwordOrNull(conf)
    parameters.get(PasswordOption).orElse(parameters.get(AuthOption)).getOrElse(fromConf)
  }

  /** Spark setting: the name every connection the connector opens sets with CLIENT SETNAME (#398). */
  val ClientNameConfKey = "spark.redis.client.name"

  /** DataFrame source option that overrides [[ClientNameConfKey]] for that source. */
  val ClientNameOption = "client.name"

  /** An unset or empty client name both mean unnamed connections, the behaviour without the setting. */
  private[redis] def clientNameOrNull(name: String): String =
    if (name == null || name.isEmpty) null else name

  /** CLIENT SETNAME accepts only printable ASCII ('!' to '~'): no spaces, newlines or other characters. */
  private[redis] def isValidClientName(name: String): Boolean =
    name.forall(c => c >= '!' && c <= '~')

  /**
    * create redis config from spark config
    */
  def fromSparkConf(conf: SparkConf): RedisConfig = {
    new RedisConfig(new RedisEndpoint(conf))
  }

  def fromSparkConfAndParameters(conf: SparkConf, parameters: Map[String, String]): RedisConfig = {
    new RedisConfig(new RedisEndpoint(conf, parameters))
  }

  /**
    * The fields of an `INFO` reply, as field -> value. Section headers (`# Replication`) and blank lines are skipped,
    * and a value keeps any `:` after the first, as in an IPv6 address.
    */
  private[redis] def parseInfo(info: String): Map[String, String] =
    info.split("\r?\n").iterator.map(_.trim).filter(line => line.nonEmpty && !line.startsWith("#")).flatMap { line =>
      val colon = line.indexOf(':')
      if (colon > 0) Some(line.substring(0, colon) -> line.substring(colon + 1)) else None
    }.toMap

  /**
    * The replicas that `INFO replication` lists on a primary with `state=online`, as (host, port). Each is a field
    * `slave<n>` with a value such as `ip=10.0.0.2,port=6380,state=online,offset=1234,lag=0`.
    */
  private[redis] def onlineReplicas(replication: Map[String, String]): Seq[(String, Int)] =
    replication.toSeq.filter { case (field, _) => field.matches("slave\\d+") }.sortBy(_._1.drop(5).toInt)
      .flatMap { case (_, value) =>
        val attributes = value.split(',').iterator.map(_.split("=", 2)).collect { case Array(k, v) => k -> v }.toMap
        for {
          ip <- attributes.get("ip")
          port <- attributes.get("port").flatMap(p => scala.util.Try(p.toInt).toOption)
          if attributes.get("state").contains("online")
        } yield (ip, port)
      }

  /**
    * The primary in `nodes` that serves `slot`.
    *
    * @throws IllegalStateException if none does, for example because the cluster was resharding or failing over when
    *                               the topology was read
    */
  private[redis] def primaryForSlot(nodes: Array[RedisNode], slot: Int): RedisNode =
    nodes.find(node => node.idx == 0 && node.startSlot <= slot && node.endSlot >= slot).getOrElse {
      throw new IllegalStateException(s"No primary serves hash slot $slot in the topology Spark-Redis read " +
        s"(${nodes.count(_.idx == 0)} primaries). The cluster may have been resharding or failing over: retry.")
    }
}

/**
  * RedisConfig holds the state of the cluster nodes, and uses consistent hashing to map
  * keys to nodes
  */
class RedisConfig(val initialHost: RedisEndpoint) extends Serializable {

  val initialAddr: String = initialHost.host

  /** Every node, primaries and replicas, read once from the server when this config is created. */
  val nodes: Array[RedisNode] = getNodes(initialHost)
  /** The primaries among [[nodes]]. */
  val hosts: Array[RedisNode] = nodes.filter(_.idx == 0)

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
    hosts(scala.util.Random.nextInt(hosts.length))
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
    * @return true if the target server is in cluster mode. A server whose `INFO` has no `cluster_enabled` field,
    *         such as a proxy, is not.
    */
  private def clusterEnabled(initialHost: RedisEndpoint): Boolean = {
    val info = withConnection(initialHost.connect()) { conn =>
      RedisConfig.parseInfo(conn.info)
    }
    info.get("cluster_enabled").contains("1")
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

  /** @throws IllegalStateException if no primary serves `slot` */
  private def getHostBySlot(slot: Int): RedisNode = RedisConfig.primaryForSlot(hosts, slot)

  /**
    * @param initialHost any redis endpoint of a single server
    * @return list of nodes
    */
  private def getNonClusterNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    val master = (initialHost.host, initialHost.port)
    val replication = withConnection(initialHost.connect()) { conn =>
      RedisConfig.parseInfo(conn.info("Replication"))
    }

    // If  this node is a slave, we need to extract the slaves from its master
    if (replication.get("role").contains("slave")) {
      val server = s"${initialHost.host}:${initialHost.port}"
      val host = replication.getOrElse("master_host",
        throw new IllegalStateException(s"Redis at $server is a replica, but INFO replication names no master_host"))
      val port = replication.get("master_port").flatMap(p => scala.util.Try(p.toInt).toOption).getOrElse(
        throw new IllegalStateException(
          s"Redis at $server is a replica, but INFO replication has no valid master_port"))

      // re-enter this function with the master's host and port, and every other setting of the initial endpoint
      // (credentials, database, timeout, TLS, client name)
      getNonClusterNodes(initialHost = initialHost.copy(host = host, port = port))

    } else {
      //this is a master - take its slaves
      val nodes = master +: RedisConfig.onlineReplicas(replication)
      val range = nodes.length
      (0 until range).map(i => {
        val endpoint = RedisEndpoint(
          host = nodes(i)._1,
          port = nodes(i)._2,
          user = initialHost.user,
          auth = initialHost.auth,
          dbNum = initialHost.dbNum,
          timeout = initialHost.timeout,
          ssl = initialHost.ssl,
          clientName = initialHost.clientName)
        RedisNode(endpoint, 0, 16383, i, range)
      }).toArray
    }
  }

  /**
    * @param initialHost any redis endpoint of a cluster server
    * @return list of nodes
    */
  private def getClusterNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    val slots = withConnection(initialHost.connect()) { conn =>
      conn.clusterSlots()
    }
    slots.asScala.flatMap {
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
          val node = slotInfo.get(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
          val host = SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]])
          val port = node.get(1).toString.toInt
          val endpoint = RedisEndpoint(
            host = host,
            port = port,
            user = initialHost.user,
            auth = initialHost.auth,
            dbNum = initialHost.dbNum,
            timeout = initialHost.timeout,
            ssl = initialHost.ssl,
            clientName = initialHost.clientName)
          RedisNode(endpoint, sPos, ePos, i, slotInfo.size - 2)
        })
      }
    }.toArray
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
