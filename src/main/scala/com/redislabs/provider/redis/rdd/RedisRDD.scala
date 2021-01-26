package com.redislabs.provider.redis.rdd

import java.util

import com.redislabs.provider.redis.partitioner._
import com.redislabs.provider.redis.util.ParseUtils.ignoreJedisWrongTypeException
import com.redislabs.provider.redis.util.PipelineUtils.mapWithPipeline
import com.redislabs.provider.redis.{ReadWriteConfig, RedisConfig, RedisNode}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, ScanParams}
import redis.clients.jedis.util.JedisClusterCRC16

import scala.collection.JavaConversions._
import scala.reflect.{ClassTag, classTag}
import scala.util.{Failure, Success, Try}


class RedisKVRDD(prev: RDD[String],
                 val rddType: String,
                 implicit val readWriteConfig: ReadWriteConfig)
  extends RDD[(String, String)](prev) with Keys {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    val keys = firstParent[String].iterator(split, context)
    val auth = partition.redisConfig.getAuth
    val db = partition.redisConfig.getDB
    rddType match {
      case "kv" => getKV(nodes, keys)
      case "hash" => getHASH(nodes, keys)
    }
  }

  def getKV(nodes: Array[RedisNode], keys: Iterator[String]): Iterator[(String, String)] = {
    groupKeysByNode(nodes, keys).flatMap { case (node, nodeKeys) =>
      val conn = node.endpoint.connect()

      val response = mapWithPipeline(conn, nodeKeys) { (pipeline, key) =>
        pipeline.get(key)
      }

      val res = nodeKeys.zip(response)
        .flatMap{
          case (_, e: Throwable) => Some(Failure(e))
          case (k, v: String) => Some(Success((k,v)))
          case _ => None
        }.flatMap(ignoreJedisWrongTypeException(_).get) // Unwrap `Try` to throw exceptions if any
        .iterator

      conn.close()
      res
    }
  }

  def getHASH(nodes: Array[RedisNode], keys: Iterator[String]): Iterator[(String, String)] = {
    groupKeysByNode(nodes, keys).flatMap { case (node, nodeKeys) =>
      val conn = node.endpoint.connect()
      val res = nodeKeys.flatMap{k =>
        ignoreJedisWrongTypeException(Try(conn.hgetAll(k).toMap)).get
      }.flatten.iterator
      conn.close()
      res
    }
  }
}

class RedisListRDD(prev: RDD[String],
                   val rddType: String,
                   implicit val readWriteConfig: ReadWriteConfig) extends RDD[String](prev) with Keys {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    val keys = firstParent[String].iterator(split, context)
    rddType match {
      case "set" => getSET(nodes, keys)
      case "list" => getLIST(nodes, keys)
    }
  }

  def getSET(nodes: Array[RedisNode], keys: Iterator[String]): Iterator[String] = {
    groupKeysByNode(nodes, keys).flatMap { case (node, nodeKeys) =>
      val conn = node.endpoint.connect()
      val res: Iterator[String] = nodeKeys.flatMap{k =>
        ignoreJedisWrongTypeException(Try(conn.smembers(k).toSet)).get
      }.flatten
        .iterator
      conn.close()
      res
    }
  }

  def getLIST(nodes: Array[RedisNode], keys: Iterator[String]): Iterator[String] = {
    groupKeysByNode(nodes, keys).flatMap { case (node, nodeKeys) =>
      val conn = node.endpoint.connect()
      val res = nodeKeys.flatMap{ k =>
        ignoreJedisWrongTypeException(Try(conn.lrange(k, 0, -1))).get
      }.flatten.iterator
      conn.close()
      res
    }
  }
}

case class ZSetContext(startPos: Long,
                       endPos: Long,
                       min: Double,
                       max: Double,
                       withScore: Boolean,
                       typ: String)

class RedisZSetRDD[T: ClassTag](prev: RDD[String],
                                zsetContext: ZSetContext,
                                rddType: Class[T],
                                implicit val readWriteConfig: ReadWriteConfig)
  extends RDD[T](prev) with Keys {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    val keys = firstParent[String].iterator(split, context)
    val auth = partition.redisConfig.getAuth
    val db = partition.redisConfig.getDB
    zsetContext.typ match {
      case "byRange" => getZSetByRange(nodes, keys, zsetContext.startPos, zsetContext.endPos)
      case "byScore" => getZSetByScore(nodes, keys, zsetContext.min, zsetContext.max)
    }
  }

  private def getZSetByRange(nodes: Array[RedisNode],
                             keys: Iterator[String],
                             startPos: Long,
                             endPos: Long): Iterator[T] = {
    groupKeysByNode(nodes, keys).flatMap { case (node, nodeKeys) =>
      val conn = node.endpoint.connect()
      val res = {
        if (classTag[T] == classTag[(String, Double)]) {
          nodeKeys.flatMap{k =>
            ignoreJedisWrongTypeException(Try(conn.zrangeWithScores(k, startPos, endPos))).get
          }.flatten
            .map(tup => (tup.getElement, tup.getScore)).iterator
        } else if (classTag[T] == classTag[String]) {
          nodeKeys.flatMap{k =>
            ignoreJedisWrongTypeException(Try(conn.zrange(k, startPos, endPos))).get
          }.flatten.iterator
        } else {
          throw new scala.Exception("Unknown RedisZSetRDD type")
        }
      }
      conn.close()
      res
    }.asInstanceOf[Iterator[T]]
  }

  private def getZSetByScore(nodes: Array[RedisNode],
                             keys: Iterator[String],
                             startScore: Double,
                             endScore: Double): Iterator[T] = {
    groupKeysByNode(nodes, keys).flatMap { case (node, nodeKeys) =>
      val conn = node.endpoint.connect()
      val res = {
        if (classTag[T] == classTag[(String, Double)]) {
          nodeKeys.flatMap{k =>
            ignoreJedisWrongTypeException(Try(conn.zrangeByScoreWithScores(k, startScore, endScore))).get
          }.
            flatten
            .map(tup => (tup.getElement, tup.getScore)).iterator
        } else if (classTag[T] == classTag[String]) {
          nodeKeys.flatMap{ k =>
            ignoreJedisWrongTypeException(Try(conn.zrangeByScore(k, startScore, endScore))).get
          }.flatten.iterator
        } else {
          throw new scala.Exception("Unknown RedisZSetRDD type")
        }
      }
      conn.close()
      res
    }.asInstanceOf[Iterator[T]]
  }
}

class RedisKeysRDD(sc: SparkContext,
                   val redisConfig: RedisConfig,
                   implicit val readWriteConfig: ReadWriteConfig,
                   val keyPattern: String = "*",
                   val partitionNum: Int = 3,
                   val keys: Array[String] = null)
  extends RDD[String](sc, Seq.empty) with Keys {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].redisConfig.initialAddr)
  }

  /**
    * hosts(ip:String, port:Int, startSlot:Int, endSlot:Int) are generated by the redis-cluster's
    * hash tragedy and partitionNum to divide the cluster to partitionNum
    *
    * @return hosts
    */
  private def scaleHostsWithPartitionNum(): Seq[(String, Int, Int, Int)] = {
    def split(host: RedisNode, cnt: Int) = {
      val endpoint = host.endpoint
      val start = host.startSlot
      val end = host.endSlot
      val range = (end - start) / cnt
      (0 until cnt).map(i => {
        (endpoint.host,
          endpoint.port,
          if (i == 0) start else (start + range * i + 1),
          if (i != cnt - 1) (start + range * (i + 1)) else end)
      })
    }

    val hosts = redisConfig.hosts.sortBy(_.startSlot)

    if (hosts.size == partitionNum) {
      hosts.map(x => (x.endpoint.host, x.endpoint.port, x.startSlot, x.endSlot))
    } else if (hosts.size < partitionNum) {
      val presExtCnt = partitionNum / hosts.size
      val lastExtCnt = if (presExtCnt * hosts.size < partitionNum) (presExtCnt + partitionNum % hosts.size) else presExtCnt
      hosts.zipWithIndex.flatMap {
        case (host, idx) => {
          split(host, if (idx == hosts.size - 1) lastExtCnt else presExtCnt)
        }
      }
    } else {
      val presExtCnt = hosts.size / partitionNum
      (0 until partitionNum).map {
        idx => {
          val ip = hosts(idx * presExtCnt).endpoint.host
          val port = hosts(idx * presExtCnt).endpoint.port
          val start = hosts(idx * presExtCnt).startSlot
          val end = hosts(if (idx == partitionNum - 1) {
            (hosts.size - 1)
          } else {
            ((idx + 1) * presExtCnt - 1)
          }).endSlot
          (ip, port, start, end)
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val hosts = scaleHostsWithPartitionNum()
    (0 until partitionNum).map(i => {
      new RedisPartition(i,
        redisConfig,
        (hosts(i)._3, hosts(i)._4)).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)

    if (Option(this.keys).isDefined) {
      this.keys.filter(key => {
        val slot = JedisClusterCRC16.getSlot(key)
        slot >= sPos && slot <= ePos
      }).iterator
    } else {
      logInfo {
        val nodesPassMasked = nodes.map(n => n.copy(endpoint = n.endpoint.maskPassword())).mkString
        s"Computing partition, get keys partId: ${partition.index},  [$sPos - $ePos] nodes: $nodesPassMasked"
      }
      getKeys(nodes, sPos, ePos, keyPattern)
    }
  }

  /**
    * filter the 'set' type keys and get all the elements of them
    *
    * @return RedisSetRDD[String]
    */
  def getSet(): RDD[String] = {
    new RedisListRDD(this, "set", readWriteConfig)
  }

  /**
    * filter the 'list' type keys and get all the elements of them
    *
    * @return RedisListRDD[String]
    */
  def getList(): RDD[String] = {
    new RedisListRDD(this, "list", readWriteConfig)
  }

  /**
    * filter the 'plain k/v' type keys and get all the k/v
    *
    * @return RedisKVRDD[(String, String)]
    */
  def getKV(): RDD[(String, String)] = {
    new RedisKVRDD(this, "kv", readWriteConfig)
  }

  /**
    * filter the 'hash' type keys and get all the elements of them
    *
    * @return RedisHashRDD[(String, String)]
    */
  def getHash(): RDD[(String, String)] = {
    new RedisKVRDD(this, "hash", readWriteConfig)
  }

  /**
    * filter the 'zset' type keys and get all the elements(without scores) of them
    *
    * @return RedisZSetRDD[String]
    */
  def getZSet(): RDD[String] = {
    val zsetContext: ZSetContext = new ZSetContext(0, -1, Double.MinValue, Double.MaxValue, false, "byRange")
    new RedisZSetRDD(this, zsetContext, classOf[String], readWriteConfig)
  }

  /**
    * filter the 'zset' type keys and get all the elements(with scores) of them
    *
    * @return RedisZSetRDD[(String, Double)]
    */
  def getZSetWithScore(): RDD[(String, Double)] = {
    val zsetContext: ZSetContext = new ZSetContext(0, -1, Double.MinValue, Double.MaxValue, true, "byRange")
    new RedisZSetRDD(this, zsetContext, classOf[(String, Double)], readWriteConfig)
  }

  /**
    * filter the 'zset' type keys and get all the elements(without scores) of range [startPos, endPos]
    *
    * @param startPos start position of zsets
    * @param endPos   end position of zsets
    * @return RedisZSetRDD[String]
    */
  def getZSetByRange(startPos: Long, endPos: Long): RDD[String] = {
    val zsetContext: ZSetContext = new ZSetContext(startPos, endPos, Double.MinValue, Double.MaxValue, false, "byRange")
    new RedisZSetRDD(this, zsetContext, classOf[String], readWriteConfig)
  }

  /**
    * filter the 'zset' type keys and get all the elements(with scores) of range [startPos, endPos]
    *
    * @param startPos start position of zsets
    * @param endPos   end position of zsets
    * @return RedisZSetRDD[(String, Double)]
    */
  def getZSetByRangeWithScore(startPos: Long, endPos: Long): RDD[(String, Double)] = {
    val zsetContext: ZSetContext = new ZSetContext(startPos, endPos, Double.MinValue, Double.MaxValue, true, "byRange")
    new RedisZSetRDD(this, zsetContext, classOf[(String, Double)], readWriteConfig)
  }

  /**
    * filter the 'zset' type keys and get all the elements(without scores) of score range [min, max]
    *
    * @param min start position of zsets
    * @param max end position of zsets
    * @return RedisZSetRDD[String]
    */
  def getZSetByScore(min: Double, max: Double): RDD[String] = {
    val zsetContext: ZSetContext = new ZSetContext(0, -1, min, max, false, "byScore")
    new RedisZSetRDD(this, zsetContext, classOf[String], readWriteConfig)
  }

  /**
    * filter the 'zset' type keys and get all the elements(with scores) of score range [min, max]
    *
    * @param min start position of zsets
    * @param max end position of zsets
    * @return RedisZSetRDD[(String, Double)]
    */
  def getZSetByScoreWithScore(min: Double, max: Double): RDD[(String, Double)] = {
    val zsetContext: ZSetContext = new ZSetContext(0, -1, min, max, true, "byScore")
    new RedisZSetRDD(this, zsetContext, classOf[(String, Double)], readWriteConfig)
  }
}


trait Keys {
  /**
    * @param key
    * @return true if the key is a RedisRegex
    */
  def isRedisRegex(key: String): Boolean = {
    def judge(key: String, escape: Boolean): Boolean = {
      if (key.length == 0) {
        false
      } else {
        escape match {
          case true => judge(key.substring(1), false)
          case false => key.charAt(0) match {
            case '*' => true
            case '?' => true
            case '[' => true
            case '\\' => judge(key.substring(1), true)
            case _ => judge(key.substring(1), false)
          }
        }
      }
    }

    judge(key, false)
  }

  /**
    * Scan keys, the result may contain duplicates
    *
    * @param jedis
    * @param params
    * @return keys of params pattern in jedis
    */
  private def scanKeys(jedis: Jedis, params: ScanParams): util.List[String] = {
    val keys = new util.ArrayList[String]
    var cursor = "0"
    do {
      val scan = jedis.scan(cursor, params)
      keys.addAll(scan.getResult)
      cursor = scan.getCursor
    } while (cursor != "0")
    keys
  }

  /**
    * @param nodes list of RedisNode
    * @param sPos  start position of slots
    * @param ePos  end position of slots
    * @param keyPattern
    * @return keys whose slot is in [sPos, ePos]
    */
  def getKeys(nodes: Array[RedisNode],
              sPos: Int,
              ePos: Int,
              keyPattern: String)
             (implicit readWriteConfig: ReadWriteConfig): Iterator[String] = {
    val endpoints = nodes.map(_.endpoint).distinct

    if (isRedisRegex(keyPattern)) {
      endpoints.iterator.map { endpoint =>
        val keys = new util.HashSet[String]()
        val conn = endpoint.connect()
        val params = new ScanParams().`match`(keyPattern).count(readWriteConfig.scanCount)
        keys.addAll(scanKeys(conn, params).filter { key =>
          val slot = JedisClusterCRC16.getSlot(key)
          slot >= sPos && slot <= ePos
        })
        conn.close()
        keys.iterator()
      }.flatten
    } else {
      val slot = JedisClusterCRC16.getSlot(keyPattern)
      if (slot >= sPos && slot <= ePos) Iterator(keyPattern) else Iterator()
    }
  }

  /**
    * Master node for a key
    *
    * @param nodes list of all nodes
    * @param key   key
    * @return master node
    */
  def getMasterNode(nodes: Array[RedisNode], key: String): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    /* Master only */
    nodes.filter { node => node.startSlot <= slot && node.endSlot >= slot }.filter(_.idx == 0)(0)
  }

  /**
    * @param nodes list of RedisNode
    * @param keys  list of keys
    * @return (node: (key1, key2, ...), node2: (key3, key4,...), ...)
    */
  def groupKeysByNode(nodes: Array[RedisNode], keys: Iterator[String]): Iterator[(RedisNode, Array[String])] = {
    keys.map(key => (getMasterNode(nodes, key), key)).toArray.groupBy(_._1).
      map(x => (x._1, x._2.map(_._2))).iterator
  }

  /**
    * @param conn
    * @param keys
    * keys are guaranteed that they belongs with the server jedis connected to.
    * @return keys of "t" type
    */
  def filterKeysByType(conn: Jedis, keys: Array[String], t: String)
                      (implicit readWriteConfig: ReadWriteConfig): Array[String] = {
    val types = mapWithPipeline(conn, keys) { (pipeline, key) =>
      pipeline.`type`(key)
    }
    keys.zip(types).filter(x => x._2 == t).map(x => x._1)
  }
}

/**
  * Key utilities to avoid serialization issues.
  */
object Keys extends Keys
