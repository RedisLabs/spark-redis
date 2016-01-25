package com.redislabs.provider.redis.rdd

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark._
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._
import com.redislabs.provider.redis.partitioner._
import com.redislabs.provider.RedisConfig

class RedisKVRDD(prev: RDD[String],
                 val rddType: String)
    extends RDD[(String, String)](prev) with Keys {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    val keys = firstParent[String].iterator(split, context)
    rddType match {
      case "kv"   => getKV(nodes, keys);
      case "hash" => getHASH(nodes, keys);
      case "zset" => getZSET(nodes, keys);
    }
  }

  def getKV(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]): Iterator[(String, String)] = {
    groupKeysByNode(nodes, keys).flatMap {
      x =>
        {
          val jedis = new Jedis(x._1._1, x._1._2)
          val stringKeys = filterKeysByType(jedis, x._2, "string")
          val pipeline = jedis.pipelined
          stringKeys.foreach(pipeline.get)
          val res = stringKeys.zip(pipeline.syncAndReturnAll).iterator.asInstanceOf[Iterator[(String, String)]]
          jedis.close
          res
        }
    }.iterator
  }
  def getHASH(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]): Iterator[(String, String)] = {
    groupKeysByNode(nodes, keys).flatMap {
      x =>
        {
          val jedis = new Jedis(x._1._1, x._1._2)
          val hashKeys = filterKeysByType(jedis, x._2, "hash")
          val res = hashKeys.flatMap(jedis.hgetAll).iterator
          jedis.close
          res
        }
    }.iterator
  }
  def getZSET(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]): Iterator[(String, String)] = {
    groupKeysByNode(nodes, keys).flatMap {
      x =>
        {
          val jedis = new Jedis(x._1._1, x._1._2)
          val zsetKeys = filterKeysByType(jedis, x._2, "zset")
          val res = zsetKeys.flatMap(k => jedis.zrangeWithScores(k, 0, -1)).map(tup => (tup.getElement, tup.getScore.toString)).iterator
          jedis.close
          res
        }
    }.iterator
  }
}

class RedisListRDD(prev: RDD[String],
                   val rddType: String)
    extends RDD[String](prev) with Keys {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    val keys = firstParent[String].iterator(split, context)
    rddType match {
      case "set"  => getSET(nodes, keys);
      case "list" => getLIST(nodes, keys);
    }
  }

  def getSET(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]): Iterator[String] = {
    groupKeysByNode(nodes, keys).flatMap {
      x =>
        {
          val jedis = new Jedis(x._1._1, x._1._2)
          val setKeys = filterKeysByType(jedis, x._2, "set")
          val res = setKeys.flatMap(jedis.smembers).iterator
          jedis.close
          res
        }
    }.iterator
  }
  def getLIST(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]): Iterator[String] = {
    groupKeysByNode(nodes, keys).flatMap {
      x =>
        {
          val jedis = new Jedis(x._1._1, x._1._2)
          val listKeys = filterKeysByType(jedis, x._2, "list")
          val res = listKeys.flatMap(jedis.lrange(_, 0, -1)).iterator
          jedis.close
          res
        }
    }.iterator
  }
}

class RedisKeysRDD(sc: SparkContext,
                   val redisNode: (String, Int),
                   val keyPattern: String = "*",
                   val partitionNum: Int = 3)
    extends RDD[String](sc, Seq.empty) with Logging with Keys {

  override protected def getPartitions: Array[Partition] = {
    val cnt = 16384 / partitionNum
    (0 until partitionNum).map(i => {
      new RedisPartition(i,
        new RedisConfig(redisNode._1, redisNode._2),
        (if (i == 0) 0 else cnt * i + 1, if (i != partitionNum - 1) cnt * (i + 1) else 16383)).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    getKeys(nodes, sPos, ePos, keyPattern).iterator;
  }
  def getSet(): RDD[String] = {
    new RedisListRDD(this, "set")
  }
  def getList(): RDD[String] = {
    new RedisListRDD(this, "list")
  }
  def getKV(): RDD[(String, String)] = {
    new RedisKVRDD(this, "kv")
  }
  def getHash(): RDD[(String, String)] = {
    new RedisKVRDD(this, "hash")
  }
  def getZSet(): RDD[(String, String)] = {
    new RedisKVRDD(this, "zset")
  }
}

trait Keys {
  /**
   * @param key
   * @return true if the key is a RedisRegex
   */
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

  /**
   * @param jedis
   * @param params
   * @return keys of params pattern in jedis
   */
  private def scanKeys(jedis: Jedis, params: ScanParams): util.HashSet[String] = {
    val keys = new util.HashSet[String]
    var cursor = "0"
    do {
      val scan = jedis.scan(cursor, params)
      keys.addAll(scan.getResult)
      cursor = scan.getStringCursor
    } while (cursor != "0")
    keys
  }

  /**
   * @param nodes list of nodes(IP:String, port:Int, index:Int, range:Int, startSlot:Int, endSlot:Int)
   * @param sPos start position of slots
   * @param ePos end position of slots
   * @param keyPattern
   * return keys whose slot is in [sPos, ePos]
   */
  def getKeys(nodes: Array[(String, Int, Int, Int, Int, Int)], sPos: Int, ePos: Int, keyPattern: String) = {
    val keys = new util.HashSet[String]()
    if (isRedisRegex(keyPattern)) {
      nodes.foreach(node => {
        val jedis = new Jedis(node._1, node._2)
        val params = new ScanParams().`match`(keyPattern)
        val res = keys.addAll(scanKeys(jedis, params).filter(key => {
          val slot = JedisClusterCRC16.getSlot(key)
          slot >= sPos && slot <= ePos
        }))
        jedis.close
        res
      })
    } else {
      val slot = JedisClusterCRC16.getSlot(keyPattern)
      if (slot >= sPos && slot <= ePos)
        keys.add(keyPattern)
    }
    keys
  }

  /**
   * @param nodes list of nodes(IP:String, port:Int, index:Int, range:Int, startSlot:Int, endSlot:Int)
   * @param keys list of keys
   * return (node: (key1, key2, ...), node2: (key3, key4,...), ...)
   */
  def groupKeysByNode(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]) = {
    def getNode(key: String) = {
      val slot = JedisClusterCRC16.getSlot(key)
      nodes.filter(node => { node._5 <= slot && node._6 >= slot }).filter(_._3 == 0)(0) // master only
    }
    keys.map(key => (getNode(key), key)).toArray.groupBy(_._1).map(x => (x._1, x._2.map(_._2))).toArray
  }

  /**
   * @param jedis
   * @param keys
   * keys are guaranteed that they belongs with the server jedis connected to.
   * Filter all the keys of "t" type.
   */
  def filterKeysByType(jedis: Jedis, keys:Array[String], t:String) = {
    val pipeline = jedis.pipelined
    keys.foreach(pipeline.`type`)
    val types = pipeline.syncAndReturnAll
    (keys).zip(types).filter(x => (x._2 == t)).map(x => x._1)
  }
}
