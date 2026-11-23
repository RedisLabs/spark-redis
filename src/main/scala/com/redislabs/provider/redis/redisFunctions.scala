package com.redislabs.provider.redis

import com.redislabs.provider.redis.rdd._
import com.redislabs.provider.redis.util.PipelineUtils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Pipeline
import redis.clients.jedis.params.SetParams
import scala.collection.JavaConverters._

/**
  * RedisContext extends sparkContext's functionality with redis functions
  *
  * @param sc a spark context
  */
class RedisContext(@transient val sc: SparkContext) extends Serializable {

  val IncorrectKeysOrKeyPatternMsg = "KeysOrKeyPattern should be String or Array[String]"

  import com.redislabs.provider.redis.RedisContext._

  /**
    * @param keyPattern   a key pattern to match, or a single key
    * @param partitionNum number of partitions
    * @return RedisKeysRDD of simple Keys stored in redis server
    */
  def fromRedisKeyPattern(keyPattern: String = "*",
                          partitionNum: Int = 3)
                         (implicit
                          redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                          readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RedisKeysRDD = {
    new RedisKeysRDD(sc, redisConfig, readWriteConfig, keyPattern, partitionNum, null)
  }

  /**
    * @param keys         an array of keys
    * @param partitionNum number of partitions
    * @return RedisKeysRDD of simple Keys stored in redis server
    */
  def fromRedisKeys(keys: Array[String],
                    partitionNum: Int = 3)
                   (implicit
                    redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                    readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RedisKeysRDD = {
    new RedisKeysRDD(sc, redisConfig, readWriteConfig, "", partitionNum, keys)
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum     number of partitions
    * @return RedisKVRDD of simple Key-Values stored in redis server
    */
  def fromRedisKV[T](keysOrKeyPattern: T,
                     partitionNum: Int = 3)
                    (implicit
                     redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                     readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[(String, String)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getKV()
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getKV()
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum     number of partitions
    * @return RedisListRDD of related values stored in redis server
    */
  def fromRedisList[T](keysOrKeyPattern: T,
                       partitionNum: Int = 3)
                      (implicit
                       redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                       readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getList()
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getList()
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum     number of partitions
    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
    */
  def fromRedisSet[T](keysOrKeyPattern: T,
                      partitionNum: Int = 3)
                     (implicit
                      redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                      readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getSet()
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getSet()
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum     number of partitions
    * @return RedisHashRDD of related Key-Values stored in redis server
    */
  def fromRedisHash[T](keysOrKeyPattern: T,
                       partitionNum: Int = 3)
                      (implicit
                       redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                       readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[(String, String)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getHash()
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getHash()
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum     number of partitions
    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
    */
  def fromRedisZSet[T](keysOrKeyPattern: T,
                       partitionNum: Int = 3)
                      (implicit
                       redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                       readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSet()
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSet()
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum     number of partitions
    * @return RedisZSetRDD of related Key-Scores stored in redis server
    */
  def fromRedisZSetWithScore[T](keysOrKeyPattern: T,
                                partitionNum: Int = 3)
                               (implicit
                                redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                                readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[(String, Double)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetWithScore()
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetWithScore()
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param start            start position of target zsets
    * @param end              end position of target zsets
    * @param partitionNum     number of partitions
    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
    */
  def fromRedisZRange[T](keysOrKeyPattern: T,
                         start: Int,
                         end: Int,
                         partitionNum: Int = 3)
                        (implicit
                         redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                         readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetByRange(start, end)
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetByRange(start, end)
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param start            start position of target zsets
    * @param end              end position of target zsets
    * @param partitionNum     number of partitions
    * @return RedisZSetRDD of related Key-Scores stored in redis server
    */
  def fromRedisZRangeWithScore[T](keysOrKeyPattern: T,
                                  start: Int,
                                  end: Int,
                                  partitionNum: Int = 3)
                                 (implicit
                                  redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                                  readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[(String, Double)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetByRangeWithScore(start, end)
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetByRangeWithScore(start, end)
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param min              min score of target zsets
    * @param max              max score of target zsets
    * @param partitionNum     number of partitions
    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
    */
  def fromRedisZRangeByScore[T](keysOrKeyPattern: T,
                                min: Double,
                                max: Double,
                                partitionNum: Int = 3)
                               (implicit
                                redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                                readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetByScore(min, max)
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetByScore(min, max)
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param min              min score of target zsets
    * @param max              max score of target zsets
    * @param partitionNum     number of partitions
    * @return RedisZSetRDD of related Key-Scores stored in redis server
    */
  def fromRedisZRangeByScoreWithScore[T](keysOrKeyPattern: T,
                                         min: Double,
                                         max: Double,
                                         partitionNum: Int = 3)
                                        (implicit
                                         redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                                         readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)):
  RDD[(String, Double)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetByScoreWithScore(min, max)
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetByScoreWithScore(min, max)
      case _ => throw new scala.Exception(IncorrectKeysOrKeyPatternMsg)
    }
  }

  /**
    * @param kvs Pair RDD of K/V
    * @param ttl time to live
    */
  def toRedisKV(kvs: RDD[(String, String)], ttl: Int = 0)
               (implicit
                redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    kvs.foreachPartition(partition => setKVs(partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * Write RDD of binary key-value pairs to Redis String
    *
    * @param kvs Pair RDD of K/V as byte arrays
    * @param ttl time to live
    */
  def toRedisByteKV(kvs: RDD[(Array[Byte], Array[Byte])], ttl: Int = 0)
                   (implicit
                    redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                    readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    kvs.foreachPartition(partition => setByteKVs(partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * @param kvs      Pair RDD of K/V
    * @param hashName target hash's name which hold all the kvs
    * @param ttl      time to live
    */
  def toRedisHASH(kvs: RDD[(String, String)], hashName: String, ttl: Int = 0)
                 (implicit
                  redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                  readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    kvs.foreachPartition(partition => setHash(hashName, partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * Write RDD of (hash name, hash KVs)
    *
    * @param kvs RDD of tuples (hash name, Map(hash field name, hash field value))
    * @param ttl time to live
    */
  def toRedisHASHes(kvs: RDD[(String, Map[String, String])], ttl: Int = 0)
                   (implicit
                    redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                    readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    kvs.foreachPartition(partition => setHash(partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * Write RDD of (hash name, hash KVs). Values are represented as byte array.
    *
    * @param kvs RDD of tuples (hash name, Map(hash field name, hash field value))
    * @param ttl time to live
    */
  def toRedisByteHASHes(kvs: RDD[(Array[Byte], Map[Array[Byte], Array[Byte]])], ttl: Int = 0)
                       (implicit
                        redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                        readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    kvs.foreachPartition(partition => setByteHash(partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * @param kvs      Pair RDD of K/V
    * @param zsetName target zset's name which hold all the kvs
    * @param ttl      time to live
    */
  def toRedisZSET(kvs: RDD[(String, String)], zsetName: String, ttl: Int = 0)
                 (implicit
                  redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                  readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    kvs.foreachPartition(partition => setZset(zsetName, partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * @param vs      RDD of values
    * @param setName target set's name which hold all the vs
    * @param ttl     time to live
    */
  def toRedisSET(vs: RDD[String], setName: String, ttl: Int = 0)
                (implicit
                 redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                 readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    vs.foreachPartition(partition => setSet(setName, partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * @param vs       RDD of values
    * @param listName target list's name which hold all the vs
    * @param ttl      time to live
    */
  def toRedisLIST(vs: RDD[String], listName: String, ttl: Int = 0)
                 (implicit
                  redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                  readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    vs.foreachPartition(partition => setList(listName, partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * Write RDD of (list name, list values) to Redis Lists.
    *
    * @param rdd RDD of tuples (list name, list values)
    * @param ttl time to live
    */
  def toRedisLISTs(rdd: RDD[(String, Seq[String])], ttl: Int = 0)
                  (implicit
                   redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                   readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    rdd.foreachPartition(partition => setList(partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * Write RDD of binary values to Redis Lists.
    *
    * @deprecated use toRedisByteLISTs, the method name has changed to make API consistent
    * @param rdd RDD of tuples (list name, list values)
    * @param ttl time to live
    */
  @Deprecated
  def toRedisByteLIST(rdd: RDD[(Array[Byte], Seq[Array[Byte]])], ttl: Int = 0)
                     (implicit
                      redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                      readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    toRedisByteLISTs(rdd, ttl)(redisConfig, readWriteConfig)
  }

  /**
    * Write RDD of binary values to Redis Lists.
    *
    * @param rdd RDD of tuples (list name, list values)
    * @param ttl time to live
    */
  def toRedisByteLISTs(rdd: RDD[(Array[Byte], Seq[Array[Byte]])], ttl: Int = 0)
                      (implicit
                       redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                       readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    rdd.foreachPartition(partition => setByteList(partition, ttl, redisConfig, readWriteConfig))
  }

  /**
    * @param vs       RDD of values
    * @param listName target list's name which hold all the vs
    * @param listSize target list's size
    *                 save all the vs to listName(list type) in redis-server
    */
  def toRedisFixedLIST(vs: RDD[String],
                       listName: String,
                       listSize: Int = 0)
                      (implicit
                       redisConfig: RedisConfig = RedisConfig.fromSparkConf(sc.getConf),
                       readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)) {
    vs.foreachPartition(partition => setFixedList(listName, listSize, partition, redisConfig, readWriteConfig))
  }
}


object RedisContext extends Serializable {

  /**
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to the target host
    * @param ttl time to live
    */
  def setKVs(arr: Iterator[(String, String)], ttl: Int, redisConfig: RedisConfig, readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val rows = arr.map(kv => (redisConfig.getHost(kv._1).endpoint, kv))
    writeKeys(rows) { case (k, v) =>
      List((pipeline: Pipeline) =>
        if (ttl <= 0) pipeline.set(k, v) else pipeline.set(k, v, SetParams.setParams().ex(ttl.toLong)))
    }
  }

  /**
    * Save KVs as byte arrays.
    *
    * @param arr k/v which should be saved in the target host as bytes
    * @param ttl time to live
    */
  def setByteKVs(arr: Iterator[(Array[Byte], Array[Byte])], ttl: Int, redisConfig: RedisConfig,
                 readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val rows = arr.map(kv => (redisConfig.getHost(kv._1).endpoint, kv))
    writeKeys(rows) { case (k, v) =>
      List((pipeline: Pipeline) =>
        if (ttl <= 0) pipeline.set(k, v) else pipeline.set(k, v, SetParams.setParams().ex(ttl.toLong)))
    }
  }

  /**
    * @param hashName
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to hashName(list type) to the target host
    * @param ttl time to live
    */
  def setHash(hashName: String, arr: Iterator[(String, String)], ttl: Int, redisConfig: RedisConfig,
              readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val fields: Iterator[WriteCommand] = arr.map { case (k, v) =>
      (pipeline: Pipeline) => pipeline.hset(hashName, k, v)
    }
    writeToKey(redisConfig.getHost(hashName).endpoint, fields, expireCommand(hashName, ttl))
  }

  /**
    * @param hashes hashName: map of k/vs to be saved in the target host
    * @param ttl    time to live
    */
  def setHash(hashes: Iterator[(String, Map[String, String])],
              ttl: Int,
              redisConfig: RedisConfig,
              readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val rows = hashes.map { case (key, hashFields) => (redisConfig.getHost(key).endpoint, (key, hashFields)) }
    writeKeys(rows) { case (key, hashFields) =>
      nonEmpty(hashFields)((pipeline: Pipeline) => pipeline.hset(key, hashFields.asJava)) ++
        expireCommand(key, ttl)
    }
  }

  /**
    * @param hashes hashName: map of k/vs to be saved in the target host
    * @param ttl    time to live
    */
  def setByteHash(hashes: Iterator[(Array[Byte], Map[Array[Byte], Array[Byte]])],
                  ttl: Int,
                  redisConfig: RedisConfig,
                  readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val rows = hashes.map { case (key, hashFields) => (redisConfig.getHost(key).endpoint, (key, hashFields)) }
    writeKeys(rows) { case (key, hashFields) =>
      nonEmpty(hashFields)((pipeline: Pipeline) => pipeline.hset(key, hashFields.asJava)) ++
        expireCommand(key, ttl)
    }
  }

  /**
    * @param zsetName
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to zsetName(zset type) to the target host
    * @param ttl time to live
    */
  def setZset(zsetName: String, arr: Iterator[(String, String)], ttl: Int, redisConfig: RedisConfig,
              readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val members: Iterator[WriteCommand] = arr.map { case (k, v) =>
      (pipeline: Pipeline) => pipeline.zadd(zsetName, v.toDouble, k)
    }
    writeToKey(redisConfig.getHost(zsetName).endpoint, members, expireCommand(zsetName, ttl))
  }

  /**
    * @param setName
    * @param arr values which should be saved in the target host
    *            save all the values to setName(set type) to the target host
    * @param ttl time to live
    */
  def setSet(setName: String, arr: Iterator[String], ttl: Int, redisConfig: RedisConfig,
             readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val members: Iterator[WriteCommand] = arr.map(v => (pipeline: Pipeline) => pipeline.sadd(setName, v))
    writeToKey(redisConfig.getHost(setName).endpoint, members, expireCommand(setName, ttl))
  }

  /**
    * @param listName
    * @param arr values which should be saved in the target host
    *            save all the values to listName(list type) to the target host
    * @param ttl time to live
    */
  def setList(listName: String,
              arr: Iterator[String],
              ttl: Int,
              redisConfig: RedisConfig,
              readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val elements: Iterator[WriteCommand] = arr.map(v => (pipeline: Pipeline) => pipeline.rpush(listName, v))
    writeToKey(redisConfig.getHost(listName).endpoint, elements, expireCommand(listName, ttl))
  }

  def setByteList(keyValues: Iterator[(Array[Byte], Seq[Array[Byte]])],
                  ttl: Int,
                  redisConfig: RedisConfig,
                  readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val rows = keyValues.map { case (key, listValues) => (redisConfig.getHost(key).endpoint, (key, listValues)) }
    writeKeys(rows) { case (key, listValues) =>
      nonEmpty(listValues)((pipeline: Pipeline) => pipeline.rpush(key, listValues: _*)) ++ expireCommand(key, ttl)
    }
  }

  def setList(keyValues: Iterator[(String, Seq[String])],
              ttl: Int,
              redisConfig: RedisConfig,
              readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val rows = keyValues.map { case (key, listValues) => (redisConfig.getHost(key).endpoint, (key, listValues)) }
    writeKeys(rows) { case (key, listValues) =>
      nonEmpty(listValues)((pipeline: Pipeline) => pipeline.rpush(key, listValues: _*)) ++ expireCommand(key, ttl)
    }
  }

  /**
    * @param key
    * @param listSize
    * @param arr values which should be saved in the target host
    *            save all the values to listName(list type) to the target host
    */
  def setFixedList(key: String,
                   listSize: Int,
                   arr: Iterator[String],
                   redisConfig: RedisConfig,
                   readWriteConfig: ReadWriteConfig) {
    implicit val rwConf: ReadWriteConfig = readWriteConfig
    val elements: Iterator[WriteCommand] = arr.map(v => (pipeline: Pipeline) => pipeline.lpush(key, v))
    val trim: List[WriteCommand] =
      if (listSize > 0) List((pipeline: Pipeline) => pipeline.ltrim(key, 0, listSize - 1)) else Nil
    writeToKey(redisConfig.getHost(key).endpoint, elements, trim)
  }

  // redisConfig's topology was read on the driver and can be out of date, so every writer goes through
  // writeWithRedirects: it follows MOVED and ASK to the slot's current owner, and fails on any other error reply
  // instead of dropping the write (#347).

  /**
    * Writes commands for one key to the endpoint that owns it, in order: `commands` in batches of
    * `rddWriteIteratorGroupingSize`, then `last` (an `EXPIRE` or `LTRIM` that must follow them).
    */
  private def writeToKey(endpoint: RedisEndpoint, commands: Iterator[WriteCommand], last: List[WriteCommand])(
      implicit readWriteConfig: ReadWriteConfig): Unit = {
    commands.grouped(readWriteConfig.rddWriteIteratorGroupingSize).foreach { batch =>
      writeWithRedirects(batch.map(command => (endpoint, command)))(command => List(command))
    }
    if (last.nonEmpty) {
      writeWithRedirects(last.map(command => (endpoint, command)))(command => List(command))
    }
  }

  /** Writes rows of several keys, each to the endpoint that owns its key, in `rddWriteIteratorGroupingSize` batches. */
  private def writeKeys[A](rows: Iterator[(RedisEndpoint, A)])(commands: A => List[WriteCommand])(
      implicit readWriteConfig: ReadWriteConfig): Unit =
    rows.grouped(readWriteConfig.rddWriteIteratorGroupingSize).foreach(batch => writeWithRedirects(batch)(commands))

  /** `command`, unless `values` is empty: Redis rejects an `HSET` or `RPUSH` with nothing to set or push. */
  private def nonEmpty(values: Iterable[_])(command: WriteCommand): List[WriteCommand] =
    if (values.nonEmpty) List(command) else Nil

  /** The `EXPIRE` that follows a key's write, when `ttl` is set. */
  private def expireCommand(key: String, ttl: Int): List[WriteCommand] =
    if (ttl > 0) List((pipeline: Pipeline) => pipeline.expire(key, ttl.toLong)) else Nil

  private def expireCommand(key: Array[Byte], ttl: Int): List[WriteCommand] =
    if (ttl > 0) List((pipeline: Pipeline) => pipeline.expire(key, ttl.toLong)) else Nil
}

trait RedisFunctions {

  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)

}

