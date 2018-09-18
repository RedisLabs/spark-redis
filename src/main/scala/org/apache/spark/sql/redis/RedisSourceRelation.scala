package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets
import java.util.UUID

import com.redislabs.provider.redis.rdd.{Keys, RedisKeysRDD}
import com.redislabs.provider.redis.util.Logger
import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, toRedisContext}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.redis.RedisSourceRelation._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import redis.clients.jedis.{Pipeline, Protocol, Response}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._

abstract class RedisSourceRelation[T](override val sqlContext: SQLContext,
                                      parameters: Map[String, String],
                                      userSpecifiedSchema: Option[StructType])
  extends BaseRelation
    with InsertableRelation
    with PrunedFilteredScan
    with Keys
    with Serializable {

  val redisConfig: RedisConfig = {
    new RedisConfig(
      if ((parameters.keySet & Set("host", "port", "auth", "dbNum", "timeout")).isEmpty) {
        new RedisEndpoint(sqlContext.sparkContext.getConf)
      } else {
        val host = parameters.getOrElse("host", Protocol.DEFAULT_HOST)
        val port = parameters.get("port").map(_.toInt).getOrElse(Protocol.DEFAULT_PORT)
        val auth = parameters.getOrElse("auth", null)
        val dbNum = parameters.get("dbNum").map(_.toInt).getOrElse(Protocol.DEFAULT_DATABASE)
        val timeout = parameters.get("timeout").map(_.toInt).getOrElse(Protocol.DEFAULT_TIMEOUT)
        RedisEndpoint(host, port, auth, dbNum, timeout)
      }
    )
  }

  // TODO: allow to specify user parameter
  val tableName: String = parameters.getOrElse("path", throw new IllegalArgumentException("'path' parameter is not specified"))
  private val numPartitions = parameters.get(SqlOptionNumPartitions).map(_.toInt)
    .getOrElse(SqlOptionNumPartitionsDefault)
  @volatile private var currentSchema: StructType = _

  override def schema: StructType = {
    if (currentSchema == null) {
      currentSchema = userSpecifiedSchema.getOrElse(loadSchema(tableName))
    }
    currentSchema
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val schema = userSpecifiedSchema.getOrElse(data.schema)
    if (currentSchema != schema) {
      // write schema, so that we can load dataframe back
      currentSchema = saveSchema(schema, tableName)
    }

    if (overwrite) {
      // truncate the table
      redisConfig.hosts.foreach { node =>
        val conn = node.connect()
        val keys = conn.keys(dataKeyPattern(tableName))
        if (keys.nonEmpty) {
          val keySeq = JavaConversions.asScalaSet(keys).toSeq
          conn.del(keySeq: _*)
        }
        conn.close()
      }
    }

    // write data
    data.foreachPartition { partition =>
      // TODO: allow user to specify key column
      val rowsWithKey: Map[String, Row] = partition.map(row => dataKey(tableName) -> row).toMap
      groupKeysByNode(redisConfig.hosts, rowsWithKey.keysIterator).foreach { case (node, keys) =>
        val conn = node.connect()
        val pipeline = conn.pipelined()
        keys.foreach { key =>
          //          Logger.info(s"saving key $key")
          val row = rowsWithKey(key)
          // serialize the entire row to byte array
          // TODO: remove schema from row
          // TODO: save as a hash
          val encodedKey = key.getBytes(StandardCharsets.UTF_8)
          val encodedRow = encodeRow(row)
          save(pipeline, encodedKey, encodedRow)
        }
        pipeline.sync()
        conn.close()
      }
    }
  }

  def isEmpty: Boolean =
    sqlContext.sparkContext.fromRedisKeyPattern(dataKeyPattern(tableName))
      .isEmpty()

  def nonEmpty: Boolean = !isEmpty

  // TODO: reuse connection to node?
  def saveSchema(schema: StructType, tableName: String): StructType = {
    val key = schemaKey(tableName)
    Logger.info(s"saving schema $key")
    val schemaNode = getMasterNode(redisConfig.hosts, key)
    val conn = schemaNode.connect()
    val schemaBytes = SerializationUtils.serialize(schema)
    conn.set(key.getBytes, schemaBytes)
    conn.close()
    schema
  }

  // TODO: reuse connection to node?
  def loadSchema(tableName: String): StructType = {
    val key = schemaKey(tableName)
    Logger.info(s"loading schema $key")
    val schemaNode = getMasterNode(redisConfig.hosts, key)
    val conn = schemaNode.connect()
    val schemaBytes = conn.get(key.getBytes)
    val schema = SerializationUtils.deserialize[StructType](schemaBytes)
    conn.close()
    schema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    Logger.info("build scan")
    val keysRdd = new RedisKeysRDD(sqlContext.sparkContext, redisConfig, dataKeyPattern(tableName),
      partitionNum = numPartitions)
    keysRdd.mapPartitions { partition =>
      groupKeysByNode(redisConfig.hosts, partition).flatMap { case (node, keys) =>
        val conn = node.connect()
        val pipeline = conn.pipelined()
        keys
          .foreach { key =>
            Logger.info(s"key $key")
            val encodedKey = key.getBytes(StandardCharsets.UTF_8)
            load(pipeline, encodedKey)
          }
        val rows = pipeline.syncAndReturnAll()
          .map {
            _.asInstanceOf[T]
          }
          .map { value =>
            decodeRow(value)
          }
        conn.close()
        rows
      }.iterator
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  def save(pipeline: Pipeline, key: Array[Byte], value: T): Unit

  def load(pipeline: Pipeline, key: Array[Byte]): Response[T]

  def encodeRow(value: Row): T

  def decodeRow(value: T): Row
}

object RedisSourceRelation {

  private val SchemaNamespace = "schema"

  private val DataNamespace = "data"

  def schemaKey(tableName: String): String = s"$tableName:$SchemaNamespace"

  def dataKey(tableName: String): String = {
    val uuid = UUID.randomUUID().toString.replace("-", "")
    s"$tableName:$DataNamespace:$uuid"
  }

  def dataKeyPattern(tableName: String): String = s"$tableName:$DataNamespace:*"
}