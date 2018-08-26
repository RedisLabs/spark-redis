package org.apache.spark.sql.redis

import java.util.UUID

import com.redislabs.provider.redis.rdd.{Keys, RedisKeysRDD}
import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import redis.clients.jedis.Protocol

//import scala.collection.JavaConverters._
import RedisSourceRelation._
import org.apache.commons.lang3.SerializationUtils
import scala.collection.JavaConversions._

// TODO: extends
class RedisSourceRelation(override val sqlContext: SQLContext,
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
        val port = parameters.getOrElse("port", Protocol.DEFAULT_PORT.toString).toInt
        val auth = parameters.getOrElse("auth", null)
        val dbNum = parameters.getOrElse("dbNum", Protocol.DEFAULT_DATABASE.toString).toInt
        val timeout = parameters.getOrElse("timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt
        RedisEndpoint(host, port, auth, dbNum, timeout)
      }
    )
  }

  // TODO: allow to specify user parameter
  val tableName: String = parameters.getOrElse("path", throw new IllegalArgumentException("'path' parameter is not specified"))

  override def schema: StructType = {
    // TODO:
    userSpecifiedSchema.getOrElse(StructType(Array[StructField]()))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // write schema, so that we can load dataframe back
    saveSchema(schema, tableName)

    // write data
    data.foreachPartition { partition =>
      // TODO: allow user to specify key column
      val rowsWithKey: Map[String, Row] = partition.map(row => generateKey(tableName) -> row).toMap
      groupKeysByNode(redisConfig.hosts, rowsWithKey.keysIterator).foreach { case (node, keys) =>
        val conn = node.connect()
        val pipeline = conn.pipelined()
        keys.foreach { key =>
          println(s"saving key $key")
          val row = rowsWithKey(key)
          val hash = row
            .getValuesMap[java.lang.Object](row.schema.fieldNames)
            .map { case (fieldName, value) => (fieldName, value.toString) }
          pipeline.hmset(key, hash)
        }
        pipeline.sync()
        conn.close()
      }
    }
  }

  def generateKey(prefix: String): String = {
    val uuid = UUID.randomUUID().toString.replace("-", "")
    s"$prefix:$uuid"
  }

  // TODO: reuse connection to node?
  def saveSchema(schema: StructType, tableName: String): Unit = {
    val schemaKey = schemaRedisKey(tableName)
    println(s"saving schema $schemaKey")
    val schemaNode = getMasterNode(redisConfig.hosts, schemaKey)
    val conn = schemaNode.connect()
    val schemaBytes = SerializationUtils.serialize(schema)
    conn.set(schemaKey.getBytes, schemaBytes)
    conn.close()
  }

  // TODO: reuse connection to node?
  def loadSchema(tableName: String): StructType = {
    val schemaKey = schemaRedisKey(tableName)
    println(s"loading schema $schemaKey")
    val schemaNode = getMasterNode(redisConfig.hosts, schemaKey)
    val conn = schemaNode.connect()
    val schemaBytes = conn.get(schemaKey.getBytes)
    val schema = SerializationUtils.deserialize(schemaBytes)
    conn.close()
    schema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("build scan")
    val schema = loadSchema(tableName)
    // TODO: partition num
    val keysRdd = new RedisKeysRDD(sqlContext.sparkContext, redisConfig, tableName + ":*")
    keysRdd.mapPartitions { partition =>
      groupKeysByNode(redisConfig.hosts, partition).map { case (node, keys) =>
        val conn = node.connect()
        val pipeline = conn.pipelined()
        keys.foreach { key =>
          // TODO
        val a = pipeline.hmget(key, requiredColumns:_*)
        }
        pipeline.sync()
        conn.close()
      }
      ???
    }

    ???
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters
}

object RedisSourceRelation {

  private val SchemaKey = "dataframe_schema"

  def schemaRedisKey(tableName: String): String =  s"$tableName:$SchemaKey"

}