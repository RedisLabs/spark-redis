package com.redislabs.provider.redis.sql

import scala.collection.JavaConversions._
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.rdd.{Keys, RedisKeysRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import redis.clients.jedis.Protocol
import redis.clients.util.JedisClusterCRC16
import java.security.MessageDigest


case class RedisRelation(parameters: Map[String, String], userSchema: StructType)
                        (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation with Keys {

  val tableName: String = parameters.getOrElse("table", "PANIC")

  val redisConfig: RedisConfig = {
    new RedisConfig({
        if ((parameters.keySet & Set("host", "port", "auth", "dbNum", "timeout")).size == 0) {
          new RedisEndpoint(sqlContext.sparkContext.getConf)
        } else {
          val host = parameters.getOrElse("host", Protocol.DEFAULT_HOST)
          val port = parameters.getOrElse("port", Protocol.DEFAULT_PORT.toString).toInt
          val auth = parameters.getOrElse("auth", null)
          val dbNum = parameters.getOrElse("dbNum", Protocol.DEFAULT_DATABASE.toString).toInt
          val timeout = parameters.getOrElse("timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt
          new RedisEndpoint(host, port, auth, dbNum, timeout)
        }
      }
    )
  }

  val partitionNum: Int = parameters.getOrElse("partitionNum", 3.toString).toInt

  val schema = userSchema

  def getNode(key: String): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    /* Master only */
    redisConfig.hosts.filter(node => { node.startSlot <= slot && node.endSlot >= slot }).filter(_.idx == 0)(0)
  }
  def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.foreach{
      row => {
        val key = tableName + ":" + MessageDigest.getInstance("MD5").digest(System.currentTimeMillis.toString.getBytes)
        val conn = getNode(key).endpoint.connect
        conn.hmset(key, row.getValuesMap(row.schema.fieldNames).map(x => (x._1, x._2.toString)))
        conn.close
      }
    }
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new RedisKeysRDD(sqlContext.sparkContext, redisConfig, tableName + ":*", partitionNum, null).
      mapPartitions {
        partition: Iterator[String] => {
          groupKeysByNode(redisConfig.hosts, partition).flatMap {
            x => {
              val conn = x._1.endpoint.connect()
              val rowKeys: Array[String] = filterKeysByType(conn, x._2, "hash")
              val res = rowKeys.map {
                key => {
                  val res = conn.hmget(key, schema.fieldNames: _*)
                  val pass = filters.zip(filters.map(filter => res.get((schema.fieldIndex(getAttr(filter)))))).forall{
                    x => parseFilter(x._1, x._2)
                  }
                  if (pass) {
                    requiredColumns.map{
                      c => {
                        val idx = schema.fieldIndex(c)
                        castToTarget(res.get(idx), schema.fields(idx))
                      }
                    }
                  } else {
                    null
                  }
                }
              }
              conn.close
              res.filter(_!=null)
            }
          }.toIterator.map(x => Row.fromSeq(x))
        }
      }
  }

  private def getAttr(f: Filter): String = {
    f match {
      case EqualTo(attribute, value) => attribute
      case GreaterThan(attribute, value) => attribute
      case GreaterThanOrEqual(attribute, value) => attribute
      case LessThan(attribute, value) => attribute
      case LessThanOrEqual(attribute, value) => attribute
      case In(attribute, values) => attribute
      case IsNull(attribute) => attribute
      case IsNotNull(attribute) => attribute
      case StringStartsWith(attribute, value) => attribute
      case StringEndsWith(attribute, value) => attribute
      case StringContains(attribute, value) => attribute
    }
  }

  private def castToTarget(value: String, field: StructField) = {
    field.dataType match {
      case IntegerType => value.toString.toInt
      case DoubleType => value.toString.toDouble
      case StringType => value.toString
      case _ => value.toString
    }
  }

  private def getDataType(attr: String) = {
    schema.fields(schema.fieldIndex(attr)).dataType
  }
  private def parseFilter(f: Filter, target: String) = {
    f match {
      case EqualTo(attribute, value) => {
        value.toString == target
      }
      case GreaterThan(attribute, value) => {
        getDataType(attribute) match {
          case IntegerType => value.toString.toInt < target.toInt
          case DoubleType => value.toString.toDouble < target.toDouble
          case StringType => value.toString < target
          case _ => value.toString < target
        }
      }
      case GreaterThanOrEqual(attribute, value) => {
        getDataType(attribute) match {
          case IntegerType => value.toString.toInt <= target.toInt
          case DoubleType => value.toString.toDouble <= target.toDouble
          case StringType => value.toString <= target
          case _ => value.toString <= target
        }
      }
      case LessThan(attribute, value) => {
        getDataType(attribute) match {
          case IntegerType => value.toString.toInt > target.toInt
          case DoubleType => value.toString.toDouble > target.toDouble
          case StringType => value.toString > target
          case _ => value.toString > target
        }
      }
      case LessThanOrEqual(attribute, value) => {
        getDataType(attribute) match {
          case IntegerType => value.toString.toInt >= target.toInt
          case DoubleType => value.toString.toDouble >= target.toDouble
          case StringType => value.toString >= target
          case _ => value.toString >= target
        }
      }
      case In(attribute, values) => {
        getDataType(attribute) match {
          case IntegerType => values.map(_.toString.toInt).contains(target.toInt)
          case DoubleType => values.map(_.toString.toDouble).contains(target.toDouble)
          case StringType => values.map(_.toString).contains(target)
          case _ => values.map(_.toString).contains(target)
        }
      }
      case IsNull(attribute) => target == null
      case IsNotNull(attribute) => target != null
      case StringStartsWith(attribute, value) => target.startsWith(value.toString)
      case StringEndsWith(attribute, value) => target.endsWith(value.toString)
      case StringContains(attribute, value) => target.contains(value.toString)
      case _ => false
    }
  }
}

class DefaultSource extends SchemaRelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    RedisRelation(parameters, schema)(sqlContext)
  }
}

