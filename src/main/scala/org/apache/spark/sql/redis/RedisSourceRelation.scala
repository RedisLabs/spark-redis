package org.apache.spark.sql.redis

import java.util.{UUID, List => JList, Map => JMap}

import com.redislabs.provider.redis.rdd.Keys
import com.redislabs.provider.redis.util.Logger
import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, RedisNode, toRedisContext}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.redis.RedisSourceRelation._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import redis.clients.jedis.Protocol

import scala.collection.JavaConversions
import scala.collection.JavaConversions._

class RedisSourceRelation(override val sqlContext: SQLContext,
                          parameters: Map[String, String],
                          userSpecifiedSchema: Option[StructType])
  extends BaseRelation
    with InsertableRelation
    with PrunedFilteredScan
    with Keys
    with Serializable {

  private implicit val redisConfig: RedisConfig = {
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

  @transient private val sc = sqlContext.sparkContext
  // TODO: allow to specify user parameter
  private val tableName: String = parameters.get("path")
    // TODO: sql parser gives table absolute path for non-temporary tables
    //    .map(p => Paths.get(p).getFileName.toString)
    .getOrElse {
    throw new IllegalArgumentException("'path' parameter is not specified")
  }
  private val keyColumn = parameters.get(SqlOptionKeyColumn)
  private val numPartitions = parameters.get(SqlOptionNumPartitions).map(_.toInt)
    .getOrElse(SqlOptionNumPartitionsDefault)
  private val inferSchemaEnabled = parameters.get(SqlOptionInferSchema).exists(_.toBoolean)
  private val persistenceModel = parameters.getOrDefault(SqlOptionModel, SqlOptionModelHash)
  private val persistence = RedisPersistence(persistenceModel)
  private val ttl = parameters.get(SqlOptionTTL).map(_.toInt).getOrElse(0)
  @volatile private var currentSchema: StructType = _

  override def schema: StructType = {
    if (currentSchema == null) {
      currentSchema = userSpecifiedSchema
        .getOrElse {
          if (inferSchemaEnabled) {
            inferSchema()
          } else {
            loadSchema()
          }
        }
    }
    currentSchema
  }

  private def inferSchema(): StructType = {
    val keys = sc.fromRedisKeyPattern(dataKeyPattern(tableName))
    if (keys.isEmpty()) {
      throw new IllegalStateException("No key is available")
    } else {
      val firstKey = keys.first()
      val node = getMasterNode(redisConfig.hosts, firstKey)
      scanRows(node, Seq(firstKey), filterColumns = false, Seq())
        .collectFirst {
          case r: Row => r.schema
        }
        .getOrElse {
          throw new IllegalStateException("No row is available")
        }
    }
  }

  private def dataKeyId(row: Row): String = {
    val id = keyColumn.map(id => row.getAs[Any](id)).map(_.toString).getOrElse(uuid())
    dataKey(tableName, id)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val schema = userSpecifiedSchema.getOrElse(data.schema)
    // write schema, so that we can load dataframe back
    currentSchema = saveSchema(schema)
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
      val rowsWithKey: Map[String, Row] = partition.map(row => dataKeyId(row) -> row).toMap
      groupKeysByNode(redisConfig.hosts, rowsWithKey.keysIterator).foreach { case (node, keys) =>
        val conn = node.connect()
        val pipeline = conn.pipelined()
        keys.foreach { key =>
          // Logger.info(s"saving key $key")
          val row = rowsWithKey(key)
          // serialize the entire row to byte array
          // TODO: remove schema from row
          // TODO: save as a hash
          val encodedRow = persistence.encodeRow(row)
          persistence.save(pipeline, key, encodedRow, ttl)
        }
        pipeline.sync()
        conn.close()
      }
    }
  }

  def isEmpty: Boolean =
    sc.fromRedisKeyPattern(dataKeyPattern(tableName)).isEmpty()

  def nonEmpty: Boolean = !isEmpty

  def saveSchema(schema: StructType): StructType = {
    val key = schemaKey(tableName)
    Logger.info(s"saving schema $key")
    val schemaNode = getMasterNode(redisConfig.hosts, key)
    val conn = schemaNode.connect()
    val schemaBytes = SerializationUtils.serialize(schema)
    conn.set(key.getBytes, schemaBytes)
    conn.close()
    schema
  }

  def loadSchema(): StructType = {
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
    val keysRdd = sc.fromRedisKeyPattern(dataKeyPattern(tableName), partitionNum = numPartitions)
    keysRdd.mapPartitions { partition =>
      groupKeysByNode(redisConfig.hosts, partition)
        .flatMap { case (node, keys) =>
          scanRows(node, keys, filterColumns = true, requiredColumns)
        }
        .iterator
    }
  }

  private def scanRows(node: RedisNode, keys: Seq[String], filterColumns: Boolean,
                       requiredColumns: Seq[String]): TraversableOnce[Row] = {
    def filteredSchema(): StructType = {
      val requiredColumnsSet = Set(requiredColumns: _*)
      val filteredFields = schema.fields
        .filter { f =>
          requiredColumnsSet.contains(f.name)
        }
      StructType(filteredFields)
    }

    val conn = node.connect()
    val pipeline = conn.pipelined()
    keys
      .foreach { key =>
        Logger.info(s"key $key")
        persistence.load(pipeline, key, requiredColumns)
      }
    val pipelineValues = pipeline.syncAndReturnAll()
    val rows =
      if (!filterColumns || persistenceModel == SqlOptionModelBinary) {
        pipelineValues
          .map {
            case jmap: JMap[_, _] => jmap.toMap
            case value: Any => value
          }
          .map { value =>
            persistence.decodeRow(value, schema, inferSchemaEnabled)
          }
      } else if (requiredColumns.isEmpty) {
        pipelineValues.map { _ =>
          new GenericRow(Array[Any]())
        }
      } else {
        pipelineValues.map { case values: JList[String] =>
          val value = requiredColumns.zip(values).toMap
          persistence.decodeRow(value, filteredSchema(), inferSchemaEnabled)
        }
      }
    conn.close()
    rows
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters
}

object RedisSourceRelation {

  private val SchemaNamespace = "schema"

  private val DataNamespace = "data"

  def schemaKey(tableName: String): String = s"$tableName:$SchemaNamespace"

  def dataKey(tableName: String, id: String = uuid()): String = {
    s"$tableName:$DataNamespace:$id"
  }

  def uuid(): String = UUID.randomUUID().toString.replace("-", "")

  def dataKeyPattern(tableName: String): String = s"$tableName:$DataNamespace:*"
}
