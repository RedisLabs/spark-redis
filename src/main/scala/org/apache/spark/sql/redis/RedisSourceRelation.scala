package org.apache.spark.sql.redis

import java.io.InvalidClassException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.{List => JList}

import com.redislabs.provider.redis.rdd.Keys
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.{FieldParseException, Logging, RedisCapabilities, WriteCapabilities}
import com.redislabs.provider.redis.util.PipelineUtils._
import com.redislabs.provider.redis.{ReadWriteConfig, RedisConfig, RedisDataTypeHash, RedisDataTypeString, RedisEndpoint, RedisNode, toRedisContext}
import org.apache.commons.lang3.{SerializationException, SerializationUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.redis.RedisSourceRelation._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.{Pipeline, PipelineBase, Protocol}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class RedisSourceRelation(override val sqlContext: SQLContext,
                          parameters: Map[String, String],
                          userSpecifiedSchema: Option[StructType])
  extends BaseRelation
    with InsertableRelation
    with PrunedFilteredScan
    with Keys
    with Serializable
    with Logging {

  private implicit val redisConfig: RedisConfig = RedisConfig.fromSparkConfAndParameters(
    sqlContext.sparkContext.getConf, parameters)

  implicit private val readWriteConfig: ReadWriteConfig = {
    val global = ReadWriteConfig.fromSparkConf(sqlContext.sparkContext.getConf)
    // override global config with dataframe specific settings
    global.copy(
      scanCount = parameters.get(SqlOptionScanCount).map(_.toInt).getOrElse(global.scanCount),
      maxPipelineSize = parameters.get(SqlOptionMaxPipelineSize).map(_.toInt).getOrElse(global.maxPipelineSize)
    )
  }

  logInfo(s"Redis config initial host: ${redisConfig.initialHost}")

  @transient private val sc = sqlContext.sparkContext

  /**
    * Will be filled while saving data to Redis or reading from Redis.
    */
  @volatile private var currentSchema: StructType = _

  /** parameters (sorted alphabetically) **/
  private val filterKeysByTypeEnabled = parameters.get(SqlOptionFilterKeysByType).exists(_.toBoolean)
  private val inferSchemaEnabled = parameters.get(SqlOptionInferSchema).exists(_.toBoolean)
  private val iteratorGroupingSize = parameters.get(SqlOptionIteratorGroupingSize).map(_.toInt)
    .getOrElse(SqlOptionIteratorGroupingSizeDefault)
  private val keyColumn = parameters.get(SqlOptionKeyColumn)
  private val keyName = keyColumn.getOrElse("_id")
  private val keysPatternOpt: Option[String] = parameters.get(SqlOptionKeysPattern)
  private val numPartitions = parameters.get(SqlOptionNumPartitions).map(_.toInt)
    .getOrElse(SqlOptionNumPartitionsDefault)
  private val persistenceModel = parameters.getOrElse(SqlOptionModel, SqlOptionModelHash)
  private val tableNameOpt: Option[String] = parameters.get(SqlOptionTableName)
  private val ttl = parameters.get(SqlOptionTTL).map(_.toInt).getOrElse(0)
  private val writeMode = parseWriteMode(parameters)
  /** `write.mode = replace` on the hash model. A binary-model row is one value, which a write always replaces. */
  private val replaceHashes = writeMode == SqlOptionWriteModeReplace && persistenceModel != SqlOptionModelBinary
  private val persistence =
    if (replaceHashes) new HashRedisPersistence(replace = true) else RedisPersistence(persistenceModel)

  /**
    * redis key pattern for rows, based either on the 'keys.pattern' or 'table' parameter
    */
  private val dataKeyPattern = keysPatternOpt
    .orElse(tableNameOpt.map(tableName => tableDataKeyPattern(tableName)))
    .getOrElse {
      val msg = s"Neither '$SqlOptionKeysPattern' or '$SqlOptionTableName' option is set."
      throw new IllegalArgumentException(msg)
    }

  /**
    * Support key column extraction from Redis prefix pattern. Otherwise,
    * return Redis key unmodified.
    */
  private val keysPrefixPattern =
    if (dataKeyPattern.endsWith("*") && dataKeyPattern.count(_ == '*') == 1) {
      dataKeyPattern
    } else {
      ""
    }

  // check specified parameters
  if (tableNameOpt.isDefined && keysPatternOpt.isDefined) {
    throw new IllegalArgumentException(s"Both options '$SqlOptionKeysPattern' and '$SqlOptionTableName' are set. " +
      s"You should only use either one.")
  }

  override def schema: StructType = {
    if (currentSchema == null) {
      currentSchema = userSpecifiedSchema.getOrElse {
        if (inferSchemaEnabled) inferSchema() else loadSchema()
      }
    }
    currentSchema
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // The binary model writes a TTL with MSETEX where every primary has it, so ask once per write, here on the driver.
    val binaryTtlWrite: Option[(BinaryRedisPersistence, WriteCapabilities)] = (persistence: AnyRef) match {
      case binary: BinaryRedisPersistence if ttl > 0 => Some(binary -> RedisCapabilities.forWrite(redisConfig))
      case _ => None
    }
    val schema = userSpecifiedSchema.getOrElse(data.schema)
    // write schema, so that we can load dataframe back
    currentSchema = saveSchema(schema)
    if (overwrite) {
      truncate()
    }

    // write data
    data.foreachPartition { partition: Iterator[Row] =>
      // grouped iterator to only allocate memory for a portion of rows
      partition.grouped(iteratorGroupingSize).foreach { batch =>
        // the following can be optimized to not create a map
        val rowsWithKey: Map[String, Row] = batch.map(row => dataKeyId(row) -> row).toMap
        groupKeysByNode(redisConfig.hosts, rowsWithKey.keysIterator).foreach { case (node, keys) =>
          saveRows(node, keys, rowsWithKey, binaryTtlWrite)
        }
      }
    }
    // If dataframe contains only key column
    val colCount = data.columns.length
    if (colCount < 2) {
      logInfo(s"Dataframe only contains key.column specified in options. No data was writen to redis.")
    }
  }

  /**
    * Deletes the table's rows. The topology is read again for this: the relation's copy can be out of date, and a key
    * scan finds nothing on a slot's old owner once the slot has moved. The deletes follow MOVED and ASK (#347).
    */
  private def truncate(): Unit = {
    val currentConfig = new RedisConfig(redisConfig.initialHost)
    sc.fromRedisKeyPattern(dataKeyPattern)(currentConfig, readWriteConfig).foreachPartition { partition =>
      partition.grouped(iteratorGroupingSize).foreach { keys =>
        writeWithRedirects(keys.map(key => (currentConfig.getHost(key).endpoint, key))) { key =>
          // the type ascription fixes an ambiguous reference to an overloaded definition
          List((pipeline: Pipeline) => (pipeline: PipelineBase).del(key))
        }
      }
    }
  }

  /** Writes the rows of `keys`, which the relation's topology puts on `node`. */
  private def saveRows(node: RedisNode, keys: Array[String], rowsWithKey: Map[String, Row],
                       binaryTtlWrite: Option[(BinaryRedisPersistence, WriteCapabilities)]): Unit =
    binaryTtlWrite match {
      case Some((binary, capabilities)) =>
        // one MSETEX per batch (per slot on a cluster), or SET ... EX per row; a rejected write fails the task
        withConnection(node.connect()) { conn =>
          val encodedRows = keys.map(key => key -> binary.encodeRow(keyName, rowsWithKey(key)))
          binary.saveWithTtl(conn, encodedRows, ttl, capabilities)
        }
      case None if replaceHashes =>
        // A replace that Redis refused fails the task instead of leaving the old row in place silently. Unlike
        // foreachWithPipeline, this also sees the errors inside each EXEC reply.
        withConnection(node.connect()) { conn =>
          val save = (pipeline: Pipeline, key: String) =>
            persistence.save(pipeline, key, persistence.encodeRow(keyName, rowsWithKey(key)), ttl)
          failOnErrorReplies(mapWithPipeline(conn, keys)(save))
        }
      case None =>
        // Follows MOVED and ASK to each key's current owner, since the relation's topology can be out of date, and
        // fails on any other error reply instead of dropping the row (#347).
        val encodedRows = keys.toList.map { key =>
          (node.endpoint, (key, persistence.encodeRow(keyName, rowsWithKey(key))))
        }
        writeWithRedirects(encodedRows) { case (key, encodedRow) =>
          persistence.saveCommands(key, encodedRow, ttl)
        }
    }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo("build scan")
    val keysRdd = sc.fromRedisKeyPattern(dataKeyPattern, partitionNum = numPartitions)
    if (requiredColumns.isEmpty) {
      keysRdd.map { _ =>
        new GenericRow(Array[Any]())
      }
    } else {
      // filter schema columns, it should be in the same order as given 'requiredColumns'
      val requiredSchema = {
        val fieldsMap = schema.fields.map(f => (f.name, f)).toMap
        val requiredFields = requiredColumns.map { c =>
          fieldsMap(c)
        }
        StructType(requiredFields)
      }
      val keyType =
        if (persistenceModel == SqlOptionModelBinary) {
          RedisDataTypeString
        } else {
          RedisDataTypeHash
        }
      keysRdd.mapPartitions { partition =>
        // grouped iterator to only allocate memory for a portion of rows
        partition.grouped(iteratorGroupingSize).flatMap { batch =>
          groupKeysByNode(redisConfig.hosts, batch.iterator)
            .flatMap { case (node, keys) =>
              scanRows(node, keys, keyType, requiredSchema, requiredColumns)
            }
        }
      }
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  /**
    * @return true if data exists in redis
    */
  def isEmpty: Boolean = {
    sc.fromRedisKeyPattern(dataKeyPattern, partitionNum = numPartitions).isEmpty()
  }

  /**
    * @return true if no data exists in redis
    */
  def nonEmpty: Boolean = {
    !isEmpty
  }

  /**
    * @return table name
    */
  private def tableName(): String = {
    tableNameOpt.getOrElse(throw new IllegalArgumentException(s"Option '$SqlOptionTableName' is not set."))
  }

  /**
    * @return redis key for the row
    */
  private def dataKeyId(row: Row): String = {
    val id = keyColumn.map(id => row.getAs[Any](id)).map(_.toString).getOrElse(uuid())
    dataKey(tableName(), id)
  }

  /**
    * infer schema from a random redis row
    */
  private def inferSchema(): StructType = {
    if (persistenceModel != SqlOptionModelHash) {
      throw new IllegalArgumentException(s"Cannot infer schema from model '$persistenceModel'. " +
        s"Currently, only '$SqlOptionModelHash' is supported")
    }
    val keys = sc.fromRedisKeyPattern(dataKeyPattern)
    if (keys.isEmpty()) {
      throw new IllegalStateException("No key is available")
    } else {
      val firstKey = keys.first()
      val node = getMasterNode(redisConfig.hosts, firstKey)
      withConnection(node.connect()) { conn =>
        val results = conn.hgetAll(firstKey).asScala.toSeq :+ keyName -> firstKey
        val fields = results.map(kv => StructField(kv._1, StringType)).toArray
        StructType(fields)
      }
    }
  }

  /**
    * write schema to redis
    */
  private def saveSchema(schema: StructType): StructType = {
    val key = schemaKey(tableName())
    logInfo(s"saving schema $key")
    val schemaNode = getMasterNode(redisConfig.hosts, key)
    val schemaBytes = encodeSchema(schema)
    withConnection(schemaNode.connect()) { conn =>
      conn.set(key.getBytes, schemaBytes)
    }
    schema
  }

  /**
    * read schema from redis
    */
  private def loadSchema(): StructType = {
    val key = schemaKey(tableName())
    logInfo(s"loading schema $key")
    val schemaNode = getMasterNode(redisConfig.hosts, key)
    val schemaBytes = withConnection(schemaNode.connect()) { conn =>
      conn.get(key.getBytes)
    }
    if (schemaBytes == null) {
      throw new IllegalStateException(s"Unable to read dataframe schema by key '$key'. " +
        s"If dataframe was not persisted by Spark, provide a schema explicitly with .schema() " +
        s"or use 'infer.schema' option. ")
    }
    decodeSchema(key, schemaBytes)
  }

  /**
    * read rows from redis
    */
  private def scanRows(node: RedisNode, keys: Seq[String], keyType: String, schema: StructType,
                       requiredColumns: Seq[String]): Seq[Row] = {
    withConnection(node.connect()) { conn =>
      val filteredKeys =
        if (filterKeysByTypeEnabled) {
          val keyTypes = mapWithPipeline(conn, keys) { (pipeline, key) =>
            pipeline.`type`(key)
          }
          keys.zip(keyTypes).filter(_._2 == keyType).map(_._1)
        } else {
          keys
        }
      val pipelineValues = mapWithPipeline(conn, filteredKeys) { (pipeline, key) =>
        persistence.load(pipeline, key, requiredColumns)
      }
      val keysAndValues = filteredKeys.zip(pipelineValues)

      // if specific key (not pattern) is provided and the value doesn't exist, filter them out
      val filteredKeysAndValues =
        if (Keys.isRedisRegex(dataKeyPattern)) {
          keysAndValues
        } else {
          keysAndValues.filter {
            case (_, null) => false // binary model
            case (_, value: JList[_]) if value.asScala.forall(_ == null) => false // hash model
            case _ => true
          }
        }

      filteredKeysAndValues.map { case (key, value) =>
        val keyMap = keyName -> tableKey(keysPrefixPattern, key)
        FieldParseException.withKey(key) {
          persistence.decodeRow(keyMap, value, schema, requiredColumns)
        }
      }
    }
  }
}

object RedisSourceRelation {

  def schemaKey(tableName: String): String = s"_spark:$tableName:schema"

  /**
    * A table's schema, as it is stored in its schema key: Spark's schema JSON (`StructType.json`), which every Spark
    * version reads.
    *
    * Earlier versions stored a Java-serialized `StructType`. Those bytes can only be read with the Spark minor version
    * and the Scala version that wrote them: `StructType` doesn't declare a `serialVersionUID`, and the one the JVM
    * computes differs between Spark 3.2, 3.3, 3.4 and 3.5, and a schema's metadata holds Scala collection classes.
    * [[decodeSchema]] still reads them.
    */
  private[redis] def encodeSchema(schema: StructType): Array[Byte] = schema.json.getBytes(UTF_8)

  /**
    * Reads a schema stored by [[encodeSchema]], or a Java-serialized one written by an earlier version of the
    * library.
    *
    * @param key   the schema key, for error messages
    * @param bytes the key's value
    * @throws IllegalStateException if the value is a Java-serialized schema from another Spark version, or isn't a
    *                               schema at all
    */
  private[redis] def decodeSchema(key: String, bytes: Array[Byte]): StructType =
    if (isJavaSerialized(bytes)) {
      try {
        SerializationUtils.deserialize[StructType](bytes)
      } catch {
        case e: SerializationException if e.getCause.isInstanceOf[InvalidClassException] =>
          throw new IllegalStateException(s"The schema in '$key' was written by an earlier version of spark-redis " +
            "with another Spark or Scala version, and can't be read with this one. Read the table with an explicit " +
            ".schema(), or with the Spark and Scala versions that wrote it; saving the table again stores its " +
            "schema in a format every version reads.", e)
      }
    } else {
      val parsed = try DataType.fromJson(new String(bytes, UTF_8)) catch {
        case NonFatal(e) =>
          throw new IllegalStateException(
            s"The value of '$key' is neither a schema JSON nor a Java-serialized schema", e)
      }
      parsed match {
        case struct: StructType => struct
        case dataType: DataType =>
          throw new IllegalStateException(s"The value of '$key' is a ${dataType.typeName} type, not a table schema")
      }
    }

  /** Java serialization streams start with the magic number 0xACED (`java.io.ObjectStreamConstants`). */
  private def isJavaSerialized(bytes: Array[Byte]): Boolean =
    bytes.length >= 2 && bytes(0) == 0xAC.toByte && bytes(1) == 0xED.toByte

  def dataKey(tableName: String, id: String = uuid()): String = {
    s"$tableName:$id"
  }

  def uuid(): String = UUID.randomUUID().toString.replace("-", "")

  def tableDataKeyPattern(tableName: String): String = s"$tableName:*"

  /**
    * @return the `write.mode` option: `merge` (the default) or `replace`
    * @throws IllegalArgumentException for any other value, so a write fails before it changes anything
    */
  def parseWriteMode(parameters: Map[String, String]): String = {
    val mode = parameters.getOrElse(SqlOptionWriteMode, SqlOptionWriteModeMerge)
    if (mode != SqlOptionWriteModeMerge && mode != SqlOptionWriteModeReplace) {
      throw new IllegalArgumentException(s"Invalid value '$mode' for option '$SqlOptionWriteMode'. " +
        s"Use '$SqlOptionWriteModeMerge' (the default) or '$SqlOptionWriteModeReplace'.")
    }
    mode
  }

  /**
    * Fails when a pipelined reply is an error, so a write that Redis refused is not dropped silently. Inside
    * `MULTI`/`EXEC`, a command that fails when queued (`MOVED`, `NOPERM`, ...) gets the error as its own reply and
    * `EXEC` then replies `EXECABORT`. A command that fails while `EXEC` runs has its error inside `EXEC`'s reply.
    */
  def failOnErrorReplies(replies: Seq[AnyRef]): Unit = {
    val errors = replies.flatMap {
      case e: JedisDataException => Seq(e)
      case execReplies: JList[_] => execReplies.asScala.collect { case e: JedisDataException => e }
      case _ => Nil
    }
    if (errors.nonEmpty) {
      // EXECABORT says only that the transaction was discarded. The error before it says why.
      val cause = errors.find(e => !String.valueOf(e.getMessage).startsWith("EXECABORT")).getOrElse(errors.head)
      throw new IllegalStateException(
        s"Redis refused a write with $SqlOptionWriteMode '$SqlOptionWriteModeReplace': ${cause.getMessage}", cause)
    }
  }

  def tableKey(keysPrefixPattern: String, redisKey: String): String = {
    if (keysPrefixPattern.endsWith("*")) {
      // keysPattern*
      redisKey.substring(keysPrefixPattern.length - 1)
    } else {
      redisKey
    }
  }
}
