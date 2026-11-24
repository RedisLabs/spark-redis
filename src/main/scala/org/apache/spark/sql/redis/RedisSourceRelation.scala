package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.{List => JList}

import com.redislabs.provider.redis.rdd.Keys
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.{FieldParseException, JavaSerializedSchema, Logging, RedisCapabilities,
  WriteCapabilities}
import com.redislabs.provider.redis.util.PipelineUtils._
import com.redislabs.provider.redis.{ReadWriteConfig, RedisConfig, RedisDataTypeHash, RedisDataTypeString, RedisEndpoint, RedisNode, toRedisContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.redis.RedisSourceRelation._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
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
  /** What a Java-serialized row or schema read from Redis may contain. Parsed here, so a bad pattern fails early. */
  private val allowedClasses = parameters.get(SqlOptionDeserializationAllowedClasses)
    .map(value => ClassAllowlist.Default ++ ClassAllowlist.parse(value))
    .getOrElse(ClassAllowlist.Default)
  private val writeMode = parseWriteMode(parameters)
  /** `write.mode = replace` on the hash model. A binary-model row is one value, which a write always replaces. */
  private val replaceHashes = writeMode == SqlOptionWriteModeReplace && persistenceModel != SqlOptionModelBinary
  /** The hash model's persistence for `write.mode = replace`, if that's how this relation writes. */
  private val replacingHashes: Option[HashRedisPersistence] =
    if (replaceHashes) Some(new HashRedisPersistence(replace = true)) else None
  private val persistence: RedisPersistence[Any] = replacingHashes.getOrElse {
    // like RedisPersistence(model), with the classes a binary-model row may contain
    if (persistenceModel == SqlOptionModelBinary) {
      new BinaryRedisPersistence(allowedClasses).asInstanceOf[RedisPersistence[Any]]
    } else {
      RedisPersistence(persistenceModel)
    }
  }

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
        // one row per key: of rows with the same key in a batch, the last one is written
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

  /**
    * Writes the rows of `keys`, which the relation's topology puts on `node`. Every write follows `MOVED` and `ASK` to
    * each key's current owner, since that topology can be out of date, and fails on any other error reply instead of
    * dropping the row (#347).
    */
  private def saveRows(node: RedisNode, keys: Array[String], rowsWithKey: Map[String, Row],
                       binaryTtlWrite: Option[(BinaryRedisPersistence, WriteCapabilities)]): Unit =
    (binaryTtlWrite, replacingHashes) match {
      case (Some((binary, capabilities)), _) =>
        // MSETEX per slot where every primary has it, or SET ... EX per row
        val encodedRows = keys.toList.map(key => (node.endpoint, (key, binary.encodeRow(keyName, rowsWithKey(key)))))
        binary.saveWithTtl(encodedRows, ttl, capabilities)
      case (None, Some(hash)) =>
        // one MULTI/EXEC per row, so a reader sees the old row or the new one
        val encodedRows = keys.toList.map(key => (node.endpoint, (key, hash.encodeRow(keyName, rowsWithKey(key)))))
        writeTransactionsWithRedirects(encodedRows) { case (key, encodedRow) =>
          hash.replaceCommands(key, encodedRow, ttl)
        }
      case (None, None) =>
        val encodedRows = keys.toList.map { key =>
          (node.endpoint, (key, persistence.encodeRow(keyName, rowsWithKey(key))))
        }
        val commands: ((String, Any)) => Seq[WriteCommand] = { case (key, encodedRow) =>
          persistence.saveCommands(key, encodedRow, ttl)
        }
        // with a ttl, one MULTI/EXEC per row, so its fields and its TTL are set in one step
        if (ttl > 0) writeTransactionsWithRedirects(encodedRows)(commands) else writeWithRedirects(encodedRows)(commands)
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
          RestrictedDeserialization.withKey(key) {
            persistence.decodeRow(keyMap, value, schema, requiredColumns)
          }
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
    * Earlier versions stored a Java-serialized `StructType`. Java deserialization reads those only with the Spark
    * minor version and the Scala version that wrote them (`StructType` declares no `serialVersionUID`, and a schema's
    * metadata holds Scala collection classes), so [[decodeSchema]] reads them with [[JavaSerializedSchema]] instead,
    * which works on any version and never deserializes an object: anyone who can write the schema key could otherwise
    * run code on the driver. `SchemaMigration` rewrites them as JSON.
    */
  private[redis] def encodeSchema(schema: StructType): Array[Byte] = schema.json.getBytes(UTF_8)

  /**
    * Reads a schema stored by [[encodeSchema]], or a Java-serialized one written by an earlier version of the
    * library, whichever Spark and Scala versions wrote it.
    *
    * @param key   the schema key, for error messages
    * @param bytes the key's value
    * @throws IllegalStateException if the value isn't a schema, or is a Java-serialized one that can't be read, for
    *                               example because it has a data type that this Spark version doesn't have
    */
  private[redis] def decodeSchema(key: String, bytes: Array[Byte]): StructType =
    if (isJavaSerialized(bytes)) {
      try JavaSerializedSchema.read(bytes) catch {
        case NonFatal(e) =>
          throw new IllegalStateException(s"The value of '$key' is Java-serialized, as Spark-Redis 3.1.0 and earlier " +
            s"stored schemas. ${e.getMessage}. Read the table with an explicit .schema(), or save it again.", e)
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

  def tableKey(keysPrefixPattern: String, redisKey: String): String = {
    if (keysPrefixPattern.endsWith("*")) {
      // keysPattern*
      redisKey.substring(keysPrefixPattern.length - 1)
    } else {
      redisKey
    }
  }
}
