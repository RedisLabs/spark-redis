package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets.UTF_8

import com.redislabs.provider.redis.util.SparkUtils
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.Pipeline

/**
  * @author The Viet Nguyen
  */
class BinaryRedisPersistence extends RedisPersistence[Array[Byte]] {

  override def save(pipeline: Pipeline, key: String, value: Array[Byte], ttl: Int): Unit = {
    val keyBytes = key.getBytes(UTF_8)
    if (ttl > 0) {
      pipeline.setex(keyBytes, ttl.toLong, value)
    } else {
      pipeline.set(keyBytes, value)
    }
  }

  override def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit =
    pipeline.get(key.getBytes(UTF_8))

  override def encodeRow(keyName: String, value: Row): Array[Byte] = {
    val fields = value.schema.fields.map(_.name)
    val valuesArray = fields.map(f => value.getAs[Any](f))
    SerializationUtils.serialize(valuesArray)
  }

  override def decodeRow(keyMap: (String, String), value: Array[Byte], schema: StructType,
                         requiredColumns: Seq[String]): Row = {
    val valuesArray: Array[Any] = SerializationUtils.deserialize(value)
    // Aligning column positions with what Catalyst expecting
    val alignedSchema = SparkUtils.alignSchemaWithCatalyst(schema, requiredColumns)
    val names = schema.fieldNames
    val alignedValuesArray = requiredColumns.toArray.map(f => valuesArray(names.indexOf(f)))
    new GenericRowWithSchema(alignedValuesArray, alignedSchema)
  }
}
