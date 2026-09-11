package com.redislabs.provider.redis.util

import org.apache.spark.sql.types.StructType

object SparkUtils {
  /**
   * Setting the schema column positions the same order as in requiredFields
   * @param schema Current schema
   * @param requiredColumns Column positions expecting by Catalyst
   */
  def alignSchemaWithCatalyst(schema: StructType, requiredColumns: Seq[String]): StructType = {
    val fieldsMap = schema.fields.map(f => (f.name, f)).toMap
    StructType(requiredColumns.map { c =>
      fieldsMap(c)
    })
  }
}
