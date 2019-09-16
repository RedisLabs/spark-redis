package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.toRedisContext
import com.redislabs.provider.redis.util.Person
import com.redislabs.provider.redis.util.Person._
import com.redislabs.provider.redis.util.TestUtils._
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.redis.RedisSourceRelation.tableDataKeyPattern
import org.apache.spark.sql.redis._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.Matchers

/**
  * @author The Viet Nguyen
  */
trait BinaryDataframeSuite extends RedisDataframeSuite with Matchers {

  test("save and load dataframe with binary mode") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBinary)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBinary)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("save with binary mode and load dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBinary)
      .option(SqlOptionTableName, tableName)
      .save()
    interceptSparkErr[SparkException] {
      spark.read.format(RedisFormat)
        .option(SqlOptionTableName, tableName)
        .load()
        .show()
    }
  }

  test("save and load with binary mode dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    interceptSparkErr[SparkException] {
      spark.read.format(RedisFormat)
        .option(SqlOptionModel, SqlOptionModelBinary)
        .option(SqlOptionTableName, tableName)
        .load()
        .show()
    }
  }

  test("load filtered hash keys with strings") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionModel, SqlOptionModelHash)
      .save()
    val extraKey = RedisSourceRelation.uuid()
    saveMap(tableName, extraKey, Person.dataMaps.head)
    val loadedIds = spark.read.format(RedisFormat)
      .schema(Person.fullSchema)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionModel, SqlOptionModelHash)
      .option(SqlOptionFilterKeysByType, value = true)
      .load()
      .collect()
      .map { r =>
        r.getAs[String]("_id")
      }
    loadedIds.length shouldBe 2
    loadedIds should not contain extraKey
    val countAll = sc.fromRedisKeyPattern(tableDataKeyPattern(tableName)).count()
    countAll shouldBe 3
  }

  test("load unfiltered hash keys with strings") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionModel, SqlOptionModelHash)
      .save()
    saveMap(tableName, RedisSourceRelation.uuid(), Person.dataMaps.head)
    interceptSparkErr[SparkException] {
      spark.read.format(RedisFormat)
        .option(SqlOptionTableName, tableName)
        .option(SqlOptionModel, SqlOptionModelHash)
        .load()
        .collect()
    }
  }

  test("read dataframe by non-existing key (not pattern)") {
    val df = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, "some-non-existing-key")
      .option(SqlOptionModel, SqlOptionModelBinary)
      .schema(StructType(Array(
        StructField("id", IntegerType),
        StructField("value", IntegerType)
      )))
      .load()
      .cache()

    df.show()
    df.count() should be (0)
  }

  def serialize(value: Map[String, String]): Array[Byte] = {
    val valuesArray = value.values.toArray
    SerializationUtils.serialize(valuesArray)
  }

  def saveMap(tableName: String, key: String, value: Map[String, String]): Unit
}
