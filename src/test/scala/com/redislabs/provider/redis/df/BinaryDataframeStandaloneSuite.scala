package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.rdd.Person._
import com.redislabs.provider.redis.rdd.RedisStandaloneSuite
import org.apache.spark.SparkException
import org.apache.spark.sql.redis._
import org.scalatest.Matchers

/**
  * @author The Viet Nguyen
  */
class BinaryDataframeStandaloneSuite extends RedisStandaloneSuite with DefaultTestDataset
  with Matchers {

  test("save and load dataframe with binary mode") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).option(SqlOptionModel, SqlOptionModelBinary).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).option(SqlOptionModel, SqlOptionModelBinary)
      .load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("save with binary mode and load dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).option(SqlOptionModel, SqlOptionModelBinary).save(tableName)
    intercept[SparkException] {
      spark.read.format(RedisFormat).load(tableName).show()
    }
  }

  test("save and load with binary mode dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    intercept[SparkException] {
      spark.read.format(RedisFormat).option(SqlOptionModel, SqlOptionModelBinary)
        .load(tableName).show()
    }
  }
}
