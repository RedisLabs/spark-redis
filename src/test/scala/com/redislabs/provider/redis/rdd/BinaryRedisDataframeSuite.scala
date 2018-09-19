package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.rdd.Person._
import org.apache.spark.SparkException
import org.apache.spark.sql.redis._
import org.scalatest.Matchers

/**
  * @author The Viet Nguyen
  */
class BinaryRedisDataframeSuite extends RedisStandaloneSuite with Matchers {

  import TestSqlImplicits._

  test("save and load dataframe with binary mode") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).option(SqlOptionMode, SqlOptionModeBinary).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).option(SqlOptionMode, SqlOptionModeBinary)
      .load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  test("save with binary mode and load dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).option(SqlOptionMode, SqlOptionModeBinary).save(tableName)
    // TODO: support cross mode read/write
    intercept[SparkException] {
      spark.read.format(RedisFormat).load(tableName).show()
    }
  }

  test("save and load with binary mode dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    // TODO: support cross mode read/write
    intercept[SparkException] {
      spark.read.format(RedisFormat).option(SqlOptionMode, SqlOptionModeBinary)
        .load(tableName).show()
    }
  }
}
