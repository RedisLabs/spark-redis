package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.rdd.Person._
import org.apache.spark.sql.redis._
import org.scalatest.Matchers

/**
  * @author The Viet Nguyen
  */
class RedisHashDataframeSuite extends RedisStandaloneSuite with Matchers {

  import TestSqlImplicits._

  test("save and load dataframe by default") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBedf.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  test("save and load dataframe with hash mode") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).option(SqlOptionMode, SqlOptionModeHash).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).option(SqlOptionMode, SqlOptionModeHash)
      .load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  test("save with hash mode and load dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).option(SqlOptionMode, SqlOptionModeHash).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  test("save and load with hash mode dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).option(SqlOptionMode, SqlOptionModeHash)
      .load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }
}
