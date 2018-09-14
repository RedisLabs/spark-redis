package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis.rdd.Person._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.redis.{RedisFormat, SqlOptionNumPartitions}
import org.scalatest.ShouldMatchers

class RedisSqlStandaloneSuite extends RedisStandaloneSuite with ShouldMatchers {

  import TestSqlImplicits._

  test("save and load dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() should be(df.count())
    loadedDf.schema should be(df.schema)
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) should be(data.toArray.sortBy(_.name))
  }

  test("overwrite data when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).mode(SaveMode.Overwrite).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  test("overwrite data when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val overrideData = data.map(p => p.copy(age = p.age + 1))
    spark.createDataFrame(overrideData)
      .write.format(RedisFormat).mode(SaveMode.Overwrite).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe overrideData.toArray.sortBy(_.name)
  }

  test("ignore data when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).mode(SaveMode.Ignore).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  test("ignore data when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    // the modified information should not be persisted
    spark.createDataFrame(data.map(p => p.copy(age = p.age + 1)))
      .write.format(RedisFormat).mode(SaveMode.Ignore).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  test("error if exists when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).mode(SaveMode.ErrorIfExists).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  test("error if exists when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    // the modified information should not be persisted
    intercept[IllegalStateException] {
      spark.createDataFrame(data.map(p => p.copy(age = p.age + 1)))
        .write.format(RedisFormat).mode(SaveMode.ErrorIfExists).save(tableName)
    }
  }

  test("repartition read/write") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionNumPartitions, 1).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }
}
