package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.df.Person.{generateTableName, _}
import com.redislabs.provider.redis.rdd.RedisStandaloneSuite
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.redis._
import org.scalatest.ShouldMatchers

class DataframeStandaloneSuite extends RedisStandaloneSuite with DefaultTestDataset
  with ShouldMatchers {

  import TestSqlImplicits._

  test("save and load dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("append data when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).mode(SaveMode.Append).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("append data when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val appendData = data.map(p => p.copy(age = p.age + 1))
    spark.createDataFrame(appendData)
      .write.format(RedisFormat).mode(SaveMode.Append).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe 2 * df.count()
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name).sortBy(_.age) shouldBe (data ++ appendData)
      .toArray.sortBy(_.name).sortBy(_.age)
  }

  test("overwrite data when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).mode(SaveMode.Overwrite).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("overwrite data when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val overrideData = data.map(p => p.copy(age = p.age + 1))
    spark.createDataFrame(overrideData)
      .write.format(RedisFormat).mode(SaveMode.Overwrite).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf, overrideData)
  }

  test("ignore data when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).mode(SaveMode.Ignore).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("ignore data when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    // the modified information should not be persisted
    spark.createDataFrame(data.map(p => p.copy(age = p.age + 1)))
      .write.format(RedisFormat).mode(SaveMode.Ignore).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("error if exists when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).mode(SaveMode.ErrorIfExists).save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
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
    verifyDf(loadedDf)
  }

  test("user defined key column") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).option(SqlOptionKeyColumn, "name").save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("user defined key column append") {
    val tableName = generateTableName(TableNamePrefix)
    spark.createDataFrame(data).write.format(RedisFormat).option(SqlOptionKeyColumn, "name")
      .save(tableName)
    val head = data.head
    val appendData = Seq(head.copy(name = "Jack"), head.copy(age = 31))
    val df = spark.createDataFrame(appendData)
    df.write.format(RedisFormat).mode(SaveMode.Append).option(SqlOptionKeyColumn, "name")
      .save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    loadedDf.show()
    loadedDf.count() shouldBe 3
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe (appendData :+ data(1)).toArray.sortBy(_.name)
  }

  test("user defined key column overwrite") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    val overrideData = data.map(p => p.copy(age = p.age + 1))
    spark.createDataFrame(overrideData)
      .write.format(RedisFormat).mode(SaveMode.Overwrite).option(SqlOptionKeyColumn, "name")
      .save(tableName)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf, overrideData)
  }

  test("save dataframe with ttl") {
    val tableName = generateTableName(TableNamePrefix)
    writeDf(tableName, Map(SqlOptionTTL -> 1))
    loadAndVerifyDf(tableName)
    Thread.sleep(1000)
    val actualDf = spark.read.format(RedisFormat).load(tableName)
    actualDf.count() shouldBe 0
  }

  test("save dataframe in binary model with ttl") {
    val tableName = generateTableName(TableNamePrefix)
    writeDf(tableName, Map(SqlOptionModel -> SqlOptionModelBinary, SqlOptionTTL -> 1))
    loadAndVerifyDf(tableName, Map(SqlOptionModel -> SqlOptionModelBinary))
    Thread.sleep(1000)
    val actualDf = spark.read.format(RedisFormat).load(tableName)
    actualDf.count() shouldBe 0
  }

  test("save dataframe with zero ttl") {
    val tableName = generateTableName(TableNamePrefix)
    writeDf(tableName, Map(SqlOptionTTL -> 0))
    loadAndVerifyDf(tableName)
    Thread.sleep(1000)
    loadAndVerifyDf(tableName)
  }

  test("save dataframe with no ttl") {
    val tableName = generateTableName(TableNamePrefix)
    writeDf(tableName)
    loadAndVerifyDf(tableName)
    Thread.sleep(1000)
    loadAndVerifyDf(tableName)
  }
}
