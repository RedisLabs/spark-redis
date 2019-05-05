package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.util.Person
import com.redislabs.provider.redis.util.Person._
import com.redislabs.provider.redis.util.TestUtils._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.redis._
import org.scalatest.Matchers


trait DataframeSuite extends RedisDataframeSuite with Matchers {

  import TestSqlImplicits._

  test("save and load dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("append data when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .mode(SaveMode.Append)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("append data when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    val appendData = data.map(p => p.copy(age = p.age + 1))
    spark.createDataFrame(appendData).write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .mode(SaveMode.Append)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
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
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .mode(SaveMode.Overwrite)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("overwrite data when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    val overrideData = data.map(p => p.copy(age = p.age + 1))
    spark.createDataFrame(overrideData)
      .write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .mode(SaveMode.Overwrite)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf, overrideData)
  }

  test("ignore data when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .mode(SaveMode.Ignore)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("ignore data when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    // the modified information should not be persisted
    spark.createDataFrame(data.map(p => p.copy(age = p.age + 1)))
      .write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .mode(SaveMode.Ignore)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("error if exists when it's empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).mode(SaveMode.ErrorIfExists)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("error if exists when it's not empty") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    // the modified information should not be persisted
    interceptSparkErr[IllegalStateException] {
      spark.createDataFrame(data.map(p => p.copy(age = p.age + 1)))
        .write.format(RedisFormat)
        .option(SqlOptionTableName, tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
    }
  }

  test("repartition read/write") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionNumPartitions, 1)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("user defined key column") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, KeyName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, KeyName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("user defined key column append") {
    val tableName = generateTableName(TableNamePrefix)
    spark.createDataFrame(data).write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, KeyName)
      .save()
    val head = data.head
    val appendData = Seq(head.copy(name = "Jack"), head.copy(age = 31))
    val df = spark.createDataFrame(appendData)
    df.write.format(RedisFormat)
      .mode(SaveMode.Append)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, KeyName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, KeyName)
      .load()
      .cache()
    loadedDf.show()
    loadedDf.count() shouldBe 3
    loadedDf.schema shouldBe df.schema
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe (appendData :+ data(1)).toArray.sortBy(_.name)
  }

  test("user defined key column overwrite") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    val overrideData = data.map(p => p.copy(age = p.age + 1))
    spark.createDataFrame(overrideData)
      .write.format(RedisFormat)
      .mode(SaveMode.Overwrite)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, KeyName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, KeyName)
      .load()
      .cache()
    verifyDf(loadedDf, overrideData)
  }

  test("save dataframe with ttl") {
    val tableName = generateTableName(TableNamePrefix)
    writeDf(tableName, Map(SqlOptionTTL -> 1))
    loadAndVerifyDf(tableName)
    Thread.sleep(1000)
    val actualDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
    actualDf.count() shouldBe 0
  }

  test("save dataframe in binary model with ttl") {
    val tableName = generateTableName(TableNamePrefix)
    writeDf(tableName, Map(SqlOptionModel -> SqlOptionModelBinary, SqlOptionTTL -> 1))
    loadAndVerifyDf(tableName, Map(SqlOptionModel -> SqlOptionModelBinary))
    Thread.sleep(1000)
    val actualDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
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

  test("filter dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .filter("age = 30")
      .cache()

    loadedDf.count() should be(1)
    loadedDf.collect().head.getAs[String]("name") should be("John")
  }

  test("max.pipeline.size option") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionNumPartitions, 1) // 1 partition to scan 2 rows
      .option(SqlOptionMaxPipelineSize, 1) // two pipelines should be created (1 key per pipeline)
      .load()
      .cache()

    verifyDf(loadedDf)
  }

}
