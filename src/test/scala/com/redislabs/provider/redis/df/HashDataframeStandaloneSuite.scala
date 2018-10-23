package com.redislabs.provider.redis.df

import java.sql.{Date, Timestamp}

import com.redislabs.provider.redis.df.Person.{data, _}
import com.redislabs.provider.redis.rdd.RedisStandaloneSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.redis._
import org.apache.spark.sql.types._
import org.scalatest.Matchers

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashDataframeStandaloneSuite extends RedisStandaloneSuite with Matchers with DefaultTestDataset {

  import TestSqlImplicits._

  test("save and load dataframe by default") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).
      option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("save and load dataframe with hash mode") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelHash)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelHash)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("save with hash mode and load dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelHash)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("save and load with hash mode dataframe") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelHash)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("load dataframe with inferred schema") {
    val tableName = generateTableName(TableNamePrefix)
    val node = redisConfig.initialHost
    val conn = node.connect()
    val data = Seq(
      Map("name" -> "John", "age" -> "30", "address" -> "60 Wall Street", "salary" -> "150.5"),
      Map("name" -> "Peter", "age" -> "35", "address" -> "110 Wall Street", "salary" -> "200.3")
    )
    data.map(_.asJava)
      .foreach { person =>
        conn.hmset(tableName + ":" + person.get("name"), person)
      }
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, tableName + ":*")
      .option(SqlOptionInferSchema, "true")
      .load()
      .cache()
    loadedDf.show()
    loadedDf.count() shouldBe 2
    val loadedArr = loadedDf
      .collect()
      .map { row =>
        val name = row.getAs[String]("name")
        val age = row.getAs[String]("age").toInt
        val address = row.getAs[String]("address")
        val salary = row.getAs[String]("salary").toDouble
        Person(name, age, address, salary)
      }
    loadedArr.sortBy(_.name) shouldBe Person.data.toArray.sortBy(_.name)
  }

  test("load dataframe with provided schema") {
    val tableName = generateTableName(TableNamePrefix)
    val node = redisConfig.initialHost
    val conn = node.connect()
    val data = Seq(
      Map("name" -> "John", "age" -> "30", "address" -> "60 Wall Street", "salary" -> "150.5"),
      Map("name" -> "Peter", "age" -> "35", "address" -> "110 Wall Street", "salary" -> "200.3")
    )
    data.map(_.asJava)
      .foreach { person =>
        conn.hmset(tableName + ":" + person.get("name"), person)
      }
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, tableName + ":*")
      .schema(StructType(Array(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("address", StringType),
        StructField("salary", DoubleType)
      )))
      .load()
      .cache()
    loadedDf.show()
    loadedDf.count() shouldBe 2
    val loadedArr = loadedDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe Person.data.toArray.sortBy(_.name)
  }

  test("write and read null values") {
    val table = generateTableName("null-test")
    val df = spark.createDataFrame(Seq(
      (1, None),
      (2, Some(222))
    )).toDF("id", "value")

    df.printSchema()
    df.show()

    df.write.format(RedisFormat)
      .option(SqlOptionTableName, table)
      .save()

    // read table
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, table)
      .load()
      .cache()

    def verfiyDf(df: DataFrame): Unit = {
      df.show()
      val arr = df.collect()
      arr.find(r => r.getAs[Int]("id") == 1).get.getAs[Int]("value") should be(null: java.lang.Integer)
      arr.find(r => r.getAs[Int]("id") == 2).get.getAs[Int]("value") should be(222)
    }

    verfiyDf(loadedDf)

    // read by pattern
    val loadedDf2 = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, table + ":*")
      .schema(StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("value", IntegerType, nullable = true)
      )))
      .load()
      .cache()

    verfiyDf(loadedDf2)
  }

  test("data types") {
    val df = spark.createDataFrame(Seq(
      (1: Int,
        2: Byte,
        3: Long,
        4.2f: Float,
        5.3d: Double,
        true: Boolean,
        7: Short,
        "str8",
        Date.valueOf("2018-10-12"),
        Timestamp.valueOf("2017-12-02 03:04:00")
      )
    )).toDF()

    df.printSchema()
    df.show()

    val table = generateTableName("types-test")
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, table)
      .save()

    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, table)
      .load()
      .cache()

    loadedDf.show()
    val row = loadedDf.collect()(0)
    row.getAs[Int]("_1") should be(1: Int)
    row.getAs[Byte]("_2") should be(2: Byte)
    row.getAs[Long]("_3") should be(3: Long)
    row.getAs[Float]("_4") should be(4.2f: Float)
    row.getAs[Double]("_5") should be(5.3d: Double)
    row.getAs[Boolean]("_6") should be(true: Boolean)
    row.getAs[Short]("_7") should be(7: Short)
    row.getAs[String]("_8") should be("str8")
    row.getAs[java.sql.Date]("_9") should be(Date.valueOf("2018-10-12"))
    row.getAs[java.sql.Timestamp]("_10") should be(Timestamp.valueOf("2017-12-02 03:04:00"))
  }

}
