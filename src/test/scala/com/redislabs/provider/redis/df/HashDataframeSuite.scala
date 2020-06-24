package com.redislabs.provider.redis.df

import java.sql.{Date, Timestamp}
import java.util.UUID

import com.redislabs.provider.redis.toRedisContext
import com.redislabs.provider.redis.util.Person.{data, _}
import com.redislabs.provider.redis.util.TestUtils._
import com.redislabs.provider.redis.util.{EntityId, Logging, Person}
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.redis.RedisSourceRelation.tableDataKeyPattern
import org.apache.spark.sql.redis._
import org.apache.spark.sql.types.{StructField, _}
import org.scalatest.Matchers

import scala.util.Random

/**
  * @author The Viet Nguyen
  */
// scalastyle:off multiple.string.literals
trait HashDataframeSuite extends RedisDataframeSuite with Matchers with Logging {

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
    saveMap(tableName)
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
    saveMap(tableName)
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, tableName + ":*")
      .schema(Person.schema)
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

  test("read key column from Redis keys") {
    val tableName = generateTableName("person")
    saveMap(tableName, "John",
      Map("age" -> "30", "address" -> "60 Wall Street", "salary" -> "150.5"))
    val loadedPersons = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, tableDataKeyPattern(tableName))
      .option(SqlOptionKeyColumn, "name")
      .schema(Person.schema)
      .load()
      .as[Person]
      .collect()
    loadedPersons should contain(Person.data.head)
  }

  test("read key column from Redis keys with prefix pattern") {
    val tableName = generateTableName("person")
    saveMap(tableName, "John",
      Map("age" -> "30", "address" -> "60 Wall Street", "salary" -> "150.5"))
    val loadedPersons = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, tableDataKeyPattern(tableName))
      .option(SqlOptionKeyColumn, "name")
      .schema(Person.schema)
      .load()
      .as[Person]
      .collect()
    loadedPersons should contain(Person.data.head)
  }

  test("read key column from Redis keys (when _id field does not exist)") {
    val tableName = generateTableName("person")
    saveMap(tableName, "John",
      Map("name" -> "John", "age" -> "30", "address" -> "60 Wall Street", "salary" -> "150.5"))
    val loadedPersons = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, tableDataKeyPattern(tableName))
      .schema(Person.schema)
      .load()
      .as[Person]
      .collect()
    loadedPersons should contain(Person.data.head)
  }

  test("read default key column from Redis keys") {
    val tableName = generateTableName("entityId")
    saveMap(tableName, "id", Map("name" -> "name"))
    val loadedEntities = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, tableDataKeyPattern(tableName))
      .schema(EntityId.schema)
      .load()
      .as[EntityId]
      .collect()
    loadedEntities should contain(EntityId("id", "name"))
  }

  test("load filtered binary keys with hashes") {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionModel, SqlOptionModelBinary)
      .save()
    val extraKey = RedisSourceRelation.uuid()
    saveMap(tableName, extraKey, Person.dataMaps.head)
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionModel, SqlOptionModelBinary)
      .option(SqlOptionFilterKeysByType, value = true)
      .load()
      .cache()
    val countFiltered = loadedDf.count()
    countFiltered shouldBe 2
    val countAll = sc.fromRedisKeyPattern(tableDataKeyPattern(tableName)).count()
    countAll shouldBe 3
    verifyDf(loadedDf)
  }

  test("load unfiltered binary keys with hashes") {
    val tableName = generateTableName(TableNamePrefix)
    val extraKey = RedisSourceRelation.uuid()
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionModel, SqlOptionModelBinary)
      .save()
    saveMap(tableName, extraKey, Person.dataMaps.head)
    interceptSparkErr[SparkException] {
      spark.read.format(RedisFormat)
        .schema(Person.fullSchema)
        .option(SqlOptionTableName, tableName)
        .option(SqlOptionModel, SqlOptionModelBinary)
        .load()
        .collect()
    }
  }

  /**
    * A test case for https://github.com/RedisLabs/spark-redis/issues/132
    */
  test("RedisSourceRelation.buildScan columns ordering") {
    val schema = {
      StructType(Array(
        StructField("id", StringType),
        StructField("int", IntegerType),
        StructField("float", FloatType),
        StructField("double", DoubleType),
        StructField("str", StringType)))
    }

    val rowsNum = 8
    val rdd = spark.sparkContext.parallelize(1 to rowsNum, 2).map { _ =>
      def genStr = UUID.randomUUID().toString
      def genInt = Random.nextInt()
      def genDouble = Random.nextDouble()
      def genFloat = Random.nextFloat()
      Row.fromSeq(Seq(genStr, genInt, genFloat, genDouble, genStr))
    }

    val df = spark.createDataFrame(rdd, schema)
    val tableName = generateTableName("cols-ordering")
    df.write.format(RedisFormat).option(SqlOptionTableName, tableName).save()
    val loadedDf = spark.read.format(RedisFormat).option(SqlOptionTableName, tableName).load()
    loadedDf.schema shouldBe schema
    loadedDf.collect().length shouldBe rowsNum
    loadedDf.show()
  }

  test("read dataframe by non-existing key (not pattern)") {
    val df = spark.read.format(RedisFormat)
      .option(SqlOptionKeysPattern, "some-non-existing-key")
      .schema(StructType(Array(
        StructField("id", IntegerType),
        StructField("value", IntegerType)
      )))
      .load()
      .cache()

    df.show()
    df.count() should be (0)
  }

  def saveMap(tableName: String): Unit = {
    Person.dataMaps.foreach { person =>
      saveMap(tableName, person("name"), person)
    }
  }

  def saveMap(tableName: String, key: String, value: Map[String, String]): Unit
}
