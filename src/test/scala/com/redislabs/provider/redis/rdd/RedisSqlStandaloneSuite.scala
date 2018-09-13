package com.redislabs.provider.redis.rdd

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.redis.{RedisFormat, SqlOptionNumPartitions}
import org.apache.spark.sql.{SQLContext, SQLImplicits, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}

class RedisSqlStandaloneSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {

  object TestSqlImplicits extends SQLImplicits {

    override protected def _sqlContext: SQLContext = spark.sqlContext
  }

  import TestSqlImplicits._

  override def beforeAll() {
    super.beforeAll()

    val conf = new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "6379")
      .set("redis.auth", "passwd")

    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  private val TableName = "person"

  private val data = Seq(
    Person("John", 30, "60 Wall Street", 150.5),
    Person("Peter", 35, "110 Wall Street", 200.3)
  )

  test("save and load dataframe") {
    val tableName = generateTableName(TableName)
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
    val tableName = generateTableName(TableName)
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
    val tableName = generateTableName(TableName)
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
    val tableName = generateTableName(TableName)
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
    val tableName = generateTableName(TableName)
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
    val tableName = generateTableName(TableName)
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
    val tableName = generateTableName(TableName)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat).save(tableName)
    // the modified information should not be persisted
    intercept[IllegalStateException] {
      spark.createDataFrame(data.map(p => p.copy(age = p.age + 1)))
        .write.format(RedisFormat).mode(SaveMode.ErrorIfExists).save(tableName)
    }
  }

  test("repartition read/write") {
    // generate random table, so we can run test multiple times and not append/overwrite data
    val tableName = generateTableName(TableName)
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

  override def afterAll(): Unit = {
    spark.stop
    System.clearProperty("spark.driver.port")
  }

  private def generateTableName(prefix: String) = {
    // generate random table, so we can run test multiple times and not append/overwrite data
    prefix + UUID.randomUUID().toString.replace("-", "")
  }
}
