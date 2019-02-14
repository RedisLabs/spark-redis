package com.redislabs.provider.redis.df

import java.util.UUID

import com.redislabs.provider.redis.util.TestUtils.generateTableName
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.redis._
import org.apache.spark.sql.types._
import org.scalatest.Matchers

import scala.util.Random

/**
  * TODO: refactor duplicates code
  * TODO: test kryo, all types
  */
trait BlockDataframeSuite extends RedisDataframeSuite with Matchers {

  test("load dataframe from test.csv file, write/read from redis") {
    val file = getClass.getClassLoader.getResource("test.csv").getFile
    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(file)
      .withColumn("id", monotonically_increasing_id())
      .cache()

    val tableName = generateTableName("csv-data")

    df.write.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBlock)
      .option(SqlOptionLogInfoVerbose, true)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, "id")
      .save()

    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBlock)
      .option(SqlOptionLogInfoVerbose, true)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, "id")
      .load()
      .cache()

    df.schema should be(loadedDf.schema)

    df.collect().toSet should be(loadedDf.collect().toSet)
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
    df.write.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBlock)
      .option(SqlOptionTableName, tableName)
      .save()
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionModel, SqlOptionModelBlock)
      .option(SqlOptionTableName, tableName)
      .load()
    loadedDf.schema shouldBe schema
    loadedDf.collect().length shouldBe rowsNum
    loadedDf.show()
  }

}
