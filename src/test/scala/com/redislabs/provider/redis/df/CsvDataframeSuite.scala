package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.util.TestUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.redis.{RedisFormat, SqlOptionKeyColumn, SqlOptionTableName}
import org.scalatest.Matchers

trait CsvDataframeSuite extends RedisDataframeSuite with Matchers {

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
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, "id")
      .save()

    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option(SqlOptionKeyColumn, "id")
      .load()
      .cache()

    df.schema should be(loadedDf.schema)

    df.collect().toSet should be(loadedDf.collect().toSet)
  }

}
