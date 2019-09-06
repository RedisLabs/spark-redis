package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.util.Person.generatePersonTableName
import org.apache.spark.sql.redis.{RedisFormat, SqlOptionTableName}
import org.scalatest.Matchers

/**
  * @author The Viet Nguyen
  */
trait SparkSqlSuite extends RedisDataframeSuite with Matchers {

  test("create temporary view then make regular insertions") {
    val tableName = generatePersonTableName()
    spark.sql(
      s"""CREATE TEMPORARY VIEW $tableName (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING $RedisFormat OPTIONS (table '$tableName')
         |""".stripMargin)
    spark.sql(
      s"""INSERT INTO TABLE $tableName
         |  VALUES ('John', 30, '60 Wall Street', 150.5),
         |    ('Peter', 35, '110 Wall Street', 200.3)
         |""".stripMargin)
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("create temporary view then make overwrite insertions when no data exists") {
    val tableName = generatePersonTableName()
    spark.sql(
      s"""CREATE TEMPORARY VIEW $tableName (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING $RedisFormat OPTIONS (table '$tableName')
         |""".stripMargin)
    spark.sql(
      s"""INSERT overwrite TABLE $tableName
         |SELECT * FROM VALUES ('John', 30, '60 Wall Street', 150.5),
         |    ('Peter', 35, '110 Wall Street', 200.3)
         |""".stripMargin)
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("create temporary view then make overwrite insertions when data exists") {
    val tableName = generatePersonTableName()
    spark.sql(
      s"""CREATE TEMPORARY VIEW $tableName (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING $RedisFormat OPTIONS (table '$tableName')
         |""".stripMargin)
    spark.sql(
      s"""INSERT INTO TABLE $tableName
         |  VALUES ('Johnny', 18, '17 Home Street', 0),
         |    ('Peter', 23, '6 Home Street', 20)
         |""".stripMargin)
    spark.sql(
      s"""INSERT overwrite TABLE $tableName
         |SELECT * FROM VALUES ('John', 30, '60 Wall Street', 150.5),
         |    ('Peter', 35, '110 Wall Street', 200.3)
         |""".stripMargin)
    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

  test("create temporary view, make regular insertions then select") {
    val tableName = generatePersonTableName()
    spark.sql(
      s"""CREATE TEMPORARY VIEW $tableName (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING $RedisFormat OPTIONS (table '$tableName')
         |""".stripMargin)
    spark.sql(
      s"""INSERT INTO TABLE $tableName
         |  VALUES ('John', 30, '60 Wall Street', 150.5),
         |    ('Peter', 35, '110 Wall Street', 200.3)
         |""".stripMargin)
    val loadedDf = spark.sql(
      s"""SELECT * FROM $tableName
         |""".stripMargin)
    verifyDf(loadedDf)
  }

  test("select from temporary view") {
    val tableName = generatePersonTableName()
    createTempView(tableName)
    val loadedDf = spark.sql(s"SELECT * FROM $tableName")
    verifyDf(loadedDf)
  }

  test("select all fields from temporary view") {
    val tableName = generatePersonTableName()
    createTempView(tableName)
    val loadedDf = spark.sql(s"SELECT name, age, address, salary FROM $tableName")
    verifyDf(loadedDf)
  }

  test("select name and salary from temporary view") {
    val tableName = generatePersonTableName()
    createTempView(tableName)
    val actualDf = spark.sql(s"SELECT name, salary FROM $tableName")
    verifyPartialDf(actualDf)
  }
}
