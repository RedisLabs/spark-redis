package com.redislabs.provider.redis.sql

import com.redislabs.provider.redis.rdd.{Person, RedisStandaloneSuite}
import org.apache.spark.sql.redis.RedisFormat
import org.scalatest.Matchers

/**
  * @author The Viet Nguyen
  */
class SparkSqlStandaloneSuite extends RedisStandaloneSuite with DefaultTestDataset with Matchers {

  test("create temporary table then make regular insertions") {
    val tableName = Person.generatePersonTableName()
    // TODO: retrieve table name from tableName instead of path option
    spark.sql(
      s"""CREATE TEMPORARY TABLE $tableName (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING $RedisFormat OPTIONS (path '$tableName')
         |""".stripMargin)
    spark.sql(
      s"""INSERT INTO TABLE $tableName
         |  VALUES ('John', 30, '60 Wall Street', 150.5),
         |    ('Peter', 35, '110 Wall Street', 200.3)
         |""".stripMargin)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("create temporary table then make overwrite insertions when no data exists") {
    val tableName = Person.generatePersonTableName()
    // TODO: retrieve table name from tableName instead of path option
    spark.sql(
      s"""CREATE TEMPORARY TABLE $tableName (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING $RedisFormat OPTIONS (path '$tableName')
         |""".stripMargin)
    spark.sql(
      s"""INSERT overwrite TABLE $tableName
         |SELECT * FROM VALUES ('John', 30, '60 Wall Street', 150.5),
         |    ('Peter', 35, '110 Wall Street', 200.3)
         |""".stripMargin)
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("create temporary table then make overwrite insertions when data exists") {
    val tableName = Person.generatePersonTableName()
    // TODO: retrieve table name from tableName instead of path option
    spark.sql(
      s"""CREATE TEMPORARY TABLE $tableName (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING $RedisFormat OPTIONS (path '$tableName')
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
    val loadedDf = spark.read.format(RedisFormat).load(tableName).cache()
    verifyDf(loadedDf)
  }

  test("create temporary table, make regular insertions then select") {
    val tableName = Person.generatePersonTableName()
    // TODO: retrieve table name from tableName instead of path option
    spark.sql(
      s"""CREATE TEMPORARY TABLE $tableName (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING $RedisFormat OPTIONS (path '$tableName')
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
    val tableName = Person.generatePersonTableName()
    createTempView(tableName)
    val loadedDf = spark.sql(
      s"""SELECT * FROM $tableName
         |""".stripMargin)
    verifyDf(loadedDf)
  }
}
