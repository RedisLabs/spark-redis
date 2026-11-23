package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.util.Person.{TableNamePrefix, data}
import com.redislabs.provider.redis.util.TestUtils.{generateTableName, interceptSparkErr}
import org.apache.spark.sql.redis.{RedisFormat, SqlOptionTableName}
import org.scalatest.Matchers
import redis.clients.jedis.exceptions.JedisConnectionException

/**
 * Basic dataframe test with user/password authentication
 */
trait AclDataframeSuite extends RedisDataframeSuite with Matchers {

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

  test("incorrect password in dataframe options") {
    interceptSparkErr[JedisConnectionException] {
      val tableName = generateTableName(TableNamePrefix)
      val df = spark.createDataFrame(data)
      df.write.format(RedisFormat)
        .option(SqlOptionTableName, tableName)
        .option("user", user)
        .option("password", "wrong_password")
        .save()
    }
  }

  test("correct user/password in dataframe options") {
    writeAndRead("password")
  }

  test("the deprecated auth option still authenticates") {
    writeAndRead("auth")
  }

  private def writeAndRead(passwordOption: String): Unit = {
    val tableName = generateTableName(TableNamePrefix)
    val df = spark.createDataFrame(data)
    df.write.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option("user", user)
      .option(passwordOption, userPassword)
      .save()

    val loadedDf = spark.read.format(RedisFormat)
      .option(SqlOptionTableName, tableName)
      .option("user", user)
      .option(passwordOption, userPassword)
      .load()
      .cache()
    verifyDf(loadedDf)
  }

}
