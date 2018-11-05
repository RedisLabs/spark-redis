package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.SparkRedisSuite
import com.redislabs.provider.redis.util.Person
import com.redislabs.provider.redis.util.Person.data
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.redis.{RedisFormat, SqlOptionTableName}
import org.scalatest.Matchers

/**
  * @author The Viet Nguyen
  */
trait RedisDataframeSuite extends SparkRedisSuite with Matchers {

  import TestSqlImplicits._

  lazy val expectedDf: DataFrame = Person.df(spark)

  def writeDf(tableName: String, options: Map[String, Any] = Map()): Unit = {
    val df = spark.createDataFrame(data)

    val initialWriter = df
      .write
      .format(RedisFormat)
      .option(SqlOptionTableName, tableName)

    val writer = options.foldLeft(initialWriter) { case (acc, (k, v)) =>
      acc.option(k, v.toString)
    }
    writer.save()
  }

  def createTempView(tableName: String): Unit = {
    spark.createDataFrame(data).createOrReplaceTempView(tableName)
  }

  def loadAndVerifyDf(tableName: String, options: Map[String, Any] = Map()): Unit = {
    val initialReader = spark
      .read
      .format(RedisFormat)
      .option(SqlOptionTableName, tableName)

    val reader = options.foldLeft(initialReader) { case (acc, (k, v)) =>
      acc.option(k, v.toString)
    }

    val actualDf = reader.load().cache()
    verifyDf(actualDf, data)
  }

  def verifyDf(actualDf: DataFrame, data: Seq[Person] = Person.data): Unit = {
    actualDf.show()
    actualDf.count() shouldBe expectedDf.count()
    // TODO: check nullable columns
    // actualDf.schema shouldBe expectedDf.schema
    val loadedArr = actualDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  def verifyPartialDf(actualDf: DataFrame): Unit = {
    actualDf.show()
    actualDf.count() shouldBe expectedDf.count()
    // TODO: check nullable columns
    // actualDf.schema shouldBe expectedDf.schema
    val loadedArr = actualDf.collect()
      .map(r => (r.getAs[String]("name"), r.getAs[Double]("salary")))
    loadedArr.sortBy(_._1) shouldBe data.toArray.sortBy(_.name).map(p => (p.name, p.salary))
  }
}
