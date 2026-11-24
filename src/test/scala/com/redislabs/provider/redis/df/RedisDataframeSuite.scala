package com.redislabs.provider.redis.df

import com.redislabs.provider.redis.SparkRedisSuite
import com.redislabs.provider.redis.util.Person
import com.redislabs.provider.redis.util.Person.data
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.redis.{RedisFormat, SqlOptionTableName}
import org.apache.spark.sql.types.DataType
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
    // Names and types, not nullability: a table read with a schema declared in SQL has only nullable columns.
    namesAndTypes(actualDf) shouldBe namesAndTypes(expectedDf)
    val loadedArr = actualDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }

  def verifyPartialDf(actualDf: DataFrame): Unit = {
    actualDf.show()
    actualDf.count() shouldBe expectedDf.count()
    namesAndTypes(actualDf) shouldBe namesAndTypes(expectedDf).filter { case (name, _) => actualDf.columns.contains(name) }
    val loadedArr = actualDf.collect()
      .map(r => (r.getAs[String]("name"), r.getAs[Double]("salary")))
    loadedArr.sortBy(_._1) shouldBe data.toArray.sortBy(_.name).map(p => (p.name, p.salary))
  }

  /** A DataFrame's columns as (name, type), by name. */
  private def namesAndTypes(df: DataFrame): Seq[(String, DataType)] =
    df.schema.fields.toSeq.map(field => field.name -> field.dataType).sortBy(_._1)
}
