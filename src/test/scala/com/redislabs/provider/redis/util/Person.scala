package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.util.TestUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author The Viet Nguyen
  */
case class Person(name: String, age: Int, address: String, salary: Double)

object Person {

  val TableNamePrefix = "person"
  val KeyName = "name"

  val data = Seq(
    Person("John", 30, "60 Wall Street", 150.5),
    Person("Peter", 35, "110 Wall Street", 200.3)
  )

  val dataMaps = Seq(
    Map("name" -> "John", "age" -> "30", "address" -> "60 Wall Street", "salary" -> "150.5"),
    Map("name" -> "Peter", "age" -> "35", "address" -> "110 Wall Street", "salary" -> "200.3")
  )

  val schema = StructType(Array(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("address", StringType),
    StructField("salary", DoubleType)
  ))

  val fullSchema = StructType(schema.fields :+ StructField("_id", StringType))

  def df(spark: SparkSession): DataFrame = spark.createDataFrame(data)

  def generatePersonTableName(): String = generateTableName(TableNamePrefix)

  def generatePersonStreamKey(): String = generatePersonTableName()

}
