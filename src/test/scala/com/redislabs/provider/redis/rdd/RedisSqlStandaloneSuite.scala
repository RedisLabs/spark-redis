package com.redislabs.provider.redis.rdd

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}

class RedisSqlStandaloneSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {

  override def beforeAll() {
    super.beforeAll()

    val conf = new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "6379")
      .set("redis.auth", "passwd")

    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  case class Person(name: String, age: Int, address: String, salary: Double)

  test("save and load dataframe") {

    // generate random table, so we can run test multiple times and not append/overwrite data
    val tableName = "person" + UUID.randomUUID().toString.replace("-", "")

    val data = Seq(
      Person("John", 30, "60 Wall Street", 150.5),
      Person("Peter", 35, "110 Wall Street", 200.3)
    )
    val df = spark.createDataFrame(data)
    df.write.format("org.apache.spark.sql.redis").save(tableName)

    val loadedDf = spark.read.format("org.apache.spark.sql.redis").load(tableName).cache()
    loadedDf.show()

    loadedDf.count() should be(df.count())
    loadedDf.schema should be(df.schema)

    val loadedArr = loadedDf.collect().map { row =>
      Person(row.getAs[String]("name"), row.getAs[Int]("age"), row.getAs[String]("address"), row.getAs[Double]("salary"))
    }

    loadedArr.sortBy(_.name) should be(data.toArray.sortBy(_.name))
  }

  override def afterAll(): Unit = {
    spark.stop
    System.clearProperty("spark.driver.port")
  }


}
