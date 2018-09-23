package com.redislabs.provider.redis.rdd

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * @author The Viet Nguyen
  */
trait SparkRedisSuite extends FunSuite with ENV with BeforeAndAfterAll {

  val conf: SparkConf

  override def beforeAll() {
    super.beforeAll()
    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop
    System.clearProperty("spark.driver.port")
  }


  object TestSqlImplicits extends SQLImplicits {

    override protected def _sqlContext: SQLContext = spark.sqlContext
  }
}
