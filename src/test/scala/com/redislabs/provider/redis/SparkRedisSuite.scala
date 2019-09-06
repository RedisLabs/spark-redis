package com.redislabs.provider.redis

import com.redislabs.provider.redis.env.Env
import com.redislabs.provider.redis.rdd.Keys
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * @author The Viet Nguyen
  */
trait SparkRedisSuite extends FunSuite with Env with Keys with BeforeAndAfterAll {

  override def beforeAll() {
    super.beforeAll()
    spark = SparkSession.builder().config(conf).getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    spark.stop
    System.clearProperty("spark.driver.port")
  }

  object TestSqlImplicits extends SQLImplicits {

    override protected def _sqlContext: SQLContext = spark.sqlContext
  }

}
