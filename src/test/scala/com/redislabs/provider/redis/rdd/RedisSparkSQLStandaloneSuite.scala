package com.redislabs.provider.redis.rdd

import com.redislabs.provider.redis._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}

class RedisSparkSQLStandaloneSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {

  var sqlContext: SQLContext = null
  override def beforeAll() {
    super.beforeAll()

    sc = new SparkContext(new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "6379")
      .set("redis.auth", "passwd")
    )
    redisConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379, "passwd"))

    // Flush all the hosts
    redisConfig.hosts.foreach( node => {
      val conn = node.connect
      conn.flushAll
      conn.close
    })

    sqlContext = new SQLContext(sc)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE rl
                       |(name STRING, score INT)
                       |USING com.redislabs.provider.redis.sql
                       |OPTIONS (table 'rl')
      """.stripMargin)

    (1 to 64).foreach{
      index => {
        sqlContext.sql(s"insert overwrite table rl select t.* from (select 'rl${index}', ${index}) t")
      }
    }
  }

  test("RedisKVRDD - default(cluster)") {
    val df = sqlContext.sql(
      s"""
         |SELECT *
         |FROM rl
       """.stripMargin)
    df.filter(df("score") > 10).count should be (54)
    df.filter(df("score") > 10 and df("score") < 20).count should be (9)
  }

  test("RedisKVRDD - cluster") {
    implicit val c: RedisConfig = redisConfig
    val df = sqlContext.sql(
      s"""
         |SELECT *
         |FROM rl
       """.stripMargin)
    df.filter(df("score") > 10).count should be (54)
    df.filter(df("score") > 10 and df("score") < 20).count should be (9)
  }

  override def afterAll(): Unit = {
    sc.stop
    System.clearProperty("spark.driver.port")
  }
}

