package com.redislabs.provider.redis.rdd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}

class RedisSqlSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {

  // TODO:
  test("test") {

//    import org.apache.log4j.Logger
//    import org.apache.log4j.Level
//
//    Logger.getLogger("org").setLevel(Level.DEBUG)
//    Logger.getLogger("akka").setLevel(Level.DEBUG)


    val conf = new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "6379")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.createDataFrame(Seq(1 -> "one", 2 -> "two"))
    println(df.count())
    df.show(false)
    df.write.option("aa", "bbb").format("org.apache.spark.sql.redis").save("test")
//    df.write.option("aa", "bbb").format("org.apache.spark.sql.redis").saveAsTable("zzz")
//    df.write.option("aa", "bbb").format("org.apache.spark.sql.redis").save("yyy")


//    val df2 = spark.read.format("org.apache.spark.sql.redis").load("aaaa")
//    df2.show()

    println("Done")

  }

}
