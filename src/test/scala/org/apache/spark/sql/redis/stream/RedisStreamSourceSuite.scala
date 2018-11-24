package org.apache.spark.sql.redis.stream

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * @author The Viet Nguyen
  */
class RedisStreamSourceSuite extends FunSuite {

  test("create redis stream source") {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("RedisStreamSourceCreationTest")
      .getOrCreate()

    val persons = spark.readStream
      .format("redis")
      .load()

//    val personCounts = persons.groupBy("_id").count()

//    val query = personCounts.writeStream
    val query = persons.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
