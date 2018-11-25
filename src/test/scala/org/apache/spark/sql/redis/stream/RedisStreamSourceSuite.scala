package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.redis.StreamOptionStreamKey
import org.scalatest.FunSuite

/**
  * @author The Viet Nguyen
  */
class RedisStreamSourceSuite extends FunSuite with RedisStandaloneEnv {

  test("create redis stream source") {
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
    val persons = spark.readStream
      .format("redis")
      .option(StreamOptionStreamKey, "mystream")
      .load()
    val personCounts = persons.groupBy("_id")
      .count()
    val query = personCounts.writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
