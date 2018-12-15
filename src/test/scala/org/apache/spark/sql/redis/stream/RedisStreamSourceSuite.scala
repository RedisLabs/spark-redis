package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.util.Person
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
      .schema(Person.fullSchema)
      .option(StreamOptionStreamKey, "mystream")
      .load()
    val personCounts = persons.groupBy("salary")
      .count()
    val query = personCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }

  test("read stream source") {
    // given:
    // - I insert 100 elements to Redis XStream
    // when:
    // - I read stream with batch size equal to 5
    // then:
    // - It eventually reach the point where there are 100 acknowledged and 0 pending messages
  }
}
