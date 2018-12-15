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

  test("read stream source with un-synchronized schedules") {
    // given:
    // - I insert 5 elements to Redis XStream every time with delay of 500 ms
    // when:
    // - I read stream with batch size equal to 4 and delay equal to 400 ms
    // then:
    // - It eventually reach the point where there are 8 acknowledged and 2 pending messages
  }
}
