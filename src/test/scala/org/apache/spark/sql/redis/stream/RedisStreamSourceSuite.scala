package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.redis.{StreamOptionGroupName, StreamOptionStreamKeys}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.EntryID

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationLong

/**
  * @author The Viet Nguyen
  */
class RedisStreamSourceSuite extends FunSuite with Matchers with RedisStandaloneEnv {

  // TODO:
  ignore("create redis stream source") {
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
    val persons = spark.readStream
      .format("redis")
      .schema(Person.fullSchema)
      .option(StreamOptionStreamKeys, "mystream")
      .option(StreamOptionGroupName, "group55")
      .load()
    val personCounts = persons.groupBy("salary")
      .count()
    val query = personCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }

  // TODO:
  ignore("read stream source") {
    // given:
    // - I insert 10 elements to Redis XStream
    val streamKey = Person.generatePersonStreamKey()
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      (1 to 10).foreach { i =>
        conn.xadd(streamKey, new EntryID(0, i), Person.dataMaps.head.asJava)
      }
      // when:
      // - I read stream with batch size equal to 5
      val spark = SparkSession
        .builder
        .config(conf)
        .getOrCreate()
      val persons = spark.readStream
        .format("redis")
        .schema(Person.fullSchema)
        .option(StreamOptionStreamKeys, streamKey)
        .load()
      val personCounts = persons.groupBy("salary")
        .count()
      personCounts.writeStream
        .outputMode("complete")
        .format("console")
        .start()
      // then:
      // - It eventually reach the point where there are 10 acknowledged and 0 pending messages
      eventually(timeout(5 seconds)) {
        val groups = conn.xinfo(XINFO.SubCommandGroups, streamKey)
        groups("group55").asInstanceOf[Map[String, Any]](XINFO.LastDeliveredId) shouldBe "0-10"
      }
    }
  }

  ignore("read stream source with un-synchronized schedules") {
    // given:
    // - I insert 5 elements to Redis XStream every time with delay of 500 ms
    // when:
    // - I read stream with batch size equal to 4 and delay equal to 400 ms
    // then:
    // - It eventually reach the point where there are 8 acknowledged and 2 pending messages
  }
}
