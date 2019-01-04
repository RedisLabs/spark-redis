package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.env.RedisStandaloneEnv
import com.redislabs.provider.redis.util.ConnectionUtils.{JedisExt, XINFO, withConnection}
import com.redislabs.provider.redis.util.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.redis._
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.{EntryID, Jedis}

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class RedisStreamSourceSuite extends FunSuite with Matchers with RedisStandaloneEnv {

  test("read stream source") {
    // given:
    // - I insert 10 elements to Redis XStream
    val streamKey = Person.generatePersonStreamKey()
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      (1 to 5).foreach { i =>
        conn.xadd(streamKey, new EntryID(0, i), Person.dataMaps.head.asJava)
      }
      readAndCheckLastId(conn, streamKey, "0-5", 5)
    }
  }

  test("it should continue reading from the last offset after query/spark restart") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
      // write 5 items to stream
      (1 to 5).foreach { i =>
        conn.xadd(streamKey, new EntryID(0, i), Person.dataMaps.head.asJava)
      }
      readAndCheckLastId(conn, streamKey, "0-5", 5)

      // write 5 more items to stream
      (6 to 10).foreach { i =>
        conn.xadd(streamKey, new EntryID(0, i), Person.dataMaps.head.asJava)
      }
      readAndCheckLastId(conn, streamKey, "0-10", 5)
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

  def readAndCheckLastId(conn: Jedis, streamKey: String, lastDeliveredId: String, expectedRowsNum: Int): Unit = {
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
    val persons = spark.readStream
      .format("redis")
      .schema(Person.fullSchema)
      .option(StreamOptionStreamKeys, streamKey)
      .load()
    val query = persons.writeStream
      .format("memory")
      .queryName("persons")
      .start()

    println(s"query id ${query.id}")
    query.processAllAvailable()
    query.stop()

    // check items consumed
    val groups = conn.xinfo(XINFO.SubCommandGroups, streamKey)
    groups("spark-source").asInstanceOf[Map[String, Any]](XINFO.LastDeliveredId) shouldBe lastDeliveredId

    val resultDf = spark.sql("select * from persons").cache()
    resultDf.show()
    val result = resultDf.collect()
    result.length should be(expectedRowsNum)
    result.map(row => row.getAs[String]("_id")).last should be(lastDeliveredId)

    spark.stop()
  }

}
