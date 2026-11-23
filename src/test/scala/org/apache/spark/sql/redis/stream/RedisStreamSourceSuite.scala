package org.apache.spark.sql.redis.stream

import java.io.File
import java.sql.{Date, Timestamp}
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.env.Env
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.TestUtils.{findCause, interceptSparkErr}
import com.redislabs.provider.redis.util.{Logging, Person}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.redis._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}
import org.apache.spark.sql.types._
import org.scalatest.concurrent.Eventually._
import org.scalatest.{FunSuite, Matchers}
import redis.clients.jedis.StreamEntryID

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationLong

/**
  * @author The Viet Nguyen
  */
trait RedisStreamSourceSuite extends FunSuite with Matchers with Env with Logging {

  val AutoEntryId: StreamEntryID = new StreamEntryID() {
    override def toString: String = "*"
  }

  // implicit for withConnection()
  implicit lazy val implicitRedisConf: RedisConfig = redisConfig

  test("read stream source (less than batch size)") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      readStream(streamKey) { spark =>
        (1 to 5).foreach { i =>
          conn.xadd(streamKey, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
        }
        checkLastDeliveredId(streamKey, "0-5")
        checkCountAndLastItem(spark, "0-5", 5)
      }
    }
  }

  test("read stream source (more than batch size)") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      readStream(streamKey) { spark =>
        (1 to 546).foreach { i =>
          conn.xadd(streamKey, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
        }
        checkLastDeliveredId(streamKey, "0-546")
        checkCountAndLastItem(spark, "0-546", 546)
      }
    }
  }

  test("it reads from the last item by default") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      // write to stream first
      (1 to 10).foreach { i =>
        conn.xadd(streamKey, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
      }
      // read from stream after that, there shouldn't any data
      readStream(streamKey) { spark =>
        Thread.sleep(100)
        checkCount(spark, 0)
      }
    }
  }

  test("read with offset option") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      (1 to 320).foreach { i =>
        conn.xadd(streamKey, new StreamEntryID(i, 0), Person.dataMaps.head.asJava)
      }

      val offsetJson = s"""{"offsets":{"$streamKey":{"groupName":"redis-source","offset":"100-0"}}}"""
      val options = Map("stream.offsets" -> offsetJson)

      readStream(streamKey, options) { spark =>
        checkCountAndLastItem(spark, "320-0", 220)
      }
    }
  }

  test("read with offset option 0-0") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      // read first time
      readStream(streamKey) { spark =>
        (1 to 749).foreach { i =>
          conn.xadd(streamKey, new StreamEntryID(i, 0), Person.dataMaps.head.asJava)
        }
        checkCountAndLastItem(spark, "749-0", 749)
      }

      // re-read from the beginning
      val offsetJson =
        s"""{"offsets":{"$streamKey":{"groupName":"redis-source","offset":"0-0"}}}"""
      val options = Map("stream.offsets" -> offsetJson)

      readStream(streamKey, options) { spark =>
        checkCountAndLastItem(spark, "749-0", 749)
      }
    }
  }

  test("it should continue reading from the last offset after query/spark restart") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      readStream(streamKey) { spark =>
        (1 to 5).foreach { i =>
          conn.xadd(streamKey, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
        }
        checkLastDeliveredId(streamKey, "0-5")
        checkCountAndLastItem(spark, "0-5", 5)
      }

      // write 5 more items to stream
      (6 to 10).foreach { i =>
        conn.xadd(streamKey, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
      }

      readStream(streamKey) { spark =>
        checkLastDeliveredId(streamKey, "0-10")
        checkCountAndLastItem(spark, "0-10", 5)
      }
    }
  }

  test("it should read with offset after spark restart") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      readStream(streamKey) { spark =>
        (1 to 130).foreach { i =>
          conn.xadd(streamKey, new StreamEntryID(i, 0), Person.dataMaps.head.asJava)
        }
        checkCountAndLastItem(spark, "130-0", 130)
      }

      val offsetJson = s"""{"offsets":{"$streamKey":{"groupName":"redis-source","offset":"100-0"}}}"""
      val options = Map("stream.offsets" -> offsetJson)

      readStream(streamKey, options) { spark =>
        checkCountAndLastItem(spark, "130-0", 30)
      }
    }
  }

  test("read several streams") {
    val stream1Key = Person.generatePersonStreamKey()
    val stream2Key = Person.generatePersonStreamKey()

    readStream(s"$stream1Key,$stream2Key") { spark =>
      withConnection(stream1Key) { conn =>
        (1 to 5).foreach { i =>
          conn.xadd(stream1Key, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
        }
      }
      withConnection(redisConfig.connectionForKey(stream2Key)) { conn =>
        (6 to 10).foreach { i =>
          conn.xadd(stream2Key, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
        }
      }

      checkLastDeliveredId(stream1Key, "0-5")
      checkLastDeliveredId(stream2Key, "0-10")
      checkCount(spark, 10)
    }
  }

  test("read stream source (generated entry ids)") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      readStream(streamKey) { spark =>
        (1 to 1000).foreach { i =>
          conn.xadd(streamKey, AutoEntryId, Person.dataMaps.head.asJava)
        }
        checkCount(spark, 1000)
      }
    }
  }

  test("read with parallelism = 3") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      val options = Map(StreamOptionParallelism -> "3")
      readStream(streamKey, options) { spark =>
        (1 to 978).foreach { i =>
          conn.xadd(streamKey, new StreamEntryID(i, 0), Person.dataMaps.head.asJava)
        }
        checkCount(spark, 978)
      }
    }
  }

  test("can start stream with checkpointing") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      val checkPointLocation = s"${new File(".").getAbsolutePath}/checkpoint-test/${UUID.randomUUID()}"
      val writeOptions = Map("checkpointLocation" -> checkPointLocation)
      readStream(streamKey, extraWriteOptions = writeOptions) { spark =>
        (1 to 5).foreach { i =>
          conn.xadd(streamKey, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
        }
      }

      // write 5 more items to stream
      (6 to 10).foreach { i =>
        conn.xadd(streamKey, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
      }

      // restart stream
      readStream(streamKey, extraWriteOptions = writeOptions, writeFormat = "console") { spark =>
      }
    }
  }

  test("read stream source with date and timestamp fields") {
    val streamKey = Person.generatePersonStreamKey()
    val schema = StructType(Array(
      StructField("time", TimestampType),
      StructField("isoTime", TimestampType),
      StructField("date", DateType),
      StructField("event", StringType),
      StructField("_id", StringType)
    ))
    withConnection(streamKey) { conn =>
      readStream(streamKey, schema = schema) { spark =>
        val entry = Map(
          "time" -> "2022-10-14 13:33:15.012345",
          "isoTime" -> "2022-10-14T13:33:15.012345",
          "date" -> "2022-10-14",
          "event" -> "click")
        conn.xadd(streamKey, new StreamEntryID(0, 1), entry.asJava)
        eventually(timeout(5.seconds)) {
          val result = spark.sql("select * from persons").collect()
          result.length shouldBe 1
          val row = result.head
          row.getAs[Timestamp]("time") shouldBe Timestamp.valueOf("2022-10-14 13:33:15.012345")
          row.getAs[Timestamp]("isoTime") shouldBe Timestamp.valueOf("2022-10-14 13:33:15.012345")
          row.getAs[Date]("date") shouldBe Date.valueOf("2022-10-14")
          row.getAs[String]("event") shouldBe "click"
          row.getAs[String]("_id") shouldBe "0-1"
        }
      }
    }
  }

  Seq(1, 3).foreach { parallelism =>
    test(s"batch has the same entries every time it is evaluated (parallelism = $parallelism)") {
      val streamKey = Person.generatePersonStreamKey()
      val counts = new ConcurrentLinkedQueue[Long]()
      val ids = new ConcurrentLinkedQueue[String]()
      val options = Map(StreamOptionParallelism -> parallelism.toString)
      foreachBatchStream(streamKey, options) { (batch, _) =>
        // the batch is evaluated twice, e.g. to log its size and then write it
        counts.add(batch.count())
        batch.collect().foreach(row => ids.add(row.getAs[String]("_id")))
      } { _ =>
        withConnection(streamKey) { conn =>
          (1 to 500).foreach { i =>
            conn.xadd(streamKey, new StreamEntryID(i, 0), Person.dataMaps.head.asJava)
          }
        }
        eventually(timeout(10.seconds)) {
          counts.asScala.sum shouldBe 500
          ids.size shouldBe 500
        }
        ids.asScala.toSet shouldBe (1 to 500).map(i => s"$i-0").toSet
      }
    }
  }

  test("first batch is read again when the query is restarted after a failure") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      // consumer group of a previous query, the first batch starts at its last delivered id
      conn.xgroupCreate(streamKey, "spark-source", new StreamEntryID(0, 0), true)
      (1 to 50).foreach { i =>
        conn.xadd(streamKey, new StreamEntryID(0, i), Person.dataMaps.head.asJava)
      }
    }
    val checkpointLocation = s"${new File("target").getAbsolutePath}/checkpoint-test/${UUID.randomUUID()}"
    val writeOptions = Map("checkpointLocation" -> checkpointLocation)

    // the query fails in the first batch
    foreachBatchStream(streamKey, extraWriteOptions = writeOptions) { (batch, _) =>
      batch.collect()
      throw new IllegalStateException("failure in the first batch")
    } { query =>
      intercept[StreamingQueryException](query.awaitTermination(10000))
    }

    // the restarted query executes the first batch again
    val ids = new ConcurrentLinkedQueue[String]()
    foreachBatchStream(streamKey, extraWriteOptions = writeOptions) { (batch, _) =>
      batch.collect().foreach(row => ids.add(row.getAs[String]("_id")))
    } { _ =>
      eventually(timeout(10.seconds)) {
        ids.size shouldBe 50
      }
      ids.asScala.toSet shouldBe (1 to 50).map(i => s"0-$i").toSet
    }
  }

  def foreachBatchStream(streamKey: String,
                         extraReadOptions: Map[String, String] = Map(),
                         extraWriteOptions: Map[String, String] = Map())
                        (processBatch: (DataFrame, Long) => Unit)
                        (body: StreamingQuery => Unit): Unit = {
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    val readerBase = spark.readStream
      .format("redis")
      .schema(Person.fullSchema)
      .option(StreamOptionStreamKeys, streamKey)
    val reader = extraReadOptions.foldLeft(readerBase) { case (r, (k, v)) => r.option(k, v) }

    val writerBase = reader.load().writeStream.foreachBatch(processBatch)
    val writer = extraWriteOptions.foldLeft(writerBase) { case (w, (k, v)) => w.option(k, v) }

    val query = writer.start()
    // give some time for spark query to start
    Thread.sleep(2000)
    try {
      body(query)
    } finally {
      query.stop()
      spark.stop()
    }
  }

  test("a value that can't be parsed names the stream key, the entry, the field, the value and the type (#358)") {
    val streamKey = Person.generatePersonStreamKey()
    withConnection(streamKey) { conn =>
      readStream(streamKey) { spark =>
        conn.xadd(streamKey, new StreamEntryID(7, 3), (Person.dataMaps.head + ("age" -> "thirty")).asJava)
        val query = spark.streams.active.head
        val e = interceptSparkErr[StreamingQueryException] {
          query.awaitTermination(10000)
        }
        val parseError = findCause[NumberFormatException](e)
        parseError.getMessage should include(s"'$streamKey'")
        parseError.getMessage should include("7-3")
        parseError.getMessage should include("'age'")
        parseError.getMessage should include("'thirty'")
        parseError.getMessage should include("integer")
      }
    }
  }

  def readStream(streamKey: String,
                 extraReadOptions: Map[String, String] = Map(),
                 extraWriteOptions: Map[String, String] = Map(),
                 writeFormat: String = "memory",
                 schema: StructType = Person.fullSchema)(body: SparkSession => Unit): Unit = {

    val (spark, query) = readStream2(streamKey, extraReadOptions, extraWriteOptions, writeFormat, schema)
    // give some time for spark query to start
    Thread.sleep(2000)
    try {
      body(spark)
    } finally {
      query.stop()
      spark.stop()
    }
  }

  def readStream2(streamKey: String,
                  extraReadOptions: Map[String, String],
                  extraWriteOptions: Map[String, String],
                  writeFormat: String,
                  schema: StructType = Person.fullSchema): (SparkSession, StreamingQuery) = {
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    val readerBase = spark.readStream
      .format("redis")
      .schema(schema)
      .option(StreamOptionStreamKeys, streamKey)

    // apply extra reader options
    val reader = extraReadOptions.foldLeft(readerBase) { case (r, (k, v)) => r.option(k, v) }

    val persons = reader.load()
    val queryBase = persons.writeStream
      .format(writeFormat)
      .queryName("persons")

    // apply extra writer options
    val queryWithOptions = extraWriteOptions.foldLeft(queryBase) { case (r, (k, v)) => r.option(k, v) }

    val query = queryWithOptions.start()

    logInfo(s"query id ${query.id}")
    (spark, query)
  }

  def checkLastDeliveredId(streamKey: String, lastDeliveredId: String): Unit = {
    eventually(timeout(5.seconds)) {
      withConnection(redisConfig.connectionForKey(streamKey)) { conn =>
        // Jedis parses the nil entries-read that Redis 7+ returns after XGROUP SETID
        val group = conn.xinfoGroup(streamKey).asScala.find(_.getName == "spark-source")
        group.map(_.getLastDeliveredId.toString) shouldBe Some(lastDeliveredId)
      }
    }
  }

  def checkCount(spark: SparkSession, rowsNum: Int): Unit = {
    eventually(timeout(5.seconds)) {
      val resultDf = spark.sql("select * from persons")
      resultDf.count() shouldBe rowsNum
      resultDf.show()
    }
  }

  def checkCountAndLastItem(spark: SparkSession, lastDeliveredId: String, rowsNum: Int): Unit = {
    eventually(timeout(5.seconds)) {
      val resultDf = spark.sql("select * from persons")
      val result = resultDf.collect()
      result.length should be(rowsNum)
      result.map(row => row.getAs[String]("_id")).last shouldBe lastDeliveredId
    }
  }

}
