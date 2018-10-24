package com.redislabs.provider.redis.df

import java.io.File
import java.net.URL

import com.redislabs.provider.redis.rdd.Keys
import com.redislabs.provider.redis.util.Person.{TableNamePrefix, generateTableName}
import com.redislabs.provider.redis.util.PipelineUtils.foreachWithPipeline
import com.redislabs.provider.redis.util.{Logging, Person}
import com.redislabs.provider.redis.{ReadWriteConfig, RedisBenchmarks, toRedisContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.redis._
import redis.clients.jedis.PipelineBase

import scala.sys.process._

/**
  * @author The Viet Nguyen
  */
trait DataframeBenchmarkSuite extends RedisDataframeSuite with RedisBenchmarks with Logging {

  val tableName: String = generateTableName(TableNamePrefix)

  private val startDate = s"${System.currentTimeMillis()}"

  def suiteTags: String = startDate

  def persistentModel: String

  def rdd(): RDD[Person]

  override def afterAll(): Unit = {
    val target = new File("target/reports/benchmarks/")
    target.mkdirs()
    logInfo(s"save Spark dashboard to ${target.getAbsolutePath}")
    (new URL("http://localhost:4040/jobs") #> new File(target, "jobs.html") !!)
    (new URL("http://localhost:4040/stages") #> new File(target, "stages.html") !!)
    time(s"$suiteTags, Cleanup") {
      val hosts = redisConfig.hosts
      implicit val readWriteConfig: ReadWriteConfig = ReadWriteConfig.Default
      sc.fromRedisKeyPattern()
        .foreachPartition { p =>
          Keys.groupKeysByNode(hosts, p)
            .foreach { case (n, ks) =>
              val conn = n.connect()
              foreachWithPipeline(conn, ks) { (pl, k) =>
                (pl: PipelineBase).del(k) // fix ambiguous reference to overloaded definition
              }
              conn.close()
            }
        }
    }
    super.afterAll()
  }

  test(s"$suiteTags, Write") {
    val df = spark.createDataFrame(rdd())
    time(s"$suiteTags, Write") {
      df.write.format(RedisFormat)
        .option(SqlOptionModel, persistentModel)
        .option(SqlOptionTableName, tableName)
        .option(SqlOptionKeyColumn, "name")
        .mode(SaveMode.Overwrite)
        .save()
    }
  }

  test(s"$suiteTags, Read") {
    time(s"$suiteTags, Read") {
      spark.read.format(RedisFormat)
        .option(SqlOptionModel, persistentModel)
        .option(SqlOptionTableName, tableName)
        .option(SqlOptionNumPartitions, 8)
        .load()
        .foreach { _ =>
          // measure read all elements
        }
    }
  }

  test(s"$suiteTags, Read 0 fields") {
    time(s"$suiteTags, Read 0 fields") {
      spark.read.format(RedisFormat)
        .option(SqlOptionModel, persistentModel)
        .option(SqlOptionTableName, tableName)
        .option(SqlOptionNumPartitions, 8)
        .load()
        .select()
        .foreach { _ =>
          // measure read all elements
        }
    }
  }
}

