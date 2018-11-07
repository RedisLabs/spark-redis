package com.redislabs.provider.redis.df.benchmark

import com.redislabs.provider.redis.df.RedisDataframeSuite
import com.redislabs.provider.redis.rdd.Keys
import com.redislabs.provider.redis.util.Person.TableNamePrefix
import com.redislabs.provider.redis.util.PipelineUtils.foreachWithPipeline
import com.redislabs.provider.redis.util.TestUtils.generateTableName
import com.redislabs.provider.redis.util.{Logging, Person}
import com.redislabs.provider.redis.{ReadWriteConfig, RedisBenchmarks, toRedisContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.redis._
import redis.clients.jedis.PipelineBase

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

  test(s"$suiteTags, Read all fields") {
    time(s"$suiteTags, Read all fields") {
      spark.read.format(RedisFormat)
        .option(SqlOptionModel, persistentModel)
        .option(SqlOptionTableName, tableName)
        .option(SqlOptionNumPartitions, 8)
        .load()
        .select("name", "age", "address", "salary")
        .foreach { _ =>
          // measure read all elements
        }
    }
  }

  test(s"$suiteTags, Read 1 fields") {
    time(s"$suiteTags, Read 1 fields") {
      spark.read.format(RedisFormat)
        .option(SqlOptionModel, persistentModel)
        .option(SqlOptionTableName, tableName)
        .option(SqlOptionNumPartitions, 8)
        .load()
        .select("name")
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

  test(s"$suiteTags, Take 10") {
    time(s"$suiteTags, Take 10") {
      spark.read.format(RedisFormat)
        .option(SqlOptionModel, persistentModel)
        .option(SqlOptionTableName, tableName)
        .option(SqlOptionNumPartitions, 8)
        .load()
        .take(10)
    }
  }
}
