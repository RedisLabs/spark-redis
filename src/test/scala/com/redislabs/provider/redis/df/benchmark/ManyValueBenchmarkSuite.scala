package com.redislabs.provider.redis.df.benchmark

import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.Person
import org.apache.spark.rdd.RDD

/**
  * @author The Viet Nguyen
  */
trait ManyValueBenchmarkSuite extends DataframeBenchmarkSuite with RedisClusterEnv {

  private def num = 1000000

  override def suiteTags: String = s"${super.suiteTags}, Many:$num"

  override def rdd(): RDD[Person] = {
    val partitionsNum = 8
    val sectionLength = num / partitionsNum
    spark.sparkContext
      .parallelize(0 until partitionsNum, partitionsNum)
      .mapPartitions {
        _
          .flatMap { i =>
            val start = i * sectionLength
            val end = start + sectionLength + 1
            Stream.range(start, end)
          }
          .map { i =>
            Person(s"John-$i", 30, "60 Wall Street", 150.5)
          }
      }
  }
}
