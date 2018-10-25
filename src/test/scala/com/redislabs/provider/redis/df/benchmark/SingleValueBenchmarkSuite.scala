package com.redislabs.provider.redis.df.benchmark

import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.Person
import org.apache.spark.rdd.RDD

/**
  * @author The Viet Nguyen
  */
trait SingleValueBenchmarkSuite extends DataframeBenchmarkSuite with RedisClusterEnv {

  override def suiteTags: String = s"${super.suiteTags}, Single"

  override def rdd(): RDD[Person] = {
    spark.sparkContext.parallelize(Seq(Person(s"John", 30, "60 Wall Street", 150.5)))
  }
}
