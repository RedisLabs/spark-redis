package com.redislabs.provider.redis.env

import com.redislabs.provider.redis.RedisConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

trait Env {

  val conf: SparkConf
  var spark: SparkSession = _
  var sc: SparkContext = _
  var ssc: StreamingContext = _

  val redisHost = "127.0.0.1"
  /** The port of this environment's Docker Redis, chosen at run time (see RedisContainers). */
  def redisPort: Int = RedisContainers.standalonePort
  val redisAuth = "passwd" // password for 'default' user (AUTH <password>)

  // user credentials
  val user = "alice"
  val userPassword = "p1pp0"

  val redisConfig: RedisConfig
}

