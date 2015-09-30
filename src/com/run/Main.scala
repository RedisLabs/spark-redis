package com.run

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.redislabs.provider.redis._


import java.net.InetAddress
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark._
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._
import com.redislabs.provider.util.ImportTimeSeriesData._
import com.redislabs.provider.redis.partitioner._
import com.redislabs.provider.RedisConfig
import com.redislabs.provider.redis._

import com.cloudera.sparkts._
import com.cloudera.sparkts.DateTimeIndex._

import com.github.nscala_time.time.Imports._

import breeze.linalg._
import breeze.numerics._


object Main {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    //ImportToRedisServer("/home/hadoop/timeseries/", "task1", sc, ("127.0.0.1", 6379))
    val kRDD = sc.fromRedisKeyPattern(("127.0.0.1", 6379), "task1*")
    val dtIndex = uniform(new DateTime("2012-10-17"), new DateTime("2014-10-17"), 1.businessDays)
    kRDD.getRedisTimeSeriesRDD(dtIndex).filterEndingAfter(new DateTime("2014-10-11")).print//foreach(x => println(x._1))
    
    //val jedis = new Jedis("127.0.0.1", 6379)
    //val pipeline = jedis.pipelined

    //val zsetmap = jedis.zrangeWithScores("z1", 0, -1).map(x => (x.getScore.toInt, x.getElement.toDouble)).toMap
    //zsetmap.foreach(println)
    //println(zsetmap.get(4))
    /*
    val res = pipeline.syncAndReturnAll.flatMap{ x => 
      (x.asInstanceOf[java.util.Set[Tuple]]).map(tup => tup.getScore.toInt)
    }
    res.filter(_ > 1).foreach(println)
    res.foreach(println)
    */
  }
}