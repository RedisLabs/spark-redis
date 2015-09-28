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
//import com.redislabs.provider.ImportTimeSeriesData._
import com.redislabs.provider.redis.partitioner._
import com.redislabs.provider.RedisConfig
import com.redislabs.provider.redis._

import com.cloudera.sparkts._
//import com.cloudera.sparkts.DateTimeIndex._

import com.github.nscala_time.time.Imports._

import breeze.linalg._
import breeze.numerics._


object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    ImportToRedisServer("/home/cnis/timeseries", sc)
    //val kRDD = sc.fromRedisKeyPattern(("127.0.0.1", 6379), "z*")
    //val dtIndex = uniform(new DateTime("2014-3-27"), new DateTime("2014-10-24"), 1.businessDays)
    //kRDD.getRedisTimeSeriesRDD(dtIndex).foreach(println)
    
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
  def RedisWrite(text: String, keyPrefix: String = "") = {
    println("Hello World")
    val lines = text.split('\n')
    val labels = lines(0).split(',').tail.map(keyPrefix + _)
    val samples = lines.tail.map(line => {
        val tokens = line.split(',')
        val dt = new DateTime(tokens.head)
        (dt, tokens.tail.map(_.toDouble))
      }
    )
    val mat = new DenseMatrix[Double](samples.length, samples.head._2.length)
    val dts = new Array[Long](samples.length)
    (0 until labels.length).map(i => {
      val (dt, vals) = samples(i)
      dts(i) = dt.getMillis
      mat(i to i, ::) := new DenseVector[Double](vals)
    })
    (0 until samples.head._2.length).map(i => {
      val vals = mat(::, i to i).toArray
      vals.foreach(println)
      //val parallRDD = sc.parallelize(0 to samples.length)
      //parallRDD.map(i => (dts(i), vals(i))).foreach(println)
    })

  }
  def ImportToRedisServer(dir: String, sc: SparkContext) = {
    println("HHH555")
    println(dir)
    //sc.textFile(dir+"/GOOG.csv").foreach(println)
    sc.wholeTextFiles(dir).map {
      case (path, text) => RedisWrite(text, path.split('/').last)
    }
  }
}