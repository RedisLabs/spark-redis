package com.redislabs.provider

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import breeze.linalg._
import breeze.numerics._

object ImportTimeSeriesData {
  def Write(sc: SparkContext, text: String, keyPrefix: String = "") = {
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
      val parallRDD = sc.parallelize(0 to samples.length)
      parallRDD.map(i => (dts(i), vals(i))).foreach(println)
    })

  }
  def ImportToRedisServer(dir: String, sc: SparkContext) = {
    sc.wholeTextFiles(dir).map {
      case (path, text) => ImportTimeSeriesData.Write(sc, text, path.split('/').last)
    }
  }
}