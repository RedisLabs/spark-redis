package com.redislabs.provider.redis.util

import java.util.{List => JList}

import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._
import scala.collection.mutable

object Utils {

  /**
    * Executes a pipeline function for each item in the sequence, returns the server response.
    *
    * Ensures that a new pipeline is creates if the number of operations exceeds the given maxPipelineSize.
    *
    * @param conn            jedis connection
    * @param maxPipelineSize the maximum number of elements per pipeline
    * @param items           a sequence of elements (typically keys)
    * @param f               function to applied for each item in the sequence
    * @return response from the server
    */
  def mapWithPipeline[A](conn: Jedis, maxPipelineSize: Int, items: Seq[A])(f: (Pipeline, A) => Unit): Iterator[AnyRef] = {
    val totalResp = mutable.ListBuffer[JList[AnyRef]]()

    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % maxPipelineSize == 0) {
        val resp = pipeline.syncAndReturnAll()
        totalResp += resp
      }
    }

    // sync remaining items
    if (i % maxPipelineSize != 0) {
      val resp = pipeline.syncAndReturnAll()
      totalResp += resp
    }

    totalResp.iterator.flatMap(_.asScala)
  }

  /**
    * Executes a pipeline function for each item in the sequence. No response is returned.
    *
    * Ensures that a new pipeline is creates if the number of operations exceeds the given maxPipelineSize.
    *
    * @param conn            jedis connection
    * @param maxPipelineSize the maximum number of elements per pipeline
    * @param items           a sequence of elements (typically keys)
    * @param f               function to applied for each item in the sequence
    */
  def foreachWithPipeline[A](conn: Jedis, maxPipelineSize: Int, items: Seq[A])(f: (Pipeline, A) => Unit): Unit = {
    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % maxPipelineSize == 0) {
        pipeline.sync()
      }
    }

    // sync remaining items
    if (i % maxPipelineSize != 0) {
      pipeline.sync()
    }
  }

}
