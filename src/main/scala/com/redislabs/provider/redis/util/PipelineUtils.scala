package com.redislabs.provider.redis.util

import java.util.{List => JList}

import com.redislabs.provider.redis.ReadWriteConfig
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.JavaConverters._
import scala.collection.{TraversableOnce, mutable}

object PipelineUtils {

  /**
    * Executes a pipeline function for each item in the sequence, returns the server response.
    *
    * Ensures that a new pipeline is created if the number of operations exceeds the given maxPipelineSize
    * while iterating over the items.
    *
    * @param conn            jedis connection
    * @param readWriteConfig read/write config
    * @param items           a sequence of elements (typically keys)
    * @param f               function to applied for each item in the sequence
    * @return response from the server
    */
  def mapWithPipeline[A](conn: Jedis, items: TraversableOnce[A])(f: (Pipeline, A) => Unit)
                        (implicit readWriteConfig: ReadWriteConfig): Seq[AnyRef] = {
    val totalResp = mutable.ListBuffer[JList[AnyRef]]()

    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % readWriteConfig.maxPipelineSize == 0) {
        val resp = pipeline.syncAndReturnAll()
        totalResp += resp
        pipeline = conn.pipelined()
      }
    }

    // sync remaining items
    if (i % readWriteConfig.maxPipelineSize != 0) {
      val resp = pipeline.syncAndReturnAll()
      totalResp += resp
    }

    totalResp.flatMap(_.asScala)
  }

  /**
    * Executes a pipeline function for each item in the sequence. No response is returned.
    *
    * Ensures that a new pipeline is created if the number of operations exceeds the given maxPipelineSize
    * while iterating over the items.
    *
    * @param conn            jedis connection
    * @param readWriteConfig read/write config
    * @param items           a sequence of elements (typically keys)
    * @param f               function to applied for each item in the sequence
    * @throws JedisDataException if Redis replies with an error to any of the commands, see [[syncAndCheck]]
    */
  def foreachWithPipeline[A](conn: Jedis, items: TraversableOnce[A])(f: (Pipeline, A) => Unit)
                            (implicit readWriteConfig: ReadWriteConfig): Unit = {
    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % readWriteConfig.maxPipelineSize == 0) {
        syncAndCheck(pipeline)
        pipeline = conn.pipelined()
      }
    }

    // sync remaining items
    if (i % readWriteConfig.maxPipelineSize != 0) {
      syncAndCheck(pipeline)
    }
  }

  /**
    * Executes a pipeline function for each item in the sequence. Doesn't sync and return the last pipeline after
    * all operations are executed. Allows to execute more operations with the returned pipeline.
    * The client is responsible of syncing the returned pipeline.
    *
    * Ensures that a new pipeline is created if the number of operations exceeds the given maxPipelineSize
    * while iterating over the items.
    *
    * @param conn            jedis connection
    * @param readWriteConfig read/write config
    * @param items           a sequence of elements (typically keys)
    * @param f               function to applied for each item in the sequence
    * @return the last pipeline
    * @throws JedisDataException if Redis replies with an error to any of the commands synced so far,
    *                            see [[syncAndCheck]]
    */
  def foreachWithPipelineNoLastSync[A](conn: Jedis, items: TraversableOnce[A])(f: (Pipeline, A) => Unit)
                                      (implicit readWriteConfig: ReadWriteConfig): Pipeline = {
    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % readWriteConfig.maxPipelineSize == 0) {
        syncAndCheck(pipeline)
        pipeline = conn.pipelined()
      }
    }

    // return pipeline, the client should sync pipeline with syncAndCheck
    pipeline
  }

  /**
    * Sends the pipelined commands, reads all their replies and fails if Redis replied with an error to any of them.
    *
    * `Pipeline.sync()` reads the replies but keeps error replies (e.g. OOM or WRONGTYPE) inside the discarded
    * `Response` objects, so a write that Redis rejected would go unnoticed. All the replies are read before
    * throwing, so the connection can still be used, or returned to the pool, afterwards.
    *
    * The pipeline is not a transaction: the commands that Redis accepted stay applied.
    *
    * @param pipeline pipeline to sync
    * @throws JedisDataException with the number of failed commands and the first Redis error message
    */
  def syncAndCheck(pipeline: Pipeline): Unit = {
    val replies = pipeline.syncAndReturnAll().asScala
    val errors = replies.collect { case e: JedisDataException => e }
    if (errors.nonEmpty) {
      throw new JedisDataException(
        s"Redis rejected ${errors.size} of ${replies.size} pipelined commands; first error: ${errors.head.getMessage}",
        errors.head)
    }
  }

}
