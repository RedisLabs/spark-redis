package com.redislabs.provider.redis.util

import java.util.UUID

import org.scalatest.Assertions
import scala.reflect.Manifest

object TestUtils {

  def generateTableName(prefix: String): String = {
    // generate random table, so we can run test multiple times and not append/overwrite data
    prefix + UUID.randomUUID().toString.replace("-", "")
  }

  def generateRandomKey(): String = {
    UUID.randomUUID().toString.replace("-", "")
  }

  /**
    * A wrapper of Assertions.intercept() that suppresses spark errors in the logs.
    * It makes easier to analyse unit tests output.
    */
  def interceptSparkErr[T <: AnyRef](f: => Any)(implicit manifest: Manifest[T]): T = {
    // turn off spark logger
    val logger = org.apache.log4j.Logger.getLogger("org")
    val levelBefore = logger.getLevel
    logger.setLevel(org.apache.log4j.Level.OFF)

    // delegate interception
    val interceptRes = Assertions.intercept(f)

    // revert logger
    logger.setLevel(levelBefore)

    interceptRes
  }


}
