package com.redislabs.provider.redis.util

/**
  * We use simplistic StopWatch rather than a time{ ... } function to have
  * a cleaner code references (stacktraces) in Spark UI and logs.
  */
class StopWatch {

  val start: Long = System.currentTimeMillis()

  def getTime(): Long = {
    System.currentTimeMillis() - start
  }

  def getTimeSec(): Double = {
    getTime.toDouble / 1000
  }


}
