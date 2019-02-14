package com.redislabs.provider.redis.util

/**
  * TODO: rename
  */
class StopWatchAdv {

  private var startTime: Long = 0
  private var runtime: Long = 0

  def start(): Unit = {
    startTime = System.currentTimeMillis()
  }

  def stop(): Unit = {
    runtime = runtime + (System.currentTimeMillis() - startTime)
  }

  def getTime(): Long = {
    runtime
  }

  def getTimeSec(): Double = {
    getTime.toDouble / 1000
  }

}
