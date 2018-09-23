package com.redislabs.provider.redis.util

/**
  * @author The Viet Nguyen
  */
object Logger {

  def info(msg: => String): Unit =
    println(msg)
}
