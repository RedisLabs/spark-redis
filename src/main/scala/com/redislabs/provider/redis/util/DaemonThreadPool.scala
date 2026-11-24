package com.redislabs.provider.redis.util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

private[redis] object DaemonThreadPool {

  /**
    * A pool of `size` threads, named `<name>-0`, `<name>-1`, ... Its threads are daemons, so they never keep the JVM
    * running after the receiver that started them is gone.
    */
  def fixed(size: Int, name: String): ExecutorService = {
    val created = new AtomicInteger()
    Executors.newFixedThreadPool(size, new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, s"$name-${created.getAndIncrement()}")
        thread.setDaemon(true)
        thread
      }
    })
  }
}
