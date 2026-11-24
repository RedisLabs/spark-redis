package com.redislabs.provider.redis.util

import java.util.concurrent.{Callable, TimeUnit}

import org.scalatest.{FunSuite, Matchers}

class DaemonThreadPoolTest extends FunSuite with Matchers {

  test("the pool's threads are named daemons, so they never keep the JVM running") {
    val pool = DaemonThreadPool.fixed(2, "RedisTestHandler")
    try {
      val threads = (1 to 2).map { _ =>
        pool.submit(new Callable[(String, Boolean)] {
          override def call(): (String, Boolean) = {
            Thread.sleep(100) // keep this thread busy, so the next task gets the other one
            (Thread.currentThread.getName, Thread.currentThread.isDaemon)
          }
        })
      }.map(_.get(10, TimeUnit.SECONDS))
      threads.map(_._1) should contain theSameElementsAs Seq("RedisTestHandler-0", "RedisTestHandler-1")
      all(threads.map(_._2)) shouldBe true
    } finally {
      pool.shutdownNow()
    }
  }
}
