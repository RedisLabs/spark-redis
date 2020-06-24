package com.redislabs.provider.redis

import java.io.{File, FileWriter, PrintWriter}
import java.time.{Duration => JDuration}

import com.redislabs.provider.redis.util.Logging

/**
  * @author The Viet Nguyen
  */
trait RedisBenchmarks extends Logging {

  val benchmarkReportDir = new File("target/reports/benchmarks/")
  benchmarkReportDir.mkdirs()

  def time[R](tag: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    new PrintWriter(new FileWriter(s"$benchmarkReportDir/results.txt", true)) {
      // scalastyle:off
      this.println(s"$tag, ${JDuration.ofNanos(t1 - t0)}")
      close()
    }
    result
  }
}
