package com.redislabs.provider.redis

import org.apache.spark.streaming.{Seconds, StreamingContext}

trait SparkStreamingRedisSuite extends SparkRedisSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ssc = new StreamingContext(sc, Seconds(1))
  }

  override def afterAll(): Unit = {
    ssc.stop()
    super.afterAll()
  }

}
