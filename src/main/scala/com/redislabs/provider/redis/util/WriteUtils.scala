package com.redislabs.provider.redis.util

import org.apache.spark.sql.types._
import redis.clients.jedis.exceptions.JedisDataException

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import scala.util.{Failure, Success, Try}

/**
  * @author The Viet Nguyen
  */
object WriteUtils {
  def convertValue(fieldValue: Any): Array[Byte] = {
    if (fieldValue == null) {
      null
    } else {
      convertNotNullValue(fieldValue)
    }
  }

  // scalastyle:off cyclomatic.complexity
  private def convertNotNullValue(data: Any): Array[Byte] = {
    data match {
      case x: Int => ByteBuffer.allocate(4).putInt(x).array()
      case x: Byte => Array(x)
      case x: Long => ByteBuffer.allocate(8).putLong(x).array()
      case x: Float => ByteBuffer.allocate(4).putFloat(x).array()
      case x: Double => ByteBuffer.allocate(8).putDouble(x).array()
      case x: Boolean => if (x) Array[Byte](1) else Array[Byte](0)
      case x: Short => ByteBuffer.allocate(2).putShort(x).array()
      case x: Date => ByteBuffer.allocate(8).putLong(x.getTime).array()
      case x: java.sql.Timestamp => ByteBuffer.allocate(8).putLong(x.getTime).array()
      case x: String => x.getBytes(StandardCharsets.UTF_8)
      case x: Array[Byte] => x
      case _ => throw new IllegalArgumentException("Unsupported data type")
    }
  }
}
