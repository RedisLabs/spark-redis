package com.redislabs.provider.redis.rdd

import org.apache.spark.sql.{Encoder, Encoders}

/**
  * @author The Viet Nguyen
  */
case class Person(name: String, age: Int, address: String, salary: Double)

object Person {
  implicit val personEncoder: Encoder[Person] = Encoders.product[Person]
}

