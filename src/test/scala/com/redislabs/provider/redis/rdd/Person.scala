package com.redislabs.provider.redis.rdd

import java.util.UUID

/**
  * @author The Viet Nguyen
  */
case class Person(name: String, age: Int, address: String, salary: Double)

object Person {

  val TableNamePrefix = "person"

  val data = Seq(
    Person("John", 30, "60 Wall Street", 150.5),
    Person("Peter", 35, "110 Wall Street", 200.3)
  )

  def generateTableName(prefix: String): String = {
    // generate random table, so we can run test multiple times and not append/overwrite data
    prefix + UUID.randomUUID().toString.replace("-", "")
  }
}
