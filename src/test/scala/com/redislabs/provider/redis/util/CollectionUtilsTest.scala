package com.redislabs.provider.redis.util

import com.redislabs.provider.redis.util.CollectionUtils.RichCollection
import org.scalatest.{FunSuite, Matchers}

/**
  * @author The Viet Nguyen
  */
class CollectionUtilsTest extends FunSuite with Matchers {

  test("distinctBy") {
    val persons = Seq(Person("John", 30, "60 Wall Street", 150.5),
      Person("John", 30, "18 Main Street", 150.5), Person("Peter", 35, "110 Wall Street", 200.3))
    val distinctPersons = persons.distinctBy(_.name)
    distinctPersons shouldBe Seq(Person("John", 30, "60 Wall Street", 150.5),
      Person("Peter", 35, "110 Wall Street", 200.3))
  }
}
