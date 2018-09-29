package com.redislabs.provider.redis.sql

import com.redislabs.provider.redis.rdd.Person.data
import com.redislabs.provider.redis.rdd.{Person, SparkRedisSuite}
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers

/**
  * @author The Viet Nguyen
  */
trait DefaultTestDataset extends SparkRedisSuite with Matchers {

  import TestSqlImplicits._

  lazy val expectedDf: DataFrame = Person.df(spark)

  def verifyDf(actualDf: DataFrame): Unit = {
    actualDf.show()
    actualDf.count() shouldBe expectedDf.count()
    // TODO: check nullable columns
    // actualDf.schema shouldBe expectedDf.schema
    val loadedArr = actualDf.as[Person].collect()
    loadedArr.sortBy(_.name) shouldBe data.toArray.sortBy(_.name)
  }
}
