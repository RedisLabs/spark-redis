package org.apache.spark.sql.redis

import com.redislabs.provider.redis.RedisConfig.{PasswordConfKey, PasswordOption}
import org.apache.spark.SparkConf
import org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils
import org.scalatest.{FunSuite, Matchers}

/**
  * Spark hides the values of settings and source options whose names match `spark.redaction.regex`. These tests run
  * Spark's own redaction, with its default pattern, on the names Spark-Redis reads the password from
  * (GHSA-wmm3-vmjq-7mmc), so they hold for whichever Spark version the build uses. `Utils.redact(conf, ...)` is what
  * the Spark UI's Environment tab, the History Server and the event log use. The suite is in an org.apache.spark
  * package to reach that private[spark] code.
  */
class PasswordRedactionSuite extends FunSuite with Matchers {

  /**
    * Spark also redacts a value that matches the pattern itself, whatever its key, so a value such as "my-secret"
    * would be hidden under any name and the tests would prove nothing. The control tests below check this one isn't.
    */
  private val password = "hunter2"

  private def redactConf(key: String): String = {
    val conf = new SparkConf(false).set(key, password)
    conf.contains(SECRET_REDACTION_PATTERN.key) shouldBe false
    Utils.redact(conf, conf.getAll.toSeq).toMap.apply(key)
  }

  private def redactOption(key: String): String =
    new SQLConf().redactOptions(Map(key -> password)).apply(key)

  test("control: Spark's default pattern doesn't hide the test password under an unrelated name") {
    redactConf("spark.redis.host") shouldBe password
    redactOption(SqlOptionTableName) shouldBe password
  }

  test("Spark's default pattern hides spark.redis.password") {
    redactConf(PasswordConfKey) should not include password
  }

  test("Spark hides the password option where query plans show source options") {
    redactOption(PasswordOption) should not include password
  }
}
