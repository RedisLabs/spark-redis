package com.redislabs.provider.redis.util

import java.util.UUID

object TestUtils {

  def generateTableName(prefix: String): String = {
    // generate random table, so we can run test multiple times and not append/overwrite data
    prefix + UUID.randomUUID().toString.replace("-", "")
  }

}
