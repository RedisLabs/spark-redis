package com.redislabs.provider.redis.util

import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

/**
  * @author The Viet Nguyen
  */
object JsonUtils {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def toJson(any: AnyRef): String = {
    Serialization.write(any)
  }
}
