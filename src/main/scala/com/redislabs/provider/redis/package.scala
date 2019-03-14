package com.redislabs.provider

package object redis extends RedisFunctions {
  val RedisSslScheme: String = "rediss"
  val RedisDataTypeHash: String = "hash"
  val RedisDataTypeString: String = "string"
}
