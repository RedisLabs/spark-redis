package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets
import java.util.{Map => JMap}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import redis.clients.jedis.{Pipeline, Response}

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
class HashRedisSourceRelation(override val sqlContext: SQLContext,
                              parameters: Map[String, String],
                              userSpecifiedSchema: Option[StructType])
  extends RedisSourceRelation[JMap[Array[Byte], Array[Byte]]](sqlContext,
    parameters, userSpecifiedSchema) {

  override def save(pipeline: Pipeline,
                    key: Array[Byte], value: JMap[Array[Byte], Array[Byte]]): Unit =
    pipeline.hmset(key, value)

  override def load(pipeline: Pipeline,
                    key: Array[Byte]): Response[JMap[Array[Byte], Array[Byte]]] =
    pipeline.hgetAll(key)

  override def encodeRow(value: Row): JMap[Array[Byte], Array[Byte]] = {
    value.schema.fields.map(_.name)
      .map { field =>
        field -> value.getAs[Any](field)
      }
      .map { case (k, v) =>
        k.getBytes(StandardCharsets.UTF_8) ->
          String.valueOf(v).getBytes(StandardCharsets.UTF_8)
      }
      .toMap
      .asJava
  }

  override def decodeRow(value: JMap[Array[Byte], Array[Byte]], schema: StructType): Row = ???
}
