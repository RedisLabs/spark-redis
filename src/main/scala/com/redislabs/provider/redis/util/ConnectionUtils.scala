package com.redislabs.provider.redis.util

import java.util.{List => JList}

import redis.clients.jedis.Jedis
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.util.SafeEncoder

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
object ConnectionUtils {

  def withConnection[A](conn: Jedis)(body: Jedis => A): A = {
    val res = body(conn)
    conn.close()
    res
  }

  implicit class JedisExt(jedis: Jedis) {

    //TODO: temporary solution to get latest offset while not supported by Jedis
    def xinfo(args: String*): Map[String, Any] = {
      val client = jedis.getClient
      client.sendCommand(XINFO, args: _*)
      asList(client.getOne).asScala.grouped(2)
        .map { group =>
          val key = asString(group.head)
          val value = group(1) match {
            case arr: Array[Byte] => asString(arr)
            case other: Any => other
          }
          key -> value
        }.toMap
    }

    private def asList(any: Any): JList[Any] =
      any.asInstanceOf[JList[Any]]

    private def asString(any: Any): String =
      new String(any.asInstanceOf[Array[Byte]])
  }

  object XINFO extends ProtocolCommand {

    val StreamKey = "STREAM"
    val LastGeneratedId = "last-generated-id"
    val LastEntry = "last-entry"
    val EntryId = "_id"

    override def getRaw: Array[Byte] = SafeEncoder.encode("XINFO")
  }

}
