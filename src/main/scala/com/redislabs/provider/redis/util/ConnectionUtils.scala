package com.redislabs.provider.redis.util

import java.util.{List => JList}

import redis.clients.jedis.Jedis
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.util.SafeEncoder

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
    def xinfo(args: String*): Map[String, AnyRef] = {
      val client = jedis.getClient
      client.sendCommand(XINFO, args: _*)
      val response = asList(client.getOne)
      Map(XINFO.LastEntry -> Map(XINFO.LastEntryIndex ->
        asString(asList(response.get(13)).get(0))))
    }

    private def asList(any: Any): JList[Any] =
      any.asInstanceOf[JList[Any]]

    private def asString(any: Any): String =
      new String(any.asInstanceOf[Array[Byte]])
  }

  object XINFO extends ProtocolCommand {

    val StreamKey = "STREAM"
    val LastEntry = "last-entry"
    val LastEntryIndex = "_id"

    override def getRaw: Array[Byte] = SafeEncoder.encode("XINFO")
  }

}
