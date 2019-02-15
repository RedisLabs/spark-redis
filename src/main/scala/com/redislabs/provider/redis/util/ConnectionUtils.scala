package com.redislabs.provider.redis.util

import java.util.{List => JList}

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.XINFO.{SubCommandGroups, SubCommandStream}
import redis.clients.jedis.Jedis
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.util.SafeEncoder

import scala.collection.JavaConverters._

/**
  * @author The Viet Nguyen
  */
object ConnectionUtils {

  def withConnection[A](conn: Jedis)(body: Jedis => A): A = {
    try {
      body(conn)
    } finally {
      conn.close()
    }
  }

  def withConnection[A](streamKey: String)(body: Jedis => A)(implicit redisConfig: RedisConfig): A = {
    withConnection(redisConfig.connectionForKey(streamKey)){
      body
    }
  }

  implicit class JedisExt(val jedis: Jedis) extends AnyVal {

    //TODO: temporary solution to get latest offset while not supported by Jedis
    def xinfo(command: String, args: String*): Map[String, Any] = {
      val client = jedis.getClient
      val combinedArgs = command +: args
      client.sendCommand(XINFO, combinedArgs: _*)
      val response = asList(client.getOne).asScala
      command match {
        case SubCommandStream =>
          asMap(response)
        case SubCommandGroups =>
          response.map(m => asList(m)).map(_.asScala).map(asMap)
            .map(m => String.valueOf(m("name")) -> m).toMap
      }
    }

    private def asMap(seq: Seq[Any]): Map[String, Any] = {
      seq.grouped(2)
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

    val SubCommandStream = "STREAM"
    val SubCommandGroups = "GROUPS"

    val LastGeneratedId = "last-generated-id"
    val LastDeliveredId = "last-delivered-id"
    val LastEntry = "last-entry"
    val EntryId = "_id"

    override def getRaw: Array[Byte] = SafeEncoder.encode("XINFO")
  }

}
