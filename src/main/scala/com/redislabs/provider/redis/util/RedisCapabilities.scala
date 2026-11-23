package com.redislabs.provider.redis.util

import java.util.{List => JList}

import com.redislabs.provider.redis.RedisConfig
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import redis.clients.jedis.Jedis
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.util.SafeEncoder

import scala.collection.JavaConverters._

/**
  * What one write may send. Decided once per write, on the driver, by [[RedisCapabilities.forWrite]].
  *
  * @param msetex     every primary has `MSETEX` (Redis 8.4+)
  * @param slotScoped the server may be a cluster, so the keys of one multi-key command must share a hash slot
  */
case class WriteCapabilities(msetex: Boolean, slotScoped: Boolean)

/**
  * Asks Redis what it can do. A command is probed with `COMMAND INFO <name>`, never inferred from the server's
  * version: Redis Open Source, Redis Software and Redis Cloud number their versions differently, and the last two can
  * disable commands.
  */
object RedisCapabilities extends Logging {

  /** `MSETEX numkeys key value [key value ...] EX seconds`: several strings and one TTL, atomically. Redis 8.4+. */
  val Msetex = "MSETEX"

  private object CommandCommand extends ProtocolCommand {
    override def getRaw: Array[Byte] = SafeEncoder.encode("COMMAND")
  }

  private object HelloCommand extends ProtocolCommand {
    override def getRaw: Array[Byte] = SafeEncoder.encode("HELLO")
  }

  /**
    * Probes every primary once. `MSETEX` is used only if all of them have it, since a cluster in the middle of a
    * rolling upgrade can mix versions.
    */
  def forWrite(redisConfig: RedisConfig): WriteCapabilities = {
    val primaries = redisConfig.hosts.map(_.endpoint).distinct
    val answers = primaries.map { endpoint =>
      withConnection(endpoint.connect()) { conn =>
        (hasCommand(conn, Msetex), isStandalone(conn))
      }
    }
    val capabilities = WriteCapabilities(
      msetex = answers.nonEmpty && answers.forall(_._1),
      slotScoped = !answers.forall(_._2))
    logInfo(s"Write capabilities of ${primaries.map(e => s"${e.host}:${e.port}").mkString(", ")}: $capabilities")
    capabilities
  }

  /**
    * True if the server knows `command`. An error reply, such as an ACL that denies `COMMAND`, counts as no, so the
    * caller takes its fallback.
    */
  def hasCommand(conn: Jedis, command: String): Boolean = {
    try {
      conn.sendCommand(CommandCommand, SafeEncoder.encode("INFO"), SafeEncoder.encode(command)) match {
        case reply: JList[_] => reply.size == 1 && reply.get(0) != null
        case _ => false
      }
    } catch {
      case e: JedisDataException =>
        logInfo(s"COMMAND INFO $command failed, so $command is treated as missing: ${e.getMessage}")
        false
    }
  }

  /**
    * True only if the server says it is not a cluster: `HELLO` with no arguments keeps the protocol and reports the
    * server's `mode`. When it can't tell, the answer is false, and grouping keys by slot is correct on any server.
    */
  def isStandalone(conn: Jedis): Boolean = {
    try {
      conn.sendCommand(HelloCommand, Seq.empty[Array[Byte]]: _*) match {
        case reply: JList[_] =>
          // toList: on Scala 2.13, grouped chunks of a Java list are mutable Buffers, which `case Seq(...)` misses
          reply.asScala.toList.grouped(2).exists {
            case List(name: Array[Byte], value: Array[Byte]) =>
              SafeEncoder.encode(name) == "mode" && SafeEncoder.encode(value) == "standalone"
            case _ => false
          }
        case _ => false
      }
    } catch {
      case e: JedisDataException =>
        logInfo(s"HELLO failed, so keys are grouped by hash slot: ${e.getMessage}")
        false
    }
  }
}
