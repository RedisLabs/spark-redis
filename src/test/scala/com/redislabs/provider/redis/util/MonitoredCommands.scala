package com.redislabs.provider.redis.util

import java.util.concurrent.ConcurrentLinkedQueue

import com.redislabs.provider.redis.RedisEndpoint
import com.redislabs.provider.redis.env.Env
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import org.scalatest.Assertions
import redis.clients.jedis.{Jedis, JedisMonitor}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * Records, with `MONITOR`, the commands the primaries receive while a test runs, to check exactly what a write
  * sends: which commands, in which order, and on which connection.
  */
trait MonitoredCommands extends Assertions { this: Env =>

  import MonitoredCommands._

  /** Each primary once. `redisConfig.hosts` lists a cluster primary once per slot range it owns. */
  protected def primaries: Seq[RedisEndpoint] =
    redisConfig.hosts.toSeq.map(_.endpoint).groupBy(e => (e.host, e.port)).values.map(_.head).toSeq

  /** Every command the primaries receive while `body` runs, in the order each primary received them. */
  protected def monitored(body: => Unit): Seq[Command] = {
    val monitors = primaries.map(new Monitor(_))
    try {
      monitors.foreach(_.sync("monitored-commands-start"))
      body
      monitors.foreach(_.sync("monitored-commands-end"))
    } finally {
      monitors.foreach(_.stop())
    }
    monitors.zipWithIndex.flatMap { case (monitor, n) =>
      monitor.lines.asScala.toSeq.collect { case MonitorLine(client, args) =>
        // client addresses are only unique per primary
        Command(s"$n/$client", QuotedArg.findAllMatchIn(args).map(_.group(1)).toList)
      }
    }
  }

  /** Runs MONITOR on one primary, on its own connection (not a pooled one), until stopped. */
  private class Monitor(endpoint: RedisEndpoint) {
    val lines = new ConcurrentLinkedQueue[String]()
    private val conn = new Jedis(endpoint.host, endpoint.port)
    if (endpoint.auth != null) conn.auth(endpoint.auth)

    private val thread = new Thread(new Runnable {
      override def run(): Unit =
        try {
          conn.monitor(new JedisMonitor {
            override def onCommand(command: String): Unit = lines.add(command)
          })
        } catch {
          case NonFatal(_) => // stop() closes the connection, which ends MONITOR
        }
    })
    thread.setDaemon(true)
    thread.start()

    /** Sends `marker` to the primary until MONITOR has reported it, so everything sent before it was seen. */
    def sync(marker: String): Unit = withConnection(endpoint.connect()) { c =>
      val deadline = System.currentTimeMillis() + 10000
      while (!lines.asScala.exists(_.contains(marker))) {
        if (System.currentTimeMillis() > deadline) {
          fail(s"MONITOR on ${endpoint.host}:${endpoint.port} did not report $marker within 10 s")
        }
        c.echo(marker)
        Thread.sleep(20)
      }
    }

    def stop(): Unit = {
      conn.disconnect()
      thread.join(10000)
    }
  }
}

object MonitoredCommands {

  /** A command one primary received, from one client connection, as MONITOR reports it. */
  final case class Command(client: String, args: Seq[String]) {
    def name: String = args.head.toUpperCase

    def key: Option[String] = args.lift(1)

    /** The arguments, with HSET's field-value pairs as a map, since their order is not defined. */
    def normalized: Seq[Any] = name match {
      case "HSET" => Seq(name, args(1), args.drop(2).grouped(2).map(p => p.head -> p(1)).toMap)
      case _ => name +: args.tail
    }
  }

  private[util] val MonitorLine = """^\S+ \[\d+ (\S+)\] (.*)$""".r
  private[util] val QuotedArg = """"((?:[^"\\]|\\.)*)"""".r
}
