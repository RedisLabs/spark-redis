package com.redislabs.provider.redis.util

import scala.collection.JavaConverters._

import com.redislabs.provider.redis.RedisEndpoint
import org.scalatest.Assertions
import redis.clients.jedis.{Jedis, Protocol}
import redis.clients.jedis.params.MigrateParams

/**
  * Moves hash slots between the primaries of the test cluster the way `redis-cli --cluster reshard` does:
  * `CLUSTER SETSLOT … IMPORTING` and `MIGRATING`, `MIGRATE`, then `CLUSTER SETSLOT … NODE` on every primary.
  *
  * Every step can be run again, so a test's `finally` block can call [[moveSlot]] to finish a move that stopped
  * part way, then move the slot back.
  */
object ClusterSlots {

  private val MigrateBatch = 100
  private val MigrateTimeoutMs = 5000
  private val AgreeTimeoutMs = 10000
  private val StableMs = 500
  private val PollMs = 50

  /**
    * A primary, from `CLUSTER NODES`. `slots` holds the ranges it owns, without importing or migrating slots.
    * `epoch` is its config epoch: when two primaries claim the same slot, the cluster gives it to the higher one.
    */
  case class Primary(id: String, host: String, port: Int, epoch: Long, slots: Seq[(Int, Int)]) {
    def owns(slot: Int): Boolean = slots.exists { case (start, end) => start <= slot && slot <= end }
  }

  def withNode[A](node: Primary)(body: Jedis => A): A = {
    val conn = RedisEndpoint(node.host, node.port).connect()
    try body(conn)
    finally conn.close()
  }

  /** The cluster's primaries, as the node at `host`:`port` sees them. */
  def primaries(host: String, port: Int): Seq[Primary] = {
    val conn = RedisEndpoint(host, port).connect()
    try primaries(conn)
    finally conn.close()
  }

  /** The primaries as `node` sees them. */
  def primaries(node: Jedis): Seq[Primary] = {
    node.clusterNodes().split("\n").map(_.trim).filter(_.nonEmpty).toSeq.flatMap { line =>
      val fields = line.split(" ")
      // <id> <ip:port@cport[,hostname]> <flags> <primary> <ping> <pong> <epoch> <link> <slot> <slot> ...
      val address = fields(1).takeWhile(c => c != '@' && c != ',')
      val colon = address.lastIndexOf(':')
      val slots = fields.drop(8).filterNot(_.startsWith("[")).map { range =>
        range.split("-") match {
          case Array(start, end) => (start.toInt, end.toInt)
          case Array(single) => (single.toInt, single.toInt)
        }
      }
      if (fields(2).split(",").contains("master")) {
        val host = address.substring(0, colon)
        val port = address.substring(colon + 1).toInt
        Some(Primary(fields(0), host, port, fields(6).toLong, slots.toSeq))
      } else {
        None
      }
    }
  }

  /** True if `node` says it owns `slot` itself. */
  def ownsItself(node: Primary, slot: Int): Boolean =
    withNode(node)(conn => primaries(conn).exists(p => p.id == node.id && p.owns(slot)))

  /**
    * Starts moving `slot` from `from` to `to`: `to` imports it and `from` migrates it. `from` still owns the slot,
    * and answers `ASK` for keys it doesn't hold. Skips each step that is already done.
    */
  def beginMove(slot: Int, from: Primary, to: Primary): Unit = {
    if (!ownsItself(to, slot)) withNode(to)(_.clusterSetSlotImporting(slot, from.id))
    if (ownsItself(from, slot)) withNode(from)(_.clusterSetSlotMigrating(slot, to.id))
  }

  /**
    * Finishes moving `slot`: migrates the keys still on `from`, assigns the slot to `to` on every primary, then waits
    * until that assignment can no longer be undone by the cluster bus (see [[awaitOwner]]).
    */
  def finishMove(slot: Int, from: Primary, to: Primary, all: Seq[Primary]): Unit = {
    withNode(from) { conn =>
      var keys = conn.clusterGetKeysInSlot(slot, MigrateBatch).asScala
      while (keys.nonEmpty) {
        conn.migrate(to.host, to.port, 0, MigrateTimeoutMs, MigrateParams.migrateParams(), keys.toSeq: _*)
        keys = conn.clusterGetKeysInSlot(slot, MigrateBatch).asScala
      }
    }
    // The new owner first, so a node that redirects a client there finds it already knows it owns the slot.
    assign(slot, to, to +: all.filterNot(_.id == to.id))
    withNode(to)(bumpEpoch)
    awaitOwner(slot, to, all)
  }

  /** `CLUSTER SETSLOT slot NODE owner` on each of `nodes`, in order. */
  private def assign(slot: Int, owner: Primary, nodes: Seq[Primary]): Unit =
    nodes.foreach(node => withNode(node)(_.clusterSetSlotNode(slot, owner.id)))

  /** `CLUSTER BUMPEPOCH`: gives the node a config epoch higher than any it knows of, unless it already has one. */
  private def bumpEpoch(node: Jedis): Unit = node.sendCommand(Protocol.Command.CLUSTER, "BUMPEPOCH")

  /** Moves `slot` and all of its keys from `from` to `to`. Safe to run again, part way through or after it ends. */
  def moveSlot(slot: Int, from: Primary, to: Primary, all: Seq[Primary]): Unit = {
    beginMove(slot, from, to)
    finishMove(slot, from, to, all)
  }

  /**
    * Waits until the move of `slot` to `owner` is settled, and stays settled for [[StableMs]].
    *
    * Every primary saying `owner` holds the slot is not enough. `SETSLOT … NODE` sets each node's slot table
    * directly, but the cluster bus still settles conflicting claims by config epoch: a ping the old owner sent before
    * it learned of the move claims the slot with the old owner's epoch, and a node that knows a lower epoch for
    * `owner` gives the slot back. That node then answers `owner`'s own claim with an `UPDATE`, and `owner` drops the
    * slot too. Each node then redirects writes to the other, until the writer runs out of redirects.
    *
    * So the move is settled only once `owner` has the highest config epoch of all the primaries, and every primary
    * sees `owner`, and only `owner`, holding the slot at that epoch. From then on, no other primary's claim can win.
    * Until then, each round bumps `owner`'s epoch if another primary's is as high, and assigns the slot again on each
    * node that sees another holder. The stable period lets messages already on the bus arrive.
    */
  private def awaitOwner(slot: Int, owner: Primary, all: Seq[Primary]): Unit = {
    val deadline = System.currentTimeMillis() + AgreeTimeoutMs
    def holders(view: Seq[Primary]): Seq[Primary] = view.filter(_.owns(slot))
    var stableSince = Option.empty[Long]
    while (stableSince.forall(since => System.currentTimeMillis() - since < StableMs)) {
      val views = all.map(node => node -> withNode(node)(primaries))
      val epochs = views.map { case (node, view) => node -> view.find(_.id == node.id).fold(-1L)(_.epoch) }
      val ownerEpoch = epochs.collectFirst { case (node, epoch) if node.id == owner.id => epoch }.getOrElse(-1L)
      val highest = epochs.forall { case (node, epoch) => node.id == owner.id || epoch < ownerEpoch }
      val wrongHolder = views.collect { case (node, view) if holders(view).map(_.id) != Seq(owner.id) => node }
      val agreed = views.forall { case (_, view) => holders(view).map(_.epoch) == Seq(ownerEpoch) }
      if (highest && wrongHolder.isEmpty && agreed) {
        if (stableSince.isEmpty) stableSince = Some(System.currentTimeMillis())
      } else {
        stableSince = None
        if (System.currentTimeMillis() > deadline) {
          val seen = views.map { case (node, view) =>
            s"${node.port} sees [${holders(view).map(p => s"${p.port} at epoch ${p.epoch}").mkString(", ")}]"
          }
          val own = epochs.map { case (node, epoch) => s"${node.port}: $epoch" }
          Assertions.fail(s"slot $slot did not settle on ${owner.port} within ${AgreeTimeoutMs}ms: " +
            s"${seen.mkString("; ")}; own config epochs ${own.mkString(", ")}")
        }
        if (!highest) withNode(owner)(bumpEpoch)
        assign(slot, owner, wrongHolder.sortBy(_.id != owner.id))
      }
      Thread.sleep(PollMs)
    }
  }
}
