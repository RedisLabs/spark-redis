package com.redislabs.provider.redis.util

import scala.collection.JavaConverters._

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import com.redislabs.provider.redis.env.Env
import com.redislabs.provider.redis.util.ClusterSlots.{Primary, withNode}
import org.scalatest.Assertions
import redis.clients.jedis.Jedis
import redis.clients.jedis.util.JedisClusterCRC16

/**
  * Runs a write to a slot whose owner changes after the connector read the cluster topology (the #347 family: writes
  * lost without an error). The cluster answers the old owner with `MOVED` once the slot has moved, or with `ASK`
  * while it is being migrated, and every element must still be stored.
  *
  * Each method finds the primary that owns `hashTag`'s slot and another primary to move it to, runs the write, then
  * calls `check` with a connection to the new owner, once every key in the slot is there. In a `finally` block it
  * moves the slot back and deletes the slot's keys whose names contain `hashTag`, so the tests that run next in this
  * JVM see the original layout. Give each suite its own hash tag, so that suites don't share a slot.
  */
trait RedirectedWrites extends Assertions { this: Env =>

  private val CleanupBatch = 10000

  private case class Move(slot: Int, from: Primary, to: Primary, all: Seq[Primary])

  /**
    * Reads the topology into a `RedisConfig`, runs `beforeMove` while that topology is current, moves `hashTag`'s
    * slot to another primary, then calls `write` with the config. The old owner answers each write with `MOVED`.
    */
  def afterSlotMoved(hashTag: String, beforeMove: () => Unit = () => ())(write: RedisConfig => Unit)(
      check: Jedis => Unit): Unit = {
    val move = plan(hashTag)
    val config = topology(hashTag, move)
    try {
      beforeMove()
      ClusterSlots.moveSlot(move.slot, move.from, move.to, move.all)
      write(config)
      withNode(move.to)(check)
    } finally restore(hashTag, move)
  }

  /**
    * Reads the topology into a `RedisConfig`, starts migrating `hashTag`'s slot to another primary, then calls
    * `write` with the config. The old owner still owns the slot, and answers `ASK` for each key it doesn't hold. The
    * migration is finished before `check`.
    */
  def whileSlotMigrating(hashTag: String)(write: RedisConfig => Unit)(check: Jedis => Unit): Unit = {
    val move = plan(hashTag)
    val config = topology(hashTag, move)
    try {
      ClusterSlots.beginMove(move.slot, move.from, move.to)
      write(config)
      ClusterSlots.finishMove(move.slot, move.from, move.to, move.all)
      withNode(move.to)(check)
    } finally restore(hashTag, move)
  }

  private def plan(hashTag: String): Move = {
    val slot = JedisClusterCRC16.getSlot(hashTag)
    val all = ClusterSlots.primaries(redisHost, redisPort)
    val from = all.find(_.owns(slot)).getOrElse(fail(s"no primary owns slot $slot"))
    val to = all.find(_.id != from.id).getOrElse(fail("the cluster has only one primary"))
    Move(slot, from, to, all)
  }

  /** A `RedisConfig` that reads the topology now, while the slot is still on `move.from`. */
  private def topology(hashTag: String, move: Move): RedisConfig = {
    val config = new RedisConfig(RedisEndpoint(redisHost, redisPort))
    assert(config.getHost(hashTag).endpoint.port == move.from.port,
      s"setup: the RedisConfig puts slot ${move.slot} on ${move.from.port}")
    config
  }

  private def restore(hashTag: String, move: Move): Unit = {
    ClusterSlots.moveSlot(move.slot, move.from, move.to, move.all) // finishes the move if it stopped part way
    ClusterSlots.moveSlot(move.slot, move.to, move.from, move.all)
    withNode(move.from) { conn =>
      val keys = conn.clusterGetKeysInSlot(move.slot, CleanupBatch).asScala.filter(_.contains(hashTag))
      if (keys.nonEmpty) conn.del(keys.toSeq: _*)
    }
  }
}
