package com.redislabs.provider.redis.util

import java.util.{List => JList}

import com.redislabs.provider.redis.{ReadWriteConfig, RedisEndpoint}
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import redis.clients.jedis.exceptions.{JedisAskDataException, JedisClusterOperationException, JedisDataException}
import redis.clients.jedis.exceptions.{JedisMovedDataException, JedisRedirectionException}
import redis.clients.jedis.{Jedis, Pipeline, Response}

import scala.collection.JavaConverters._
import scala.collection.{TraversableOnce, mutable}

object PipelineUtils {

  /**
    * Executes a pipeline function for each item in the sequence, returns the server response.
    *
    * Ensures that a new pipeline is created if the number of operations exceeds the given maxPipelineSize
    * while iterating over the items.
    *
    * @param conn            jedis connection
    * @param readWriteConfig read/write config
    * @param items           a sequence of elements (typically keys)
    * @param f               function to applied for each item in the sequence
    * @return response from the server
    */
  def mapWithPipeline[A](conn: Jedis, items: TraversableOnce[A])(f: (Pipeline, A) => Unit)
                        (implicit readWriteConfig: ReadWriteConfig): Seq[AnyRef] = {
    val totalResp = mutable.ListBuffer[JList[AnyRef]]()

    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % readWriteConfig.maxPipelineSize == 0) {
        val resp = pipeline.syncAndReturnAll()
        totalResp += resp
        pipeline = conn.pipelined()
      }
    }

    // sync remaining items
    if (i % readWriteConfig.maxPipelineSize != 0) {
      val resp = pipeline.syncAndReturnAll()
      totalResp += resp
    }

    totalResp.flatMap(_.asScala).toSeq
  }

  /**
    * Executes a pipeline function for each item in the sequence. No response is returned.
    *
    * Ensures that a new pipeline is created if the number of operations exceeds the given maxPipelineSize
    * while iterating over the items.
    *
    * @param conn            jedis connection
    * @param readWriteConfig read/write config
    * @param items           a sequence of elements (typically keys)
    * @param f               function to applied for each item in the sequence
    * @throws JedisDataException if Redis replies with an error to any of the commands, see [[syncAndCheck]]
    */
  def foreachWithPipeline[A](conn: Jedis, items: TraversableOnce[A])(f: (Pipeline, A) => Unit)
                            (implicit readWriteConfig: ReadWriteConfig): Unit = {
    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % readWriteConfig.maxPipelineSize == 0) {
        syncAndCheck(pipeline)
        pipeline = conn.pipelined()
      }
    }

    // sync remaining items
    if (i % readWriteConfig.maxPipelineSize != 0) {
      syncAndCheck(pipeline)
    }
  }

  /**
    * Executes a pipeline function for each item in the sequence. Doesn't sync and return the last pipeline after
    * all operations are executed. Allows to execute more operations with the returned pipeline.
    * The client is responsible of syncing the returned pipeline.
    *
    * Ensures that a new pipeline is created if the number of operations exceeds the given maxPipelineSize
    * while iterating over the items.
    *
    * @param conn            jedis connection
    * @param readWriteConfig read/write config
    * @param items           a sequence of elements (typically keys)
    * @param f               function to applied for each item in the sequence
    * @return the last pipeline
    * @throws JedisDataException if Redis replies with an error to any of the commands synced so far,
    *                            see [[syncAndCheck]]
    */
  def foreachWithPipelineNoLastSync[A](conn: Jedis, items: TraversableOnce[A])(f: (Pipeline, A) => Unit)
                                      (implicit readWriteConfig: ReadWriteConfig): Pipeline = {
    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % readWriteConfig.maxPipelineSize == 0) {
        syncAndCheck(pipeline)
        pipeline = conn.pipelined()
      }
    }

    // return pipeline, the client should sync pipeline with syncAndCheck
    pipeline
  }

  /** How many times one write follows a `MOVED` or `ASK` redirect before it fails. */
  val MaxRedirects = 5

  /** One command of a write: sends exactly one command on the pipeline and returns its response. */
  type WriteCommand = Pipeline => Response[_]

  /** Where to send a write: an endpoint, and whether to send `ASKING` first. */
  private case class WriteTarget(endpoint: RedisEndpoint, asking: Boolean)

  /**
    * Sends one command per item, pipelined to the endpoint paired with the item, and checks every reply. See
    * [[writeWithRedirects]], which this calls with one command per item.
    *
    * @param items           items, each paired with the endpoint that owns its key in the cached topology
    * @param write           sends exactly one command for the item and returns its response
    * @param readWriteConfig read/write config
    */
  def foreachWithRedirects[A](items: Seq[(RedisEndpoint, A)])(write: (Pipeline, A) => Response[_])(
      implicit readWriteConfig: ReadWriteConfig): Unit =
    writeWithRedirects(items)(item => List((pipeline: Pipeline) => write(pipeline, item)))

  /**
    * Sends each item's commands, in order, pipelined to the endpoint paired with the item, and checks every reply.
    * All of an item's commands must be for keys in one hash slot, so that they share an owner.
    *
    * An item with a command the server answers with `MOVED` (its slot has moved since the topology was read) is sent
    * again, pipelined, to the node the redirect names. An item answered with `ASK` (its slot is being migrated) is
    * sent there with `ASKING` before each of its commands. A redirected item is sent again whole, so its commands
    * should be safe to repeat; a slot that moves between two of one item's commands can repeat the first. Each item
    * follows at most [[MaxRedirects]] redirects. Any other error reply fails the write with an exception, so a
    * partial write never passes silently.
    *
    * Items are written in order, round by round: an item redirected in one round is sent after every item of that
    * round, so commands for one key that are split across items keep their order.
    *
    * @param items           items, each paired with the endpoint that owns its keys in the cached topology
    * @param commands        the commands for an item, in the order they must run
    * @param readWriteConfig read/write config
    */
  def writeWithRedirects[A](items: Seq[(RedisEndpoint, A)])(commands: A => Seq[WriteCommand])(
      implicit readWriteConfig: ReadWriteConfig): Unit =
    redirectRounds(items) { (target, group) =>
      send(target, group, commands).flatMap { case (item, responses) =>
        nextTarget(target, responses).map(next => (next, item))
      }
    }

  /**
    * Like [[writeWithRedirects]], but each item's commands run in one `MULTI`/`EXEC` transaction, so a reader sees all
    * of them applied or none. All of an item's commands must be for keys in one hash slot.
    *
    * Redis checks each command of a transaction when it is queued. A command it refuses there (`MOVED`, `ASK`,
    * `NOPERM`, ...) gets the error as its own reply, and `EXEC` then aborts the whole transaction with `EXECABORT`, so
    * none of the item is applied. An item refused with `MOVED` or `ASK` is sent again whole, as in
    * [[writeWithRedirects]]; for `ASK`, `ASKING` goes before `MULTI` and holds until `EXEC`. Any other error, when a
    * command is queued or inside the `EXEC` reply, fails the write.
    *
    * @param items           items, each paired with the endpoint that owns its keys in the cached topology
    * @param commands        the commands for an item, which run in one transaction, in this order
    * @param readWriteConfig read/write config
    */
  def writeTransactionsWithRedirects[A](items: Seq[(RedisEndpoint, A)])(commands: A => Seq[WriteCommand])(
      implicit readWriteConfig: ReadWriteConfig): Unit =
    redirectRounds(items) { (target, group) =>
      withConnection(target.endpoint.connect()) { conn =>
        // ASKING holds for the one transaction that follows it, so with ASKING each item goes on its own
        val batches = if (target.asking) group.map(Seq(_)) else group.grouped(readWriteConfig.maxPipelineSize).toList
        batches.flatMap { batch =>
          if (target.asking) conn.asking()
          val pipeline = conn.pipelined()
          val counts = batch.map { item =>
            pipeline.multi()
            val itemCommands = commands(item).toList
            itemCommands.foreach(command => command(pipeline))
            pipeline.exec()
            itemCommands.size
          }
          // Every reply in order: per item, MULTI's, one for each queued command, and EXEC's. The queued commands'
          // own Responses only fill in from the EXEC reply, so they can't show a refusal at queue time.
          var replies = pipeline.syncAndReturnAll().asScala.toList
          batch.zip(counts).flatMap { case (item, count) =>
            val (itemReplies, rest) = replies.splitAt(count + 2)
            replies = rest
            transactionNextTarget(target, itemReplies).map(next => (next, item))
          }
        }
      }
    }

  /**
    * Like [[writeWithRedirects]], for items whose one command writes several keys of one hash slot, such as `MSETEX`.
    *
    * Redis can't run such a command while the slot is being migrated. The old owner answers `ASK` if it holds none
    * of the keys, and `TRYAGAIN` if it holds some; the new owner answers `TRYAGAIN` for keys it doesn't hold yet. So an
    * item answered with `ASK` or `TRYAGAIN` is split with `split` into single-key items, which [[writeWithRedirects]]
    * then writes with `singleCommands`, following each one's `ASK`. An item answered with `MOVED` is sent whole to the
    * slot's new owner. Any other error fails the write.
    *
    * @param items           items, each paired with the endpoint that owns its keys in the cached topology
    * @param commands        the commands for an item, all for keys in its one hash slot
    * @param split           the single-key items an item breaks into
    * @param singleCommands  the commands for a single-key item
    * @param readWriteConfig read/write config
    */
  def writeMultiKeyWithRedirects[A, B](items: Seq[(RedisEndpoint, A)])(commands: A => Seq[WriteCommand])(
      split: A => Seq[B], singleCommands: B => Seq[WriteCommand])(implicit readWriteConfig: ReadWriteConfig): Unit = {
    val singles = mutable.ListBuffer[(RedisEndpoint, B)]()
    redirectRounds(items) { (target, group) =>
      send(target, group, commands).flatMap { case (item, responses) =>
        if (errorsOf(responses).exists(isMigrating)) {
          singles ++= split(item).map(single => (target.endpoint, single))
          None
        } else {
          nextTarget(target, responses).map(next => (next, item))
        }
      }
    }
    writeWithRedirects(singles.toList)(singleCommands)
  }

  /**
    * Sends `items` round by round, each to its target, until none is redirected. `write` sends one round's items for
    * one target and returns those the server redirected, each with its next target. Each item follows at most
    * [[MaxRedirects]] redirects.
    */
  private def redirectRounds[A](items: Seq[(RedisEndpoint, A)])(
      write: (WriteTarget, List[A]) => Seq[(WriteTarget, A)]): Unit = {
    // Strict collections throughout: a lazy one could send its writes after the connection is closed.
    var pending: List[(WriteTarget, A)] = items.toList.map { case (endpoint, item) =>
      (WriteTarget(endpoint, asking = false), item)
    }
    var redirects = 0
    while (pending.nonEmpty) {
      if (redirects > MaxRedirects) {
        throw new JedisClusterOperationException(
          s"${pending.size} writes were still redirected after $MaxRedirects redirects")
      }
      // groupBy keeps each group's items in their original order
      pending = pending.groupBy(_._1).toList.flatMap { case (target, group) => write(target, group.map(_._2)) }
      redirects += 1
    }
  }

  /** Sends each item's commands to `target`, and returns each item with its commands' responses. */
  private def send[A](target: WriteTarget, items: Seq[A], commands: A => Seq[WriteCommand])(
      implicit readWriteConfig: ReadWriteConfig): Seq[(A, List[Response[_]])] = {
    withConnection(target.endpoint.connect()) { conn =>
      val responses: Seq[(A, List[Response[_]])] = if (target.asking) {
        // ASKING applies only to the command that follows it, so each command goes on its own, after ASKING.
        items.map { item =>
          val itemResponses = commands(item).toList.map { command =>
            conn.asking()
            val pipeline = conn.pipelined()
            val response = command(pipeline)
            pipeline.sync()
            response
          }
          (item, itemResponses)
        }
      } else {
        items.grouped(readWriteConfig.maxPipelineSize).toList.flatMap { batch =>
          val pipeline = conn.pipelined()
          val batchResponses = batch.map(item => (item, commands(item).toList.map(command => command(pipeline))))
          pipeline.sync()
          batchResponses
        }
      }
      responses
    }
  }

  /**
    * Where an item goes next: nowhere if every reply succeeded, or the node a `MOVED` or `ASK` reply names. Any other
    * error reply fails the write.
    */
  private def nextTarget(target: WriteTarget, responses: List[Response[_]]): Option[WriteTarget] =
    redirectTarget(target, errorsOf(responses))

  /** The error replies among `responses`. */
  private def errorsOf(responses: List[Response[_]]): List[JedisDataException] =
    responses.flatMap { response =>
      try {
        response.get
        None
      } catch {
        case e: JedisDataException => Some(e)
      }
    }

  /** The node a `MOVED` or `ASK` among `errors` names, if any. Any other error fails the write. */
  private def redirectTarget(target: WriteTarget, errors: List[JedisDataException]): Option[WriteTarget] = {
    errors.find(e => !e.isInstanceOf[JedisRedirectionException]).foreach(e => failWrite(target, e))
    errors.collectFirst { case e: JedisMovedDataException =>
      WriteTarget(redirected(target.endpoint, e), asking = false)
    }
      .orElse(errors.collectFirst { case e: JedisAskDataException =>
        WriteTarget(redirected(target.endpoint, e), asking = true)
      })
  }

  /**
    * Where a transaction goes next, from its replies: MULTI's, one for each queued command, and EXEC's. A queued
    * command refused with `MOVED` or `ASK` redirects the whole transaction, which `EXEC` aborted. Any other error, or
    * an error inside the `EXEC` reply, fails the write.
    */
  private def transactionNextTarget(target: WriteTarget, replies: List[AnyRef]): Option[WriteTarget] = {
    val queued = replies.init.collect { case e: JedisDataException => e }
    redirectTarget(target, queued).orElse {
      replies.last match {
        case results: JList[_] =>
          results.asScala.collectFirst { case e: JedisDataException => e }.foreach(e => failWrite(target, e))
          None
        case e: JedisDataException => failWrite(target, e)
        case other => failWrite(target, new JedisDataException(s"EXEC replied $other"))
      }
    }
  }

  /** `ASK`, or `TRYAGAIN`: the key's slot is being migrated, so a command on several of its keys can't run. */
  private def isMigrating(e: JedisDataException): Boolean =
    e.isInstanceOf[JedisAskDataException] || String.valueOf(e.getMessage).startsWith("TRYAGAIN")

  private def failWrite(target: WriteTarget, e: JedisDataException): Nothing = {
    // Only host and port: the endpoint's toString would include its password.
    val node = s"${target.endpoint.host}:${target.endpoint.port}"
    throw new JedisDataException(s"Write to $node failed: ${e.getMessage}", e)
  }

  /** The endpoint a redirect names, with the same user, password, timeout and TLS settings as `from`. */
  private def redirected(from: RedisEndpoint, e: JedisRedirectionException): RedisEndpoint =
    from.copy(host = e.getTargetNode.getHost, port = e.getTargetNode.getPort)

  /**
    * Sends the pipelined commands, reads all their replies and fails if Redis replied with an error to any of them.
    *
    * `Pipeline.sync()` reads the replies but keeps error replies (e.g. OOM or WRONGTYPE) inside the discarded
    * `Response` objects, so a write that Redis rejected would go unnoticed. All the replies are read before
    * throwing, so the connection can still be used, or returned to the pool, afterwards.
    *
    * The pipeline is not a transaction: the commands that Redis accepted stay applied.
    *
    * @param pipeline pipeline to sync
    * @throws JedisDataException with the number of failed commands and the first Redis error message
    */
  def syncAndCheck(pipeline: Pipeline): Unit = {
    val replies = pipeline.syncAndReturnAll().asScala
    val errors = replies.collect { case e: JedisDataException => e }
    if (errors.nonEmpty) {
      throw new JedisDataException(
        s"Redis rejected ${errors.size} of ${replies.size} pipelined commands; first error: ${errors.head.getMessage}",
        errors.head)
    }
  }

}
