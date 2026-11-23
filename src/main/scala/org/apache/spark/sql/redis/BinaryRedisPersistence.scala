package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets.UTF_8

import com.redislabs.provider.redis.ReadWriteConfig
import com.redislabs.provider.redis.util.PipelineUtils.{WriteCommand, mapWithPipeline}
import com.redislabs.provider.redis.util.{RedisCapabilities, WriteCapabilities}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.params.SetParams
import redis.clients.jedis.util.{JedisClusterCRC16, SafeEncoder}
import redis.clients.jedis.{Jedis, Pipeline}

/**
  * @author The Viet Nguyen
  */
class BinaryRedisPersistence extends RedisPersistence[Array[Byte]] {

  import BinaryRedisPersistence._

  override def save(pipeline: Pipeline, key: String, value: Array[Byte], ttl: Int): Unit =
    saveCommands(key, value, ttl).foreach(command => command(pipeline))

  override def saveCommands(key: String, value: Array[Byte], ttl: Int): List[WriteCommand] = {
    val keyBytes = key.getBytes(UTF_8)
    if (ttl > 0) {
      List((pipeline: Pipeline) => pipeline.set(keyBytes, value, SetParams.setParams().ex(ttl.toLong)))
    } else {
      List((pipeline: Pipeline) => pipeline.set(keyBytes, value))
    }
  }

  /**
    * Writes one batch of rows that all live on the node `conn` points at, each with the same TTL, and fails if
    * Redis rejects any write. With `MSETEX` the batch is one command, or one per hash slot when `slotScoped`.
    * Without it, each row is one `SET … EX`. Both store the same values with the same TTL.
    *
    * @param rows         Redis key and encoded row
    * @param ttl          seconds, more than 0
    * @param capabilities what the server can do, probed once per write
    */
  def saveWithTtl(conn: Jedis, rows: Seq[(String, Array[Byte])], ttl: Int, capabilities: WriteCapabilities)
                 (implicit readWriteConfig: ReadWriteConfig): Unit = {
    require(ttl > 0, s"ttl must be more than 0, got $ttl")
    if (capabilities.msetex) {
      val groups = if (capabilities.slotScoped) {
        rows.groupBy { case (key, _) => JedisClusterCRC16.getSlot(key) }.values.toSeq
      } else {
        Seq(rows)
      }
      val replies = mapWithPipeline(conn, groups.filter(_.nonEmpty)) { (pipeline, group) =>
        msetex(pipeline, group, ttl)
      }
      failOnRejected(conn, RedisCapabilities.Msetex, replies, MsetexOk)
    } else {
      val replies = mapWithPipeline(conn, rows) { case (pipeline, (key, value)) =>
        save(pipeline, key, value, ttl)
      }
      failOnRejected(conn, "SET", replies, SetOk)
    }
  }

  override def load(pipeline: Pipeline, key: String, requiredColumns: Seq[String]): Unit =
    pipeline.get(key.getBytes(UTF_8))

  override def encodeRow(keyName: String, value: Row): Array[Byte] = {
    val fields = value.schema.fields.map(_.name)
    val valuesArray = fields.map(f => value.getAs[Any](f))
    SerializationUtils.serialize(valuesArray)
  }

  override def decodeRow(keyMap: (String, String), value: Array[Byte], schema: StructType,
                         requiredColumns: Seq[String]): Row = {
    val valuesArray: Array[Any] = SerializationUtils.deserialize(value)
    new GenericRowWithSchema(valuesArray, schema)
  }
}

object BinaryRedisPersistence {

  private object MsetexCommand extends ProtocolCommand {
    override def getRaw: Array[Byte] = SafeEncoder.encode(RedisCapabilities.Msetex)
  }

  private val Ex = SafeEncoder.encode("EX")
  private val MsetexOk: AnyRef = Long.box(1L)
  private val SetOk: AnyRef = "OK"

  /** `MSETEX numkeys key value [key value ...] EX ttl`. On a cluster the keys must share a hash slot. */
  private def msetex(pipeline: Pipeline, rows: Seq[(String, Array[Byte])], ttl: Int): Unit = {
    val args = Array.newBuilder[Array[Byte]]
    args.sizeHint(2 * rows.size + 3)
    args += SafeEncoder.encode(rows.size.toString)
    rows.foreach { case (key, value) =>
      args += key.getBytes(UTF_8)
      args += value
    }
    args += Ex
    args += SafeEncoder.encode(ttl.toString)
    pipeline.sendCommand(MsetexCommand, args.result(): _*)
  }

  /** A pipelined error reply never surfaces by itself, so a write Redis rejected must fail here. */
  private def failOnRejected(conn: Jedis, command: String, replies: Seq[AnyRef], ok: AnyRef): Unit = {
    val rejected = replies.filterNot(_ == ok)
    if (rejected.nonEmpty) {
      val (detail, cause) = rejected.head match {
        case e: JedisDataException => (e.getMessage, e)
        case other: Any => (s"unexpected reply $other", null)
      }
      val node = s"${conn.getClient.getHost}:${conn.getClient.getPort}"
      throw new JedisDataException(
        s"$command with TTL: Redis rejected ${rejected.size} of ${replies.size} commands on $node: $detail", cause)
    }
  }
}
