package org.apache.spark.sql.redis

import java.nio.charset.StandardCharsets.UTF_8

import com.redislabs.provider.redis.{ReadWriteConfig, RedisEndpoint}
import com.redislabs.provider.redis.util.PipelineUtils.{WriteCommand, writeMultiKeyWithRedirects, writeWithRedirects}
import com.redislabs.provider.redis.util.{RedisCapabilities, WriteCapabilities}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.params.SetParams
import redis.clients.jedis.util.{JedisClusterCRC16, SafeEncoder}
import redis.clients.jedis.Pipeline

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
    * Writes rows that all get the same TTL, following `MOVED` and `ASK`, and fails if Redis rejects any write.
    *
    * With `MSETEX`, the rows of one node go in one command, or one per hash slot when `slotScoped`. A slot that is
    * being migrated gets one `SET … EX` per row instead, because Redis can't run a command on several of its keys
    * then. Without `MSETEX`, each row is one `SET … EX`. Both store the same values with the same TTL.
    *
    * @param rows         each row's Redis key and encoded row, paired with the endpoint that owns the key in the
    *                     cached topology
    * @param ttl          seconds, more than 0
    * @param capabilities what the server can do, probed once per write
    */
  def saveWithTtl(rows: Seq[(RedisEndpoint, (String, Array[Byte]))], ttl: Int, capabilities: WriteCapabilities)
                 (implicit readWriteConfig: ReadWriteConfig): Unit = {
    require(ttl > 0, s"ttl must be more than 0, got $ttl")
    val setWithTtl: ((String, Array[Byte])) => Seq[WriteCommand] = { case (key, value) =>
      saveCommands(key, value, ttl)
    }
    if (capabilities.msetex) {
      val groups = rows.groupBy { case (endpoint, (key, _)) =>
        (endpoint, if (capabilities.slotScoped) JedisClusterCRC16.getSlot(key) else 0)
      }.toList.map { case ((endpoint, _), group) => endpoint -> group.map(_._2) }
      writeMultiKeyWithRedirects(groups)(group => List(msetex(group, ttl)))(group => group, setWithTtl)
    } else {
      writeWithRedirects(rows)(setWithTtl)
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

  /** `MSETEX numkeys key value [key value ...] EX ttl`. On a cluster the keys must share a hash slot. */
  private def msetex(rows: Seq[(String, Array[Byte])], ttl: Int): WriteCommand = {
    val args = Array.newBuilder[Array[Byte]]
    args.sizeHint(2 * rows.size + 3)
    args += SafeEncoder.encode(rows.size.toString)
    rows.foreach { case (key, value) =>
      args += key.getBytes(UTF_8)
      args += value
    }
    args += Ex
    args += SafeEncoder.encode(ttl.toString)
    val command = args.result()
    (pipeline: Pipeline) => pipeline.sendCommand(MsetexCommand, command: _*)
  }
}
