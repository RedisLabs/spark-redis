package com.redislabs.provider.redis.rdd.cluster

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.collection.JavaConverters._

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.env.RedisClusterEnv
import com.redislabs.provider.redis.util.RedirectedWrites
import org.scalatest.Matchers
import redis.clients.jedis.Jedis

/**
  * The hash writers (`toRedisHASH`, `toRedisHASHes`, `toRedisByteHASHes`) write to a slot that moved to another
  * primary after the `RedisConfig` read the topology (`MOVED`), or that is being migrated (`ASK`). Every field must
  * still be stored, as [[MovedWriteClusterSuite]] and [[AskWriteClusterSuite]] require of `toRedisKV` (#347).
  */
class HashRedirectedWriteClusterSuite extends SparkRedisSuite with RedisClusterEnv with RedirectedWrites
    with Matchers {

  private val HashTag = "{redirected-hash}"
  private val FieldCount = 100
  private val HashCount = 20
  private val FieldsPerHash = 5

  private def run(): String = UUID.randomUUID().toString.replace("-", "")

  private def rwConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)

  /** One hash with [[FieldCount]] fields, for `toRedisHASH`. */
  private def oneHash(): (String, Seq[(String, String)]) =
    (s"$HashTag:${run()}:hash", (1 to FieldCount).map(i => (s"field-$i", s"value-$i")))

  /** [[HashCount]] hashes of [[FieldsPerHash]] fields each, for `toRedisHASHes` and `toRedisByteHASHes`. */
  private def manyHashes(): Seq[(String, Map[String, String])] = {
    val id = run()
    (1 to HashCount).map { h =>
      (s"$HashTag:$id:hash:$h", (1 to FieldsPerHash).map(f => (s"field-$f", s"value-$h-$f")).toMap)
    }
  }

  private def toBytes(hashes: Seq[(String, Map[String, String])]): Seq[(Array[Byte], Map[Array[Byte], Array[Byte]])] =
    hashes.map { case (key, fields) =>
      (key.getBytes(UTF_8), fields.map { case (f, v) => (f.getBytes(UTF_8), v.getBytes(UTF_8)) })
    }

  /** Checks that every hash holds exactly its fields, and names how many fields are missing. */
  private def checkStored(conn: Jedis, expected: Seq[(String, Map[String, String])], when: String): Unit = {
    val stored = expected.map { case (key, _) => key -> conn.hgetAll(key).asScala.toMap }.toMap
    val total = expected.map(_._2.size).sum
    val missing = expected.map { case (key, fields) =>
      fields.count { case (field, value) => !stored(key).get(field).contains(value) }
    }.sum
    withClue(s"$missing of $total hash fields not stored $when:") {
      stored shouldBe expected.toMap
    }
  }

  private val Moved = "after their slot moved"
  private val Migrating = "while their slot was migrating"

  test("toRedisHASH stores every field after its slot moved to another primary") {
    val (key, fields) = oneHash()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisHASH(sc.parallelize(fields, 4), key)(config, rwConfig)
    } { conn =>
      checkStored(conn, Seq(key -> fields.toMap), Moved)
    }
  }

  test("toRedisHASH stores every field while its slot is being migrated to another primary") {
    val (key, fields) = oneHash()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisHASH(sc.parallelize(fields, 4), key)(config, rwConfig)
    } { conn =>
      checkStored(conn, Seq(key -> fields.toMap), Migrating)
    }
  }

  test("toRedisHASHes stores every field after its slot moved to another primary") {
    val hashes = manyHashes()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisHASHes(sc.parallelize(hashes, 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, hashes, Moved)
    }
  }

  test("toRedisHASHes stores every field while its slot is being migrated to another primary") {
    val hashes = manyHashes()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisHASHes(sc.parallelize(hashes, 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, hashes, Migrating)
    }
  }

  test("toRedisByteHASHes stores every field after its slot moved to another primary") {
    val hashes = manyHashes()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisByteHASHes(sc.parallelize(toBytes(hashes), 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, hashes, Moved)
    }
  }

  test("toRedisByteHASHes stores every field while its slot is being migrated to another primary") {
    val hashes = manyHashes()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisByteHASHes(sc.parallelize(toBytes(hashes), 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, hashes, Migrating)
    }
  }
}
