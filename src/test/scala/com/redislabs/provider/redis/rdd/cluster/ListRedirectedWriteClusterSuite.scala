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
  * The list writers (`toRedisLIST`, `toRedisLISTs`, `toRedisByteLISTs`, `toRedisFixedLIST`) write to a slot that
  * moved to another primary after the `RedisConfig` read the topology (`MOVED`), or that is being migrated (`ASK`).
  * Every element must still be stored, as [[MovedWriteClusterSuite]] and [[AskWriteClusterSuite]] require of
  * `toRedisKV` (#347). The partitions run in parallel, so the checks ignore the order of the elements.
  */
class ListRedirectedWriteClusterSuite extends SparkRedisSuite with RedisClusterEnv with RedirectedWrites
    with Matchers {

  private val HashTag = "{redirected-list}"
  private val ElementCount = 100
  private val ListCount = 20
  private val ElementsPerList = 5

  private def run(): String = UUID.randomUUID().toString.replace("-", "")

  private def rwConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)

  /** One list of [[ElementCount]] elements, for `toRedisLIST` and `toRedisFixedLIST`. */
  private def oneList(): (String, Seq[String]) =
    (s"$HashTag:${run()}:list", (1 to ElementCount).map(i => s"element-$i"))

  /** [[ListCount]] lists of [[ElementsPerList]] elements each, for `toRedisLISTs` and `toRedisByteLISTs`. */
  private def manyLists(): Seq[(String, Seq[String])] = {
    val id = run()
    (1 to ListCount).map(l => (s"$HashTag:$id:list:$l", (1 to ElementsPerList).map(e => s"element-$l-$e")))
  }

  private def toBytes(lists: Seq[(String, Seq[String])]): Seq[(Array[Byte], Seq[Array[Byte]])] =
    lists.map { case (key, elements) => (key.getBytes(UTF_8), elements.map(_.getBytes(UTF_8))) }

  /** Checks that every list holds exactly its elements, in any order, and names how many elements are missing. */
  private def checkStored(conn: Jedis, expected: Seq[(String, Seq[String])], when: String): Unit = {
    val stored = expected.map { case (key, _) => key -> conn.lrange(key, 0, -1).asScala.sorted }.toMap
    val total = expected.map(_._2.size).sum
    val missing = expected.map { case (key, elements) => elements.count(e => !stored(key).contains(e)) }.sum
    withClue(s"$missing of $total list elements not stored $when:") {
      stored shouldBe expected.map { case (key, elements) => key -> elements.sorted }.toMap
    }
  }

  private val Moved = "after their slot moved"
  private val Migrating = "while their slot was migrating"

  test("toRedisLIST stores every element after its slot moved to another primary") {
    val (key, elements) = oneList()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisLIST(sc.parallelize(elements, 4), key)(config, rwConfig)
    } { conn =>
      checkStored(conn, Seq(key -> elements), Moved)
    }
  }

  test("toRedisLIST stores every element while its slot is being migrated to another primary") {
    val (key, elements) = oneList()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisLIST(sc.parallelize(elements, 4), key)(config, rwConfig)
    } { conn =>
      checkStored(conn, Seq(key -> elements), Migrating)
    }
  }

  test("toRedisLISTs stores every element after its slot moved to another primary") {
    val lists = manyLists()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisLISTs(sc.parallelize(lists, 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, lists, Moved)
    }
  }

  test("toRedisLISTs stores every element while its slot is being migrated to another primary") {
    val lists = manyLists()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisLISTs(sc.parallelize(lists, 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, lists, Migrating)
    }
  }

  test("toRedisByteLISTs stores every element after its slot moved to another primary") {
    val lists = manyLists()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisByteLISTs(sc.parallelize(toBytes(lists), 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, lists, Moved)
    }
  }

  test("toRedisByteLISTs stores every element while its slot is being migrated to another primary") {
    val lists = manyLists()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisByteLISTs(sc.parallelize(toBytes(lists), 4))(config, rwConfig)
    } { conn =>
      checkStored(conn, lists, Migrating)
    }
  }

  test("toRedisFixedLIST stores every element after its slot moved to another primary") {
    val (key, elements) = oneList()
    afterSlotMoved(HashTag) { config =>
      sc.toRedisFixedLIST(sc.parallelize(elements, 4), key, ElementCount)(config, rwConfig)
    } { conn =>
      checkStored(conn, Seq(key -> elements), Moved)
    }
  }

  test("toRedisFixedLIST stores every element while its slot is being migrated to another primary") {
    val (key, elements) = oneList()
    whileSlotMigrating(HashTag) { config =>
      sc.toRedisFixedLIST(sc.parallelize(elements, 4), key, ElementCount)(config, rwConfig)
    } { conn =>
      checkStored(conn, Seq(key -> elements), Migrating)
    }
  }
}
