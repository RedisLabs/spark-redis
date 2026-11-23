package com.redislabs.provider.redis

import java.io.PrintStream
import java.nio.charset.StandardCharsets.UTF_8

import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.{JavaSerializedSchema, Logging}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DataType
import redis.clients.jedis.params.SetParams
import redis.clients.jedis.{Jedis, ScanParams}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Rewrites the schemas of DataFrame tables that earlier versions of spark-redis stored as a Java-serialized
  * `StructType` into Spark's schema JSON, the format spark-redis now writes and every Spark and Scala version reads.
  *
  * A Java-serialized schema loads only with the Spark minor version and the Scala version that wrote it, so a table
  * saved before the format changed stops loading after an upgrade. The migration reads those schemas with
  * [[JavaSerializedSchema]], which doesn't depend on any version, so it works whichever versions wrote them and
  * whichever this runs on. It changes only schema keys (`_spark:<table>:schema`), never the table's rows.
  *
  * Run it from a Spark job or notebook with [[migrate]], or once from the command line with the main method:
  * {{{
  * spark-submit --class com.redislabs.provider.redis.SchemaMigration \
  *   --conf spark.redis.host=... --conf spark.redis.port=... --conf spark.redis.password=... \
  *   <spark-redis jar> [--dry-run] [--tables <pattern>]
  * }}}
  */
object SchemaMigration extends Logging {

  /** What the migration did, or would do, with one schema key. */
  sealed trait Outcome

  object Outcome {

    /** The key now holds the schema as JSON. */
    case object Migrated extends Outcome

    /** A dry run: the key holds a Java-serialized schema that the migration would rewrite as JSON. */
    case object WouldMigrate extends Outcome

    /** The key doesn't hold a Java-serialized value, for example because it already holds JSON. It is left alone. */
    case object NotJavaSerialized extends Outcome

    /** The key changed or disappeared while it was migrated, so it was left alone. Run the migration again. */
    case object Changed extends Outcome

    /** The key holds a Java-serialized value that can't be migrated, for the given reason. It is left alone. */
    final case class Failed(reason: String) extends Outcome
  }

  /** @param key the schema key, `_spark:<table>:schema` */
  final case class Result(key: String, outcome: Outcome)

  private val JavaSerializationMagic = Array(0xAC.toByte, 0xED.toByte)

  /**
    * Rewrites every Java-serialized schema of the tables that match `tables` as JSON.
    *
    * Each key is rewritten in a `MULTI`/`EXEC` under `WATCH`, so a key that changes meanwhile is left alone and
    * reported as [[Outcome.Changed]]. The key's TTL, if it has one, is kept. A schema is rewritten only if its JSON
    * reads back as the same schema.
    *
    * @param redisConfig the deployment: one server, or every primary of a cluster
    * @param tables      a glob of the table names to migrate, as in `SCAN MATCH`; by default every table
    * @param dryRun      report what would be migrated, and change nothing
    * @return one result per schema key found
    */
  def migrate(redisConfig: RedisConfig, tables: String = "*", dryRun: Boolean = false): Seq[Result] = {
    val pattern = s"_spark:$tables:schema"
    redisConfig.hosts.map(_.endpoint).distinct.toSeq.flatMap { endpoint =>
      withConnection(endpoint.connect()) { conn =>
        scan(conn, pattern).map { key =>
          val result = Result(new String(key, UTF_8), migrateKey(conn, key, dryRun))
          if (result.outcome == Outcome.Migrated) logInfo(s"Migrated the schema in '${result.key}' to JSON")
          result
        }
      }
    }
  }

  /** The keys on the server behind `conn` that match `pattern`, each once. */
  private def scan(conn: Jedis, pattern: String): Seq[Array[Byte]] = {
    val params = new ScanParams().`match`(pattern).count(1000)
    val keys = mutable.LinkedHashMap[String, Array[Byte]]()
    var cursor = ScanParams.SCAN_POINTER_START_BINARY
    var done = false
    while (!done) {
      val reply = conn.scan(cursor, params)
      reply.getResult.asScala.foreach(key => keys.getOrElseUpdate(new String(key, UTF_8), key))
      cursor = reply.getCursorAsBytes
      done = reply.isCompleteIteration
    }
    keys.values.toList
  }

  private def migrateKey(conn: Jedis, key: Array[Byte], dryRun: Boolean): Outcome = {
    var watching = false
    try {
      conn.watch(key)
      watching = true
      val value = conn.get(key)
      if (value == null) {
        Outcome.Changed
      } else if (!value.startsWith(JavaSerializationMagic)) {
        Outcome.NotJavaSerialized
      } else {
        toJson(value) match {
          case Left(reason) => Outcome.Failed(reason)
          case Right(_) if dryRun => Outcome.WouldMigrate
          case Right(json) =>
            val transaction = conn.multi()
            transaction.set(key, json, SetParams.setParams().keepttl())
            watching = false // EXEC ends the WATCH
            val replies = transaction.exec()
            if (replies == null || replies.isEmpty) Outcome.Changed else Outcome.Migrated
        }
      }
    } catch {
      case NonFatal(e) => Outcome.Failed(s"${e.getClass.getSimpleName}: ${e.getMessage}")
    } finally {
      if (watching) conn.unwatch()
    }
  }

  /** The schema's JSON, if the value is a Java-serialized schema whose JSON reads back as the same schema. */
  private def toJson(value: Array[Byte]): Either[String, Array[Byte]] = {
    val schema = try JavaSerializedSchema.read(value) catch {
      case e: IllegalArgumentException => return Left(e.getMessage)
    }
    val json = schema.json
    val readBack = try DataType.fromJson(json) catch {
      case NonFatal(e) => return Left(s"this Spark version can't read its JSON back: ${e.getMessage}")
    }
    if (readBack == schema) Right(json.getBytes(UTF_8)) else Left("its JSON doesn't read back as the same schema")
  }

  private val Usage =
    """Usage: spark-submit --class com.redislabs.provider.redis.SchemaMigration [--conf spark.redis.*=...]
      |         <spark-redis jar> [--dry-run] [--tables <pattern>]
      |
      |Rewrites the Java-serialized schemas of spark-redis DataFrame tables as JSON. Connects with the
      |spark.redis.* settings, as the connector does.
      |
      |  --dry-run           report what would be migrated, and change nothing
      |  --tables <pattern>  only the tables whose names match this glob (default: every table)
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    val code = run(args.toSeq, new SparkConf(), System.out)
    if (code != 0) sys.exit(code)
  }

  /**
    * The command line: parses `args`, migrates with the `spark.redis.*` settings of `conf`, and prints one line per
    * schema key and a summary to `out`.
    *
    * @return 0 if nothing failed, 1 if a key failed or changed meanwhile, 2 for bad arguments
    */
  private[redis] def run(args: Seq[String], conf: SparkConf, out: PrintStream): Int = {
    parse(args.toList, tables = "*", dryRun = false) match {
      case None =>
        out.print(Usage)
        2
      case Some((tables, dryRun)) =>
        val results = migrate(RedisConfig.fromSparkConf(conf), tables, dryRun)
        results.foreach { case Result(key, outcome) => out.println(describe(outcome) + "\t" + key) }
        val counts = results.groupBy(r => label(r.outcome)).map { case (l, rs) => s"$l: ${rs.size}" }
        out.println(s"${results.size} schema keys" + (if (counts.nonEmpty) counts.toSeq.sorted.mkString(" (", ", ", ")") else ""))
        if (results.exists(r => r.outcome.isInstanceOf[Outcome.Failed] || r.outcome == Outcome.Changed)) 1 else 0
    }
  }

  private def parse(args: List[String], tables: String, dryRun: Boolean): Option[(String, Boolean)] = args match {
    case Nil => Some((tables, dryRun))
    case "--dry-run" :: rest => parse(rest, tables, dryRun = true)
    case "--tables" :: pattern :: rest => parse(rest, pattern, dryRun)
    case _ => None
  }

  private def label(outcome: Outcome): String = outcome match {
    case Outcome.Migrated => "migrated"
    case Outcome.WouldMigrate => "would migrate"
    case Outcome.NotJavaSerialized => "not Java-serialized"
    case Outcome.Changed => "changed meanwhile"
    case _: Outcome.Failed => "failed"
  }

  private def describe(outcome: Outcome): String = outcome match {
    case Outcome.Failed(reason) => s"failed ($reason)"
    case other => label(other)
  }
}
