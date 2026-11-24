package org.apache.spark.sql.redis

import java.io.{ByteArrayOutputStream, DataOutputStream, ObjectInputStream, ObjectStreamClass, ObjectStreamConstants,
  Serializable => JSerializable}
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.commons.lang3.{SerializationException, SerializationUtils}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, ObjectType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.immutable.Queue

/**
  * A binary-model row, and a schema stored by an earlier version, are read with Java deserialization that only
  * allows the classes spark-redis writes itself, so a value someone else stored in Redis can't run code in Spark.
  */
class RestrictedDeserializationTest extends FunSuite with Matchers with BeforeAndAfterAll {

  import RestrictedDeserializationTest._

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().master("local[1]").appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false").getOrCreate()
  }

  override def afterAll(): Unit = {
    try spark.stop() finally super.afterAll()
  }

  private val persistence = new BinaryRedisPersistence()

  /** The rows Spark hands the binary model for `sql`, written and read back as the binary model does. */
  private def roundTrip(sql: String): Unit =
    spark.sql(sql).collect().foreach { row =>
      val bytes = persistence.encodeRow("_id", row)
      persistence.decodeRow("_id" -> "key", bytes, row.schema, row.schema.fieldNames) shouldBe row
    }

  test("atomic columns, nulls included, read back") {
    roundTrip("SELECT true b, CAST(1 AS TINYINT) t, CAST(2 AS SMALLINT) s, 3 i, 4L l, CAST(5.5 AS FLOAT) f, " +
      "6.5D d, CAST(7.25 AS DECIMAL(10,2)) dec, CAST(1 AS DECIMAL(38,18)) bigdec, 'x' str, " +
      "CAST('ab' AS BINARY) bin, DATE'2024-01-02' dt, TIMESTAMP'2024-01-02 03:04:05.123' ts, CAST(NULL AS STRING) n")
  }

  test("interval columns read back") {
    roundTrip("SELECT INTERVAL '1 02:03:04' DAY TO SECOND dts, INTERVAL '1-2' YEAR TO MONTH ym, " +
      "make_interval(1, 2, 3, 4, 5, 6, 7.5) ci")
  }

  test("TIMESTAMP_NTZ columns read back") {
    assume(sparkAtLeast(3, 4), "TIMESTAMP_NTZ is a Spark 3.4 type")
    roundTrip("SELECT CAST(TIMESTAMP'2024-01-02 03:04:05' AS TIMESTAMP_NTZ) ntz")
  }

  test("dates and timestamps read back as java.time values too") {
    spark.conf.set("spark.sql.datetime.java8API.enabled", "true")
    try {
      roundTrip("SELECT DATE'2024-01-02' dt, TIMESTAMP'2024-01-02 03:04:05.123' ts, array(DATE'2024-01-02') ad")
    } finally {
      spark.conf.set("spark.sql.datetime.java8API.enabled", "false")
    }
  }

  test("array, map and struct columns read back, nested and empty ones included") {
    roundTrip("SELECT array(1, 2) ai, array('a', 'b') astr, array() empty, array(array(1L), array(2L)) nested, " +
      "array(named_struct('a', 1)) structs, array(1.5D) ad, array(true) ab")
    // up to four entries Scala has a class per map size, above that a hash map
    roundTrip("SELECT map('a', 1) m1, map('a', 1, 'b', 2) m2, map('a', 1, 'b', 2, 'c', 3) m3, " +
      "map('a', 1, 'b', 2, 'c', 3, 'd', 4) m4, map('a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5, 'f', 6) m6, " +
      "map() mempty, map(1, array('x')) marr, map('k', named_struct('a', 1)) mstruct")
    roundTrip("SELECT named_struct('a', 1, 'b', named_struct('c', 'x', 'd', array(1), 'e', map('k', 2L))) s")
  }

  test("large binary and array values read back: every element takes at least a byte of the value") {
    roundTrip("SELECT CAST(repeat('a', 100000) AS BINARY) bin, sequence(1, 20000) ints, " +
      "transform(sequence(1, 2000), i -> CAST(NULL AS STRING)) nulls, array_repeat(CAST(1 AS BOOLEAN), 5000) bools")
  }

  test("a class that isn't allowed fails before any of its code runs") {
    Tripwire.tripped.set(false)
    val bytes = SerializationUtils.serialize(Array[Any]("John", new Tripwire))
    val e = the[ClassNotAllowedException] thrownBy decode(bytes)
    e.getMessage should include(classOf[Tripwire].getName)
    e.getMessage should include(SqlOptionDeserializationAllowedClasses)
    Tripwire.tripped.get shouldBe false
  }

  test("classes that gadget chains are built from aren't allowed, even nested in an allowed value") {
    def nested(value: AnyRef): Array[Byte] = SerializationUtils.serialize(Array[Any]("John", Array[AnyRef](value)))
    Seq(new java.util.HashMap[String, String](), new java.util.PriorityQueue[String](),
      new java.net.URL("http://example.com/")).foreach { value =>
      val e = the[ClassNotAllowedException] thrownBy decode(nested(value))
      e.getMessage should include(value.getClass.getName)
    }
    // a Scala collection the binary model doesn't write: on Scala 2.13, its serialization proxy's factory (the Queue
    // companion object) is rejected
    val e = the[ClassNotAllowedException] thrownBy decode(nested(Queue(1, 2)))
    e.getMessage should include("scala.collection.immutable.Queue")
  }

  test("the default allowlist has none of the classes known gadget chains use") {
    Seq(
      "scala.collection.immutable.LazyList", // CVE-2022-36944
      "scala.collection.immutable.LazyList$SerializationProxy",
      "scala.collection.immutable.Stream$Cons",
      "java.lang.invoke.SerializedLambda",
      "java.util.HashMap", "java.util.HashSet", "java.util.PriorityQueue", "java.net.URL",
      "javax.management.BadAttributeValueExpException",
      // sorted collections compare their elements while they're deserialized, and this ordering calls any Comparator
      "java.util.TreeMap", "java.util.TreeSet", "scala.collection.immutable.TreeMap",
      "scala.collection.immutable.TreeSet", "scala.math.LowPriorityOrderingImplicits$$anon$3",
      "org.apache.commons.collections.functors.InvokerTransformer",
      "org.apache.commons.collections4.functors.InvokerTransformer",
      "org.apache.spark.sql.catalyst.expressions.Expression"
    ).foreach { name =>
      withClue(name) {
        ClassAllowlist.Default.allows(name) shouldBe false
      }
    }
  }

  test("a dynamic proxy is never allowed") {
    val proxy = Proxy.newProxyInstance(getClass.getClassLoader, Array[Class[_]](classOf[Comparable[_]]),
      new SerializableHandler)
    val e = the[ClassNotAllowedException] thrownBy decode(SerializationUtils.serialize(Array[Any](proxy)))
    e.getMessage should include("dynamic proxy")
    e.getMessage should include(classOf[Comparable[_]].getName)
  }

  test("the option allows more classes, for example a user-defined type's") {
    val schema = StructType(Seq(StructField("name", StringType), StructField("udt", ObjectType(classOf[Tripwire]))))
    val row = new GenericRowWithSchema(Array[Any]("John", new Tripwire), schema)
    val allowing = new BinaryRedisPersistence(ClassAllowlist.Default ++ ClassAllowlist.parse(classOf[Tripwire].getName))
    val decoded = allowing.decodeRow("_id" -> "key", allowing.encodeRow("_id", row), schema, schema.fieldNames)
    decoded.get(1) shouldBe a[Tripwire]
  }

  test("a short value that declares a huge array fails without allocating it") {
    assume(RestrictedDeserialization.limitsAvailable, "this JVM has no serialization filter API")
    val bytes = hugeArrayRow
    val e = the[ValueRejectedException] thrownBy decode(bytes)
    e shouldBe a[SerializationException]
    e.getMessage should include(s"${bytes.length} bytes")
  }

  test("a long array fails if the value can't hold 8 bytes for each of its elements") {
    assume(RestrictedDeserialization.limitsAvailable, "this JVM has no serialization filter API")
    // as many elements as the value has bytes: they fit as bytes, not as longs
    val bytes = rowDeclaringLongArray(SerializationUtils.serialize(Array[Any]("John", Array.emptyLongArray)).length)
    val e = the[ValueRejectedException] thrownBy decode(bytes)
    // the row's two-element array takes at least 2 more
    e.getMessage should include(s"take at least ${8L * bytes.length + 2} bytes to write, more than its ${bytes.length}")
  }

  test("long, double and boolean arrays, which take exactly their bytes, still read back") {
    val metadata = new MetadataBuilder().putLongArray("l", Array.tabulate(10000)(_.toLong))
      .putDoubleArray("d", Array.fill(10000)(0.5)).putBooleanArray("b", Array.fill(10000)(true)).build()
    val schema = StructType(Seq(StructField("f", StringType, nullable = true, metadata)))
    RedisSourceRelation.decodeSchema("_spark:arrays:schema", SerializationUtils.serialize(schema)) shouldBe schema
  }

  test("an array that declares a negative length fails as a rejected value") {
    Seq(-1, -5).foreach { length =>
      withClue(length) {
        val e = the[ValueRejectedException] thrownBy decode(rowDeclaringLongArray(length))
        e.getMessage should include("negative length")
      }
    }
  }

  test("arrays that each fit in the value, but not all together, fail before the one that goes over is allocated") {
    assume(RestrictedDeserialization.limitsAvailable, "this JVM has no serialization filter API")
    val bytes = nestedArrays(depth = 3)
    val e = the[ValueRejectedException] thrownBy decode(bytes)
    e.getMessage should include(s"${bytes.length} bytes")
  }

  test("a value nested deeper than the limit fails before it can overflow the stack") {
    assume(RestrictedDeserialization.limitsAvailable, "this JVM has no serialization filter API")
    // without the limit, reading these overflows a 512 KB stack at about 1,000 levels
    val bytes = deeplyNestedArrays(100000)
    val result = onThread(stackSize = 512L << 10)(decode(bytes))
    result.left.get shouldBe a[ValueRejectedException]
    result.left.get.getMessage should include(s"more than ${RestrictedDeserialization.MaxDepth} levels")
  }

  test("rows and schemas nested 60 structs deep still read back") {
    def struct(n: Int): String =
      if (n == 0) "named_struct('a', 1, 'arr', array(map('k', array(1))))"
      else s"named_struct('s', ${struct(n - 1)}, 'arr', array(${struct(0)}), 'm', map('k', 1))"
    roundTrip(s"SELECT ${struct(60)} s")
    val schema = spark.sql(s"SELECT ${struct(60)} s").schema
    RedisSourceRelation.decodeSchema("_spark:deep:schema", SerializationUtils.serialize(schema)) shouldBe schema
  }

  test("a stream that isn't Java serialization still fails as before") {
    an[org.apache.commons.lang3.SerializationException] should be thrownBy decode("not serialized".getBytes)
  }

  test("the error names the Redis key") {
    val bytes = SerializationUtils.serialize(Array[Any](new java.util.HashMap[String, String]()))
    val e = the[ClassNotAllowedException] thrownBy RestrictedDeserialization.withKey("person:1")(decode(bytes))
    e.key shouldBe Some("person:1")
    e.getMessage should include("'person:1'")
  }

  test("a rejected value's error names the Redis key") {
    assume(RestrictedDeserialization.limitsAvailable, "this JVM has no serialization filter API")
    val e = the[ValueRejectedException] thrownBy RestrictedDeserialization.withKey("person:1")(decode(hugeArrayRow))
    e.key shouldBe Some("person:1")
    e.getMessage should include("'person:1'")
  }

  test("patterns allow a class, a package, a package and its subpackages, or a name prefix") {
    val allowlist = ClassAllowlist.parse("a.b.C, p.*, t.**,x.y.Z*")
    allowlist.allows("a.b.C") shouldBe true
    allowlist.allows("a.b.Cx") shouldBe false
    allowlist.allows("a.b.C$Inner") shouldBe false
    allowlist.allows("p.Q") shouldBe true
    allowlist.allows("p.Q$Inner") shouldBe true
    allowlist.allows("p.sub.Q") shouldBe false
    allowlist.allows("t.Q") shouldBe true
    allowlist.allows("t.sub.deeper.Q") shouldBe true
    allowlist.allows("tx.Q") shouldBe false
    allowlist.allows("x.y.Zebra") shouldBe true
    allowlist.allows("x.y.A") shouldBe false
  }

  test("a pattern that isn't a class name, package or prefix is rejected, naming the option") {
    Seq("*", ".*", "a..b", "a.b.", "a.*.C", "a b", "!a.b.C", "a.b.C;d.E").foreach { pattern =>
      withClue(pattern) {
        val e = the[IllegalArgumentException] thrownBy ClassAllowlist.parse(pattern)
        e.getMessage should include(SqlOptionDeserializationAllowedClasses)
      }
    }
    ClassAllowlist.parse(" , ").patterns shouldBe empty
  }

  private def decode(bytes: Array[Byte]): Row = {
    val schema = StructType(Seq(StructField("value", IntegerType)))
    persistence.decodeRow("_id" -> "key", bytes, schema, schema.fieldNames)
  }

  /**
    * A stream of `depth` nested `Object` arrays, each declaring as many elements as the stream has bytes, and holding
    * only the next array. ObjectInputStream allocates each array before it reads the elements.
    */
  private def nestedArrays(depth: Int): Array[Byte] = {
    import ObjectStreamConstants._
    val descriptorBytes = 1 + 2 + "[Ljava.lang.Object;".length + 8 + 1 + 2 + 1 + 1
    val length = 4 + (1 + descriptorBytes + 4) + (depth - 1) * (1 + 5 + 4)
    val out = new ByteArrayOutputStream()
    val data = new DataOutputStream(out)
    data.writeShort(STREAM_MAGIC)
    data.writeShort(STREAM_VERSION)
    data.writeByte(TC_ARRAY)
    data.writeByte(TC_CLASSDESC)
    data.writeUTF("[Ljava.lang.Object;")
    data.writeLong(ObjectStreamClass.lookup(classOf[Array[AnyRef]]).getSerialVersionUID)
    data.writeByte(SC_SERIALIZABLE)
    data.writeShort(0) // fields
    data.writeByte(TC_ENDBLOCKDATA) // class annotations
    data.writeByte(TC_NULL) // superclass
    data.writeInt(length)
    (1 until depth).foreach { _ =>
      data.writeByte(TC_ARRAY)
      data.writeByte(TC_REFERENCE)
      data.writeInt(baseWireHandle) // the class descriptor above
      data.writeInt(length)
    }
    data.flush()
    out.toByteArray.ensuring(_.length == length)
  }

  private def sparkAtLeast(major: Int, minor: Int): Boolean = {
    val Array(ma, mi) = SPARK_VERSION.split("[.-]").take(2).map(_.toInt)
    ma > major || (ma == major && mi >= minor)
  }
}

object RestrictedDeserializationTest {

  /** Runs body on a thread of its own with the given stack size, returning what it threw, errors included. */
  def onThread[T](stackSize: Long)(body: => T): Either[Throwable, T] = {
    var result: Either[Throwable, T] = Left(new IllegalStateException("the thread didn't run"))
    val thread = new Thread(null, new Runnable {
      override def run(): Unit = result = try Right(body) catch {
        case e: Throwable => Left(e)
      }
    }, "deserialize", stackSize)
    thread.start()
    thread.join()
    result
  }

  /** A row whose last value is a long array that declares Int.MaxValue - 8 elements and holds none. */
  def hugeArrayRow: Array[Byte] = rowDeclaringLongArray(Int.MaxValue - 8)

  /** A row whose last value is a long array that declares `length` elements and holds none. */
  def rowDeclaringLongArray(length: Int): Array[Byte] = {
    // the empty array's length is the stream's last 4 bytes
    val bytes = SerializationUtils.serialize(Array[Any]("John", Array.emptyLongArray))
    ByteBuffer.wrap(bytes).putInt(bytes.length - 4, length)
    bytes
  }

  /** `depth` nested `Object[1]` arrays, the innermost holding null: 10 bytes a level. */
  def deeplyNestedArrays(depth: Int): Array[Byte] = {
    import ObjectStreamConstants._
    val out = new ByteArrayOutputStream()
    val data = new DataOutputStream(out)
    data.writeShort(STREAM_MAGIC)
    data.writeShort(STREAM_VERSION)
    data.writeByte(TC_ARRAY)
    data.writeByte(TC_CLASSDESC)
    data.writeUTF("[Ljava.lang.Object;")
    data.writeLong(ObjectStreamClass.lookup(classOf[Array[AnyRef]]).getSerialVersionUID)
    data.writeByte(SC_SERIALIZABLE)
    data.writeShort(0) // fields
    data.writeByte(TC_ENDBLOCKDATA) // class annotations
    data.writeByte(TC_NULL) // superclass
    data.writeInt(1)
    (1 until depth).foreach { _ =>
      data.writeByte(TC_ARRAY)
      data.writeByte(TC_REFERENCE)
      data.writeInt(baseWireHandle) // the class descriptor above
      data.writeInt(1)
    }
    data.writeByte(TC_NULL)
    data.flush()
    out.toByteArray
  }

  /** Records that its deserialization code ran. */
  class Tripwire extends JSerializable {
    private def readObject(in: ObjectInputStream): Unit = {
      in.defaultReadObject()
      Tripwire.tripped.set(true)
    }
  }

  object Tripwire {
    val tripped = new AtomicBoolean(false)
  }

  class SerializableHandler extends InvocationHandler with JSerializable {
    override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = null
  }
}
