package org.apache.spark.sql.redis

import java.io.{ByteArrayInputStream, IOException, InputStream, InvalidClassException, ObjectInputStream,
  ObjectStreamClass}

import org.apache.commons.lang3.SerializationException

/**
  * Java deserialization of values read from Redis, limited to the classes that spark-redis itself writes there.
  *
  * The binary model stores each row as a Java-serialized array of its values, and every release up to 3.1.0 stored a
  * table's schema as a Java-serialized `StructType`. Anyone who can write to those keys can store any serialized
  * object there instead, and a plain `ObjectInputStream` loads and instantiates whatever classes the stream names.
  * With a gadget chain on the classpath, reading the table then runs that code on the driver or the executors.
  * Spark's Scala 2.13 builds ship Scala 2.13.8, for example, whose `LazyList` is one (CVE-2022-36944).
  *
  * So every class in the stream must be allowed by a [[ClassAllowlist]], and it is checked before the class is
  * loaded, so a class that isn't allowed never runs any code. Dynamic proxies are never allowed.
  */
private[redis] object RestrictedDeserialization {

  /**
    * @throws ClassNotAllowedException if the stream names a class that `allowlist` doesn't allow
    * @throws SerializationException   if the bytes can't be deserialized, for example because a class has another
    *                                  `serialVersionUID` (cause: `InvalidClassException`) or doesn't exist
    */
  def deserialize[T](bytes: Array[Byte], allowlist: ClassAllowlist): T =
    try {
      // the constructor already reads the stream header
      val in = new AllowlistObjectInputStream(new ByteArrayInputStream(bytes), allowlist)
      try in.readObject().asInstanceOf[T] finally in.close()
    } catch {
      case e: ClassNotAllowedException => throw e
      case e@(_: IOException | _: ClassNotFoundException) => throw new SerializationException(e)
    }

  private class AllowlistObjectInputStream(in: InputStream, allowlist: ClassAllowlist) extends ObjectInputStream(in) {

    /**
      * An array is only a container: its component class is loaded without being initialized, so none of its code
      * runs, and each element's own class is checked when the element is read. So arrays of any type are allowed,
      * which keeps the arrays Spark builds for each Scala and Spark version readable.
      */
    override protected def resolveClass(desc: ObjectStreamClass): Class[_] = {
      val name = desc.getName
      if (!name.startsWith("[") && !allowlist.allows(name)) {
        throw new ClassNotAllowedException(name)
      }
      super.resolveClass(desc)
    }

    override protected def resolveProxyClass(interfaces: Array[String]): Class[_] =
      throw new ClassNotAllowedException(interfaces.mkString(", "), proxy = true)
  }
}

/**
  * A Java-serialized value read from Redis names a class that isn't allowed, see [[RestrictedDeserialization]].
  * It is thrown before the class is loaded.
  *
  * @param className the class, as the stream names it, or the interfaces of a dynamic proxy
  * @param key       the Redis key the value was read from, when known
  * @param proxy     whether the stream holds a dynamic proxy, which is never allowed
  */
final class ClassNotAllowedException(className: String, val key: Option[String] = None, val proxy: Boolean = false)
  extends InvalidClassException(className, "not allowed") {

  override def getMessage: String = {
    val where = key.map(k => s" in key '$k'").getOrElse("")
    val what = if (proxy) s"a dynamic proxy implementing $className" else s"class '$className'"
    val hint = if (proxy) "" else " If the value holds a user-defined type, allow its class with the option " +
      s"'$SqlOptionDeserializationAllowedClasses'."
    s"The Java-serialized value$where contains $what, which isn't allowed. spark-redis only deserializes the " +
      "classes it writes itself, so that a value someone else stored in Redis can't run code in Spark." + hint
  }
}

object ClassNotAllowedException {

  /** Runs body, adding the Redis key to a [[ClassNotAllowedException]] it throws. */
  private[redis] def withKey[T](key: String)(body: => T): T =
    try {
      body
    } catch {
      case e: ClassNotAllowedException if e.key.isEmpty =>
        val withKey = new ClassNotAllowedException(e.classname, Some(key), e.proxy)
        withKey.setStackTrace(e.getStackTrace)
        throw withKey
    }
}

/**
  * Which classes a Java-serialized value read from Redis may contain. Each pattern is one of:
  *
  *  - `a.b.C`: the class `a.b.C`. A nested class is named with `$`, as in `a.b.Outer$Inner`.
  *  - `a.b.*`: every class in package `a.b`
  *  - `a.b.**`: every class in package `a.b` and its subpackages
  *  - `a.b.C*`: every class whose name starts with `a.b.C`
  *
  * These are the patterns of the JDK's serialization filters (`jdk.serialFilter`), without rejections and limits.
  */
final case class ClassAllowlist(patterns: Seq[String]) {

  private val (exact, packages, trees, prefixes) = {
    val (wildcards, names) = patterns.partition(_.endsWith("*"))
    val (treePatterns, rest) = wildcards.partition(_.endsWith(".**"))
    val (packagePatterns, prefixPatterns) = rest.partition(_.endsWith(".*"))
    (names.toSet,
      packagePatterns.map(_.dropRight(1)).toSet, // "a.b."
      treePatterns.map(_.dropRight(2)), // "a.b."
      prefixPatterns.map(_.dropRight(1)))
  }

  def allows(className: String): Boolean =
    exact.contains(className) ||
      packages.contains(className.substring(0, className.lastIndexOf('.') + 1)) ||
      trees.exists(className.startsWith) ||
      prefixes.exists(className.startsWith)

  def ++(other: ClassAllowlist): ClassAllowlist = ClassAllowlist(patterns ++ other.patterns)
}

object ClassAllowlist {

  /**
    * The classes that the values spark-redis writes contain: Spark's external types for each column type and the
    * classes they're serialized with, for Spark 3.2 to 3.5 on Scala 2.12 and 2.13.
    */
  val Default: ClassAllowlist = ClassAllowlist(Seq(
    // atomic types (strings are written without a class descriptor)
    "java.lang.Boolean", "java.lang.Byte", "java.lang.Short", "java.lang.Integer", "java.lang.Long",
    "java.lang.Float", "java.lang.Double", "java.lang.Character", "java.lang.Number", "java.lang.String",
    "java.math.BigDecimal", "java.math.BigInteger",
    "java.util.Date", "java.sql.Date", "java.sql.Timestamp",
    // LocalDate, Instant, LocalDateTime, Duration and Period are all written as java.time.Ser
    "java.time.Ser",
    "org.apache.spark.unsafe.types.CalendarInterval",
    // structs and schemas
    "org.apache.spark.sql.catalyst.expressions.GenericRow",
    "org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema",
    "org.apache.spark.sql.types.*",
    // In Spark 3.2 to 3.4, CharType, VarcharType and the ANSI interval types hold the ordering of their values:
    // Ordering.ordered (anonymous class 2 on Scala 2.12.15+ and 2.13, which Spark 3.2+ ship; class 3 wraps any
    // Comparator, and isn't allowed) with the identity function A <:< A, or Ordering.Int and Ordering.Long.
    "scala.math.LowPriorityOrderingImplicits$$anon$2", "scala.math.Ordering$Int$", "scala.math.Ordering$Long$",
    "scala.Predef$$anon$2", "scala.Predef$$less$colon$less", // Scala 2.12
    "scala.$less$colon$less$$anon$1", "scala.$less$colon$less", "scala.$eq$colon$eq", // Scala 2.13
    // arrays, maps, and the set of field names that a StructType caches once a field is looked up, on Scala 2.12
    "scala.collection.mutable.WrappedArray$of*",
    "scala.collection.immutable.Map$EmptyMap$",
    "scala.collection.immutable.Map$Map1", "scala.collection.immutable.Map$Map2",
    "scala.collection.immutable.Map$Map3", "scala.collection.immutable.Map$Map4",
    "scala.collection.immutable.HashMap$SerializationProxy",
    "scala.collection.immutable.Set$EmptySet$",
    "scala.collection.immutable.Set$Set1", "scala.collection.immutable.Set$Set2",
    "scala.collection.immutable.Set$Set3", "scala.collection.immutable.Set$Set4",
    "scala.collection.immutable.HashSet$SerializationProxy",
    // and on Scala 2.13, which writes Scala objects and larger maps and sets through proxies
    "scala.collection.mutable.ArraySeq", "scala.collection.mutable.ArraySeq$of*",
    "scala.collection.immutable.ArraySeq", "scala.collection.immutable.ArraySeq$of*",
    "scala.collection.generic.DefaultSerializationProxy", "scala.collection.generic.SerializeEnd$",
    "scala.collection.MapFactory$ToFactory", "scala.collection.immutable.HashMap$",
    "scala.collection.IterableFactory$ToFactory", "scala.collection.immutable.HashSet$",
    "scala.runtime.ModuleSerializationProxy",
    "scala.Tuple2"
  ))

  /**
    * Parses the [[SqlOptionDeserializationAllowedClasses]] option: comma-separated patterns.
    *
    * @throws IllegalArgumentException for a pattern that is not one of the forms above
    */
  def parse(value: String): ClassAllowlist = {
    val patterns = value.split(',').map(_.trim).filter(_.nonEmpty).toSeq
    patterns.foreach { pattern =>
      val base = Seq(".**", ".*", "*").find(pattern.endsWith).map(w => pattern.dropRight(w.length)).getOrElse(pattern)
      val valid = base.nonEmpty && !base.startsWith(".") && !base.endsWith(".") && !base.contains("..") &&
        base.forall(c => c == '.' || Character.isJavaIdentifierPart(c))
      if (!valid) {
        throw new IllegalArgumentException(s"Invalid class pattern '$pattern' in option " +
          s"'$SqlOptionDeserializationAllowedClasses'. Use a class name (a.b.C), a package (a.b.*), a package and " +
          "its subpackages (a.b.**) or a name prefix (a.b.C*), separated by commas.")
      }
    }
    ClassAllowlist(patterns)
  }
}
