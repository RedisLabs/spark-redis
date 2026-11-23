package org.apache.spark.sql.redis

import java.io.{ByteArrayInputStream, IOException, InputStream, InvalidClassException, ObjectInputStream,
  ObjectStreamClass}
import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}

import org.apache.commons.lang3.SerializationException

import scala.util.Try

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
  *
  * The arrays in the stream can't declare more elements in all than the stream has bytes, and the stream can't nest
  * deeper than [[MaxDepth]], see [[StreamLimits]], so a short value can't make the read run out of memory or stack.
  */
private[redis] object RestrictedDeserialization {

  /**
    * The deepest a Java-serialized value may nest, as ObjectInputStream counts it: a level for each object or array
    * inside another. A row or a schema with 30 levels of nested structs reaches about 100, so this reads values with
    * about 60. Reading recurses once per level, and nested Scala maps overflow a 1 MB stack, the default on Linux, at
    * about 480 levels on Scala 2.12, and a 512 KB one at about 320.
    */
  private[redis] val MaxDepth = 200

  /** Whether this JVM lets spark-redis limit what a stream declares, see [[StreamLimits]]. */
  private[redis] def limitsAvailable: Boolean = StreamLimits.available

  /**
    * @throws ClassNotAllowedException if the stream names a class that `allowlist` doesn't allow
    * @throws ValueRejectedException   if the stream's arrays declare more elements than it has bytes, it nests deeper
    *                                  than [[MaxDepth]], or the JVM's serialization filter rejects it
    * @throws SerializationException   if the bytes can't be deserialized otherwise, for example because a class has
    *                                  another `serialVersionUID` (cause: `InvalidClassException`) or doesn't exist
    */
  def deserialize[T](bytes: Array[Byte], allowlist: ClassAllowlist): T =
    try {
      // the constructor already reads the stream header
      val in = new AllowlistObjectInputStream(new ByteArrayInputStream(bytes), allowlist)
      val limits = StreamLimits.install(in, bytes.length)
      try in.readObject().asInstanceOf[T] catch {
        case e: ClassNotAllowedException => throw e
        // what a filter rejects fails as "filter status: REJECTED"
        case e: InvalidClassException if limits.rejection.isDefined =>
          throw new ValueRejectedException(limits.rejection.get, None, e)
      } finally in.close()
    } catch {
      case e: ClassNotAllowedException => throw e
      case e@(_: IOException | _: ClassNotFoundException) => throw new SerializationException(e)
    }

  /** Runs body, adding the Redis key to a [[ClassNotAllowedException]] or [[ValueRejectedException]] it throws. */
  private[redis] def withKey[T](key: String)(body: => T): T =
    try {
      body
    } catch {
      case e: ClassNotAllowedException if e.key.isEmpty => throw e.withKey(key)
      case e: ValueRejectedException if e.key.isEmpty => throw e.withKey(key)
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

  /**
    * Limits what a stream declares, before ObjectInputStream acts on it:
    *
    *  - The elements of its arrays, in all, to the stream's length in bytes. ObjectInputStream allocates an array as
    *    soon as it has read its length, before any element, so without a limit a value of a few bytes could declare
    *    an array of 2^31 longs, or nest arrays that each declare as many elements as the value has bytes. Every
    *    element takes at least one byte of the stream that ObjectOutputStream writes, so this never fails a value
    *    that spark-redis wrote.
    *  - Its depth, to [[MaxDepth]]. ObjectInputStream recurses once per level, so without a limit about 100 KB of
    *    nested arrays or maps could overflow the stack.
    *
    * Running out of memory or stack ends the JVM: Spark ends an executor that does, and the driver reads the schema.
    *
    * ObjectInputStream reports an array's length before allocating it, and the depth of each object before reading
    * it, only to a serialization filter, whose interface is `java.io.ObjectInputFilter` on Java 9 and later and
    * `sun.misc.ObjectInputFilter` on Java 8 (8u121 and later). spark-redis is built for Java 8, so it implements
    * whichever one the JVM has through reflection, and reads without the limits on a JVM that has neither. A filter
    * that the JVM already gives the stream, such as one set with `jdk.serialFilter`, still decides everything these
    * limits don't reject.
    */
  private object StreamLimits {

    /** What one stream has declared so far, and whether a filter rejected it. */
    final class Limits private[StreamLimits](maxElements: Long) {
      private[StreamLimits] var elements = 0L
      private[StreamLimits] var depth = 0L
      private[StreamLimits] var rejectedByJvmFilter = false

      private[StreamLimits] def exceeded: Boolean = elements > maxElements || depth > MaxDepth

      /** Why the stream was rejected, if it was. What went over is neither allocated nor read. */
      def rejection: Option[String] =
        if (elements > maxElements) {
          Some(s"its arrays declare $elements elements, more than its $maxElements bytes can hold, so Spark-Redis " +
            "didn't write it. Reading it could run Spark out of memory.")
        } else if (depth > MaxDepth) {
          Some(s"it nests more than $MaxDepth levels deep, which a value Spark-Redis wrote only does with more than " +
            "60 levels of nested structs. Reading it could overflow the stack.")
        } else if (rejectedByJvmFilter) {
          Some("the JVM's serialization filter, for example one set with jdk.serialFilter, rejected it.")
        } else {
          None
        }
    }

    /** The parts of a serialization filter API. */
    private final case class FilterApi(filterInterface: Class[_], checkInput: Method, arrayLength: Method,
                                       depth: Method, rejected: AnyRef, undecided: AnyRef,
                                       get: ObjectInputStream => AnyRef, set: (ObjectInputStream, AnyRef) => Unit)

    private def filterApi(filterInterface: String, get: Class[_] => ObjectInputStream => AnyRef,
                          set: Class[_] => (ObjectInputStream, AnyRef) => Unit): FilterApi = {
      val filter = Class.forName(filterInterface)
      val info = Class.forName(filterInterface + "$FilterInfo")
      val status = Class.forName(filterInterface + "$Status")
      FilterApi(filter, filter.getMethod("checkInput", info), info.getMethod("arrayLength"), info.getMethod("depth"),
        status.getField("REJECTED").get(null), status.getField("UNDECIDED").get(null), get(filter), set(filter))
    }

    /** Java 9 and later: methods of ObjectInputStream. */
    private def javaIoApi: FilterApi = filterApi("java.io.ObjectInputFilter",
      _ => {
        val getter = classOf[ObjectInputStream].getMethod("getObjectInputFilter")
        in => getter.invoke(in)
      },
      filter => {
        val setter = classOf[ObjectInputStream].getMethod("setObjectInputFilter", filter)
        (in, f) => setter.invoke(in, f)
      })

    /** Java 8u121 and later: static methods of ObjectInputFilter.Config. */
    private def sunMiscApi: FilterApi = filterApi("sun.misc.ObjectInputFilter",
      _ => {
        val getter = Class.forName("sun.misc.ObjectInputFilter$Config")
          .getMethod("getObjectInputFilter", classOf[ObjectInputStream])
        in => getter.invoke(null, in)
      },
      filter => {
        val setter = Class.forName("sun.misc.ObjectInputFilter$Config")
          .getMethod("setObjectInputFilter", classOf[ObjectInputStream], filter)
        (in, f) => setter.invoke(null, in, f)
      })

    private lazy val api: Option[FilterApi] = Try(javaIoApi).orElse(Try(sunMiscApi)).toOption

    def available: Boolean = api.isDefined

    /**
      * Limits the arrays that `in` reads to `maxElements` elements in all, and its depth to [[MaxDepth]]. If the JVM
      * has no filter API, or doesn't take the filter, `in` reads without the limits, as before they existed.
      */
    def install(in: ObjectInputStream, maxElements: Long): Limits = {
      val limits = new Limits(maxElements)
      api.foreach { api =>
        Try {
          val previous = Option(api.get(in))
          val handler = new InvocationHandler {
            override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
              case "checkInput" =>
                val info = args(0)
                // the length of the array about to be allocated, or -1 for a class or a reference
                val length = long(api.arrayLength, info)
                if (length > 0) limits.elements += length
                // 1 for the value itself, and one more for each object or array inside another
                limits.depth = math.max(limits.depth, long(api.depth, info))
                if (limits.exceeded) {
                  api.rejected
                } else {
                  previous.fold(api.undecided)(filter => checkedBy(filter, info))
                }
              case "equals" => java.lang.Boolean.valueOf(proxy eq args(0))
              case "hashCode" => Integer.valueOf(System.identityHashCode(proxy))
              case "toString" => s"spark-redis stream limits ($maxElements array elements, depth $MaxDepth)"
              case _ => throw new UnsupportedOperationException(method.toString)
            }

            /** What the JVM's own filter says, noting a rejection. ObjectInputStream rejects on an exception too. */
            private def checkedBy(filter: AnyRef, info: AnyRef): AnyRef = {
              val status = try unwrapped(api.checkInput.invoke(filter, info)) catch {
                case e: RuntimeException =>
                  limits.rejectedByJvmFilter = true
                  throw e
              }
              // ObjectInputStream rejects a null status too
              if (status == null || status == api.rejected) limits.rejectedByJvmFilter = true
              status
            }
          }
          api.set(in, Proxy.newProxyInstance(getClass.getClassLoader, Array(api.filterInterface), handler))
        }
      }
      limits
    }

    private def long(method: Method, info: AnyRef): Long = method.invoke(info).asInstanceOf[java.lang.Long].longValue

    private def unwrapped(result: => AnyRef): AnyRef =
      try result catch {
        case e: InvocationTargetException => throw e.getCause
      }
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

  private[redis] def withKey(key: String): ClassNotAllowedException = {
    val keyed = new ClassNotAllowedException(classname, Some(key), proxy)
    keyed.setStackTrace(getStackTrace)
    keyed
  }
}

/**
  * A Java-serialized value read from Redis was not read, because it declares more than a value spark-redis wrote
  * can, or because the JVM's serialization filter rejected it, see [[RestrictedDeserialization]]. It is thrown before
  * anything the value declares beyond that is allocated or read.
  *
  * @param reason why the value was rejected
  * @param key    the Redis key the value was read from, when known
  */
final class ValueRejectedException private[redis](val reason: String, val key: Option[String], cause: Throwable)
  extends SerializationException(reason, cause) {

  override def getMessage: String = {
    val where = key.map(k => s" in key '$k'").getOrElse("")
    s"The Java-serialized value$where was not read: $reason"
  }

  private[redis] def withKey(key: String): ValueRejectedException = {
    val keyed = new ValueRejectedException(reason, Some(key), getCause)
    keyed.setStackTrace(getStackTrace)
    keyed
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
