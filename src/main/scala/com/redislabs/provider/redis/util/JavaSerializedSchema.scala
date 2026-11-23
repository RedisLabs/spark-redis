package com.redislabs.provider.redis.util

import java.io.{ByteArrayInputStream, DataInputStream, EOFException, IOException}
import java.nio.ByteBuffer

import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * Reads a table schema that earlier versions of spark-redis stored as a Java-serialized `StructType`, whatever Spark
  * and Scala versions wrote it.
  *
  * Java deserialization can't do that: `StructType` declares no `serialVersionUID`, so the one the JVM computes differs
  * in every Spark minor version, and a schema's metadata holds Scala collections, whose classes differ between Scala
  * 2.12 and 2.13. So this parses the stream itself, following the grammar of the Java Object Serialization Stream
  * Protocol, and builds the schema from the field values it finds. It never deserializes an object from the stream:
  * the only classes it loads are the data types the schema names, and only after checking that each one is a Spark
  * `DataType`. Spark's own types are looked up by name, and a user-defined type is created with its no-arg
  * constructor.
  *
  * Supported: every Spark data type that is a singleton (`StringType`, `IntegerType`, `TimestampType`, ...), decimal,
  * char and varchar, array, map, struct, the day-time and year-month interval types, user-defined types with a no-arg
  * constructor, and field metadata, as Spark stores them on Scala 2.12 or 2.13.
  */
object JavaSerializedSchema {

  /**
    * @param bytes a Java serialization stream that holds one `StructType`
    * @throws IllegalArgumentException if the bytes aren't such a stream, or hold something it can't read, such as a
    *                                  data type that this Spark version doesn't have
    */
  def read(bytes: Array[Byte]): StructType = {
    val value = try new StreamReader(bytes).readStream() catch {
      case _: EOFException => fail("the stream ends early")
      case e: IOException => fail(s"the stream is malformed (${e.getMessage})")
    }
    value match {
      case o: Obj if o.className == StructTypeClass => toStruct(o)
      case o: Obj => fail(s"it holds a ${o.className}, not a table schema (StructType)")
      case other => fail(s"it holds ${describe(other)}, not a table schema (StructType)")
    }
  }

  private val StructTypeClass = classOf[StructType].getName
  private val TypesPackage = "org.apache.spark.sql.types."

  private def fail(reason: String): Nothing =
    throw new IllegalArgumentException(s"Can't read the Java-serialized schema: $reason")

  // ---- The stream, as a tree of values. A value is a primitive (boxed), a String, an Obj, an Arr, an EnumConst,
  // ---- a ClassRef, a ClassDesc, a Block of annotation bytes, or null.

  private final class FieldDesc(val typeCode: Char, val name: String)

  private final class ClassDesc(val name: String) {
    var flags: Int = 0
    var fields: Seq[FieldDesc] = Nil
    var superDesc: Option[ClassDesc] = None

    /** This class and its serializable superclasses, the top-most first, as their data appears in the stream. */
    def hierarchy: Seq[ClassDesc] = superDesc.map(_.hierarchy).getOrElse(Nil) :+ this
  }

  private final class Obj(val desc: ClassDesc) {
    val fields: mutable.Map[String, Any] = mutable.Map()
    /** What each class's `writeObject` or `writeExternal` wrote after its fields, by class name. */
    val annotations: mutable.Map[String, Seq[Any]] = mutable.Map()

    /**
      * The object's class. Scala 2.13 writes a Scala `object` (such as `StringType` or an empty `Map`) as a
      * `ModuleSerializationProxy` that names the object's class, so for one of those, that class.
      */
    def className: String =
      if (desc.name == ModuleSerializationProxy) {
        fields.get("moduleClass") match {
          case Some(ClassRef(module)) => module.name
          case _ => fail(s"a $ModuleSerializationProxy doesn't name its object's class")
        }
      } else {
        desc.name
      }

    def field(name: String): Any = fields.getOrElse(name, fail(s"a $className has no field '$name'"))
  }

  private val ModuleSerializationProxy = "scala.runtime.ModuleSerializationProxy"

  private final class Arr(val desc: ClassDesc, val values: IndexedSeq[Any])

  private final case class EnumConst(desc: ClassDesc, name: String)

  private final case class ClassRef(desc: ClassDesc)

  private final class Block(val bytes: Array[Byte])

  private case object EndBlock

  /** A parser for the grammar in the Java Object Serialization Specification, chapter 6. */
  private final class StreamReader(bytes: Array[Byte]) {
    private val in = new DataInputStream(new ByteArrayInputStream(bytes))
    private val handles = mutable.ArrayBuffer[Any]()
    private val MaxDepth = 1000
    private var depth = 0

    private final val Magic: Short = 0xACED.toShort
    private final val Version: Short = 5
    private final val BaseHandle = 0x7E0000

    private final val TcNull = 0x70
    private final val TcReference = 0x71
    private final val TcClassDesc = 0x72
    private final val TcObject = 0x73
    private final val TcString = 0x74
    private final val TcArray = 0x75
    private final val TcClass = 0x76
    private final val TcBlockData = 0x77
    private final val TcEndBlockData = 0x78
    private final val TcReset = 0x79
    private final val TcBlockDataLong = 0x7A
    private final val TcException = 0x7B
    private final val TcLongString = 0x7C
    private final val TcProxyClassDesc = 0x7D
    private final val TcEnum = 0x7E

    private final val ScWriteMethod = 0x01
    private final val ScSerializable = 0x02
    private final val ScExternalizable = 0x04
    private final val ScBlockData = 0x08

    def readStream(): Any = {
      if (in.readShort() != Magic) fail("it doesn't start with the Java serialization magic number")
      val version = in.readShort()
      if (version != Version) fail(s"stream version $version isn't supported")
      readContent() match {
        case EndBlock | _: Block => fail("it holds no object")
        case value => value
      }
    }

    private def newHandle[A](value: A): A = {
      handles += value
      value
    }

    private def nested[A](body: => A): A = {
      depth += 1
      if (depth > MaxDepth) fail(s"objects are nested more than $MaxDepth deep")
      try body finally depth -= 1
    }

    private def readContent(): Any = nested {
      in.readUnsignedByte() match {
        case TcNull => null
        case TcReference => readReference()
        case TcObject => readObject()
        case TcString => newHandle(in.readUTF())
        case TcLongString => newHandle(readLongUtf())
        case TcArray => readArray()
        case TcEnum =>
          val desc = readClassDesc().getOrElse(fail("an enum has no class"))
          val handle = handles.size
          newHandle(null)
          val constant = EnumConst(desc, readContent() match {
            case name: String => name
            case other => fail(s"an enum constant's name is ${describe(other)}")
          })
          handles(handle) = constant
          constant
        case TcClass => newHandle(ClassRef(readClassDesc().getOrElse(fail("a class reference has no class"))))
        case TcClassDesc => readNewClassDesc()
        case TcProxyClassDesc => readNewProxyClassDesc()
        case TcBlockData => new Block(readBytes(in.readUnsignedByte()))
        case TcBlockDataLong => new Block(readBytes(in.readInt()))
        case TcEndBlockData => EndBlock
        case TcReset =>
          handles.clear()
          readContent()
        case TcException => fail("it holds an exception the writer threw")
        case other => fail(f"unknown type code 0x$other%02X")
      }
    }

    private def readReference(): Any = {
      val handle = in.readInt() - BaseHandle
      if (handle < 0 || handle >= handles.size) fail(s"a reference points to a handle that doesn't exist")
      handles(handle)
    }

    private def readBytes(length: Int): Array[Byte] = {
      if (length < 0 || length > in.available()) fail("a length goes past the end of the stream")
      val b = new Array[Byte](length)
      in.readFully(b)
      b
    }

    /** A TC_LONGSTRING: an 8-byte length and modified UTF-8, as `DataInputStream.readUTF` reads with a 2-byte one. */
    private def readLongUtf(): String = {
      val length = in.readLong()
      if (length < 0 || length > in.available()) fail("a string goes past the end of the stream")
      val b = readBytes(length.toInt)
      // prefix the 2-byte length readUTF expects, chunk by chunk: no modified UTF-8 sequence is longer than 3 bytes
      val out = new StringBuilder
      var at = 0
      while (at < b.length) {
        var end = math.min(at + 65535, b.length)
        while (end < b.length && (b(end) & 0xC0) == 0x80) end -= 1
        val chunk = ByteBuffer.allocate(2 + end - at).putShort((end - at).toShort).put(b, at, end - at).array()
        out.append(new DataInputStream(new ByteArrayInputStream(chunk)).readUTF())
        at = end
      }
      out.toString
    }

    private def readClassDesc(): Option[ClassDesc] = nested {
      in.readUnsignedByte() match {
        case TcNull => None
        case TcClassDesc => Some(readNewClassDesc())
        case TcProxyClassDesc => Some(readNewProxyClassDesc())
        case TcReference => readReference() match {
          case desc: ClassDesc => Some(desc)
          case other => fail(s"a class descriptor reference points to ${describe(other)}")
        }
        case other => fail(f"expected a class descriptor, got type code 0x$other%02X")
      }
    }

    private def readNewClassDesc(): ClassDesc = {
      val desc = new ClassDesc(in.readUTF())
      in.readLong() // serialVersionUID: ignored, which is the point
      newHandle(desc)
      desc.flags = in.readUnsignedByte()
      val count = in.readShort()
      if (count < 0) fail(s"${desc.name} has a negative field count")
      desc.fields = (0 until count).map { _ =>
        val typeCode = in.readUnsignedByte().toChar
        val name = in.readUTF()
        if (typeCode == 'L' || typeCode == '[') {
          readContent() match {
            case _: String => // the field's class name, which the values describe themselves
            case other => fail(s"field '$name' of ${desc.name} has a class name that is ${describe(other)}")
          }
        } else if ("BCDFIJSZ".indexOf(typeCode) < 0) {
          fail(s"field '$name' of ${desc.name} has unknown type code '$typeCode'")
        }
        new FieldDesc(typeCode, name)
      }
      readUntilEnd() // class annotation
      desc.superDesc = readClassDesc()
      desc
    }

    private def readNewProxyClassDesc(): ClassDesc = {
      val desc = newHandle(new ClassDesc("<proxy>"))
      val interfaces = in.readInt()
      if (interfaces < 0 || interfaces > in.available()) fail("a proxy class has a bad interface count")
      (0 until interfaces).foreach(_ => in.readUTF())
      readUntilEnd() // class annotation
      desc.superDesc = readClassDesc()
      desc
    }

    private def readObject(): Obj = {
      val desc = readClassDesc().getOrElse(fail("an object has no class"))
      val obj = newHandle(new Obj(desc))
      desc.hierarchy.foreach { cls =>
        if ((cls.flags & ScSerializable) != 0) {
          cls.fields.foreach(f => obj.fields(f.name) = readValue(f.typeCode))
          if ((cls.flags & ScWriteMethod) != 0) obj.annotations(cls.name) = readUntilEnd()
        } else if ((cls.flags & ScExternalizable) != 0) {
          if ((cls.flags & ScBlockData) == 0) fail(s"${cls.name} uses the old externalizable format")
          obj.annotations(cls.name) = readUntilEnd()
        }
      }
      obj
    }

    private def readArray(): Arr = {
      val desc = readClassDesc().getOrElse(fail("an array has no class"))
      val handle = handles.size
      newHandle(null)
      val size = in.readInt()
      if (size < 0 || size > in.available()) fail(s"an array of ${desc.name} has a bad size")
      val componentType = if (desc.name.length > 1) desc.name.charAt(1) else fail(s"bad array class ${desc.name}")
      val arr = new Arr(desc, (0 until size).map(_ => readValue(componentType)))
      handles(handle) = arr
      arr
    }

    private def readValue(typeCode: Char): Any = typeCode match {
      case 'B' => in.readByte()
      case 'C' => in.readChar()
      case 'D' => in.readDouble()
      case 'F' => in.readFloat()
      case 'I' => in.readInt()
      case 'J' => in.readLong()
      case 'S' => in.readShort()
      case 'Z' => in.readBoolean()
      case 'L' | '[' => readContent()
      case other => fail(s"unknown type code '$other'")
    }

    /** Contents up to TC_ENDBLOCKDATA: an object's or a class's annotation. */
    private def readUntilEnd(): Seq[Any] = {
      val contents = mutable.ArrayBuffer[Any]()
      var value = readContent()
      while (value != EndBlock) {
        contents += value
        value = readContent()
      }
      contents.toList
    }
  }

  private def describe(value: Any): String = value match {
    case null => "null"
    case o: Obj => s"a ${o.className}"
    case a: Arr => s"an array of ${a.desc.name}"
    case _: String => "a string"
    case other => s"a ${other.getClass.getSimpleName}"
  }

  // ---- From the tree to Spark's data types

  private def toStruct(o: Obj): StructType = o.field("fields") match {
    case arr: Arr => StructType(arr.values.map(toStructField))
    case other => fail(s"a StructType's fields are ${describe(other)}")
  }

  private def toStructField(value: Any): StructField = value match {
    case o: Obj if o.className == classOf[StructField].getName =>
      val name = o.field("name") match {
        case s: String => s
        case other => fail(s"a field's name is ${describe(other)}")
      }
      StructField(name, toDataType(o.field("dataType")), boolean(o, "nullable"), toMetadata(o.field("metadata")))
    case other => fail(s"a StructType holds ${describe(other)} among its fields")
  }

  private def toDataType(value: Any): DataType = value match {
    case o: Obj => o.className.stripPrefix(TypesPackage) match {
      case "StructType" => toStruct(o)
      case "ArrayType" => ArrayType(toDataType(o.field("elementType")), boolean(o, "containsNull"))
      case "MapType" =>
        MapType(toDataType(o.field("keyType")), toDataType(o.field("valueType")), boolean(o, "valueContainsNull"))
      case "DecimalType" => DecimalType(int(o, "precision"), int(o, "scale"))
      case "CharType" => CharType(int(o, "length"))
      case "VarcharType" => VarcharType(int(o, "length"))
      case "DayTimeIntervalType" => DayTimeIntervalType(byte(o, "startField"), byte(o, "endField"))
      case "YearMonthIntervalType" => YearMonthIntervalType(byte(o, "startField"), byte(o, "endField"))
      case _ if o.className.startsWith(TypesPackage) && o.className.endsWith("$") => singletonType(o.className)
      case _ => userDefinedType(o.className)
    }
    case other => fail(s"a field's data type is ${describe(other)}")
  }

  /** A data type that is a Scala object, such as `StringType`: the stream holds `StringType$`. */
  private def singletonType(className: String): DataType = {
    val cls = localClass(className)
    if (!classOf[DataType].isAssignableFrom(cls)) fail(s"$className is not a Spark data type")
    cls.getField("MODULE$").get(null).asInstanceOf[DataType]
  }

  private def userDefinedType(className: String): DataType = {
    val cls = localClass(className)
    if (!classOf[UserDefinedType[_]].isAssignableFrom(cls)) fail(s"$className is not a Spark data type")
    try cls.getConstructor().newInstance().asInstanceOf[DataType] catch {
      case _: NoSuchMethodException => fail(s"the user-defined type $className has no no-arg constructor")
    }
  }

  private def localClass(className: String): Class[_] =
    try Class.forName(className, false, Option(Thread.currentThread.getContextClassLoader).getOrElse(getClass.getClassLoader))
    catch {
      case _: ClassNotFoundException => fail(s"the data type $className doesn't exist in this Spark version")
    }

  private def toMetadata(value: Any): Metadata = value match {
    case null => Metadata.empty
    case o: Obj if o.className == classOf[Metadata].getName =>
      val builder = new MetadataBuilder
      mapEntries(o.field("map")).foreach {
        case (key: String, v) => putMetadataValue(builder, key, v)
        case (key, _) => fail(s"a metadata key is ${describe(key)}")
      }
      builder.build()
    case other => fail(s"a field's metadata is ${describe(other)}")
  }

  /** Metadata values are Long, Double, Boolean, String, Metadata, or arrays of one of those. */
  private def putMetadataValue(builder: MetadataBuilder, key: String, value: Any): Unit = value match {
    case s: String => builder.putString(key, s)
    case o: Obj if o.className == "java.lang.Long" => builder.putLong(key, unbox[Long](o))
    case o: Obj if o.className == "java.lang.Double" => builder.putDouble(key, unbox[Double](o))
    case o: Obj if o.className == "java.lang.Boolean" => builder.putBoolean(key, unbox[Boolean](o))
    case o: Obj if o.className == classOf[Metadata].getName => builder.putMetadata(key, toMetadata(o))
    case a: Arr => a.desc.name match {
      case "[J" => builder.putLongArray(key, a.values.map(_.asInstanceOf[Long]).toArray)
      case "[D" => builder.putDoubleArray(key, a.values.map(_.asInstanceOf[Double]).toArray)
      case "[Z" => builder.putBooleanArray(key, a.values.map(_.asInstanceOf[Boolean]).toArray)
      case "[Ljava.lang.String;" => builder.putStringArray(key, a.values.map {
        case s: String => s
        case other => fail(s"metadata '$key' holds ${describe(other)} in a string array")
      }.toArray)
      case "[Lorg.apache.spark.sql.types.Metadata;" =>
        builder.putMetadataArray(key, a.values.map(toMetadata).toArray)
      case other => fail(s"metadata '$key' is an array of $other")
    }
    case other => fail(s"metadata '$key' is ${describe(other)}")
  }

  private def unbox[A](o: Obj): A = o.field("value").asInstanceOf[A]

  /**
    * The entries of a serialized Scala immutable `Map`. Scala 2.12 and 2.13 write maps of up to four entries with
    * their fields, and larger ones through a serialization proxy that writes the entries in its `writeObject`.
    */
  private def mapEntries(value: Any): Seq[(Any, Any)] = value match {
    case o: Obj =>
      val name = o.className
      if (name.endsWith("$EmptyMap$")) {
        Nil
      } else if (name.matches("scala\\.collection\\.immutable\\.Map\\$Map[1-4]")) {
        // Scala 2.13 mangles the names of Map2 to Map4's fields: scala$collection$immutable$Map$Map2$$key1
        def mapField(field: String): Any = o.fields.getOrElse(field,
          o.fields.collectFirst { case (k, v) if k.endsWith("$$" + field) => v }
            .getOrElse(fail(s"a $name has no field '$field'")))
        (1 to name.last.asDigit).map(i => (mapField(s"key$i"), mapField(s"value$i")))
      } else if (name == "scala.collection.immutable.HashMap$SerializationProxy") {
        // Scala 2.12: writeInt(size), then each key and value
        val (size, objects) = sizeAndObjects(o, name)
        if (objects.length != 2 * size) fail(s"a map that holds $size entries has ${objects.length} keys and values")
        objects.grouped(2).map(kv => (kv.head, kv(1))).toList
      } else if (name == "scala.collection.generic.DefaultSerializationProxy") {
        // Scala 2.13: writeInt(knownSize), then each entry as a Tuple2, then SerializeEnd if the size wasn't known
        val (size, objects) = sizeAndObjects(o, name)
        val entries = objects.filterNot {
          case end: Obj => end.className == "scala.collection.generic.SerializeEnd$"
          case _ => false
        }
        if (size >= 0 && entries.length != size) fail(s"a map that holds $size entries has ${entries.length}")
        entries.map {
          case t: Obj if t.className == "scala.Tuple2" => (t.field("_1"), t.field("_2"))
          case other => fail(s"a map entry is ${describe(other)}")
        }
      } else {
        fail(s"metadata is a $name, which isn't a Scala map this reader knows")
      }
    case other => fail(s"metadata holds ${describe(other)}, not a map")
  }

  /** A proxy's `writeObject` annotation: an Int in block data, then objects. */
  private def sizeAndObjects(o: Obj, className: String): (Int, Seq[Any]) = {
    val annotation = o.annotations.getOrElse(className, fail(s"a $className has no data"))
    val blockBytes = annotation.collect { case b: Block => b.bytes.toSeq }.flatten.toArray
    if (blockBytes.length < 4) fail(s"a $className doesn't say how many entries it holds")
    (ByteBuffer.wrap(blockBytes, 0, 4).getInt, annotation.filterNot(_.isInstanceOf[Block]))
  }

  private def boolean(o: Obj, field: String): Boolean = o.field(field) match {
    case b: Boolean => b
    case other => fail(s"field '$field' of a ${o.className} is ${describe(other)}")
  }

  private def int(o: Obj, field: String): Int = o.field(field) match {
    case i: Int => i
    case other => fail(s"field '$field' of a ${o.className} is ${describe(other)}")
  }

  private def byte(o: Obj, field: String): Byte = o.field(field) match {
    case b: Byte => b
    case other => fail(s"field '$field' of a ${o.className} is ${describe(other)}")
  }
}
