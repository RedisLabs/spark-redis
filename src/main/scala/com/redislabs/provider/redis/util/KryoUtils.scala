package com.redislabs.provider.redis.util

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.pool.KryoFactory
import com.esotericsoftware.kryo.pool.KryoPool

object KryoUtils {

  private val TwoDimArrayClass = classOf[Array[Array[Any]]]

  private val factory = new KryoFactory {
    override def create(): Kryo = new Kryo
  }

  val Pool = new KryoPool.Builder(factory).softReferences.build

  def serialize(any: AnyRef, kryo: Kryo): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)
    kryo.writeObject(output, any)
    output.flush()
    output.close()
    baos.toByteArray
  }

  // TODO: name
  def deserializeTwoDimArray(bytes: Array[Byte], kryo: Kryo): Array[Array[Any]] = {
    val input = new Input(bytes)
    val obj = kryo.readObject(input, TwoDimArrayClass)
    obj
  }

}
