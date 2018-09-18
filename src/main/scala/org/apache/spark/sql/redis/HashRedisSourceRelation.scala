//package org.apache.spark.sql.redis
//
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.sql.types.StructType
//
///**
//  * @author The Viet Nguyen
//  */
//class HashRedisSourceRelation(override val sqlContext: SQLContext,
//                                parameters: Map[String, String],
//                                userSpecifiedSchema: Option[StructType])
//  extends RedisSourceRelation(sqlContext, parameters, userSpecifiedSchema) {
//
//  override def rowToBytes(value: Row): Array[Byte] = ???
//
//  override def bytesToRow(value: Array[Byte]): Row = ???
//}
