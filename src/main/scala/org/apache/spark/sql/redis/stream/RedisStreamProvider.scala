package org.apache.spark.sql.redis.stream

import com.redislabs.provider.redis.util.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @author The Viet Nguyen
  */
class RedisStreamProvider extends DataSourceRegister with StreamSourceProvider with Logging {

  override def shortName(): String = "redis"

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): (String, StructType) = {
    providerName -> schema.getOrElse {
      StructType(Seq(StructField("_id", StringType)))
    }
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType], providerName: String,
                            parameters: Map[String, String]): Source = {
    val (_, ss) = sourceSchema(sqlContext, schema, providerName, parameters)
    val source = new RedisSource(sqlContext, metadataPath, Some(ss), parameters)
    source.start()
    source
  }
}
