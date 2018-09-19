package org.apache.spark.sql.redis

import org.apache.spark.sql.SaveMode.{Append, ErrorIfExists, Ignore, Overwrite}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider
  //  with SchemaRelationProvider
  with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    createRelation0(sqlContext, parameters)

  /**
    * Creates a new relation by saving the data to Redis
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation = createRelation0(sqlContext, parameters)
    mode match {
      case Append => relation.insert(data, overwrite = false)
      case Overwrite => relation.insert(data, overwrite = true)
      case ErrorIfExists =>
        if (relation.nonEmpty) {
          throw new IllegalStateException("SaveMode is set to ErrorIfExists and dataframe " +
            "already exists in Redis and contains data.")
        }
        relation.insert(data, overwrite = false)
      case Ignore =>
        if (relation.isEmpty) {
          relation.insert(data, overwrite = false)
        }
    }

    relation
  }

  private def createRelation0(sqlContext: SQLContext,
                              parameters: Map[String, String]): RedisSourceRelation[_] =
    parameters.getOrElse(SqlOptionMode, SqlOptionModeHash) match {
      case SqlOptionModeBinary => new BinaryRedisSourceRelation(sqlContext, parameters, None)
      case _ => new HashRedisSourceRelation(sqlContext, parameters, None)
    }
}
