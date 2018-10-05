### Dataframe

#### User defined key column

By default, Spark-Redis generates UUID identifier for each row to ensure their uniqueness.
However, you can also provide your own column as key, e.g.
```scala
df.write.format(RedisFormat).option(SqlOptionKeyColumn, "name").save(tableName)
```
When key collision happens on ```SaveMode.Append```, the former row would be replaced by the new row.
