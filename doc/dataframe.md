# Dataset (previously Dataframe)

## Reading

### Read command

e.g. loading `person` table to Dataframe

```scala
val loadedDf = spark.read.format("org.apache.spark.sql.redis").load("person")
loadedDf.show()
```

```
+-----+---+---------------+------+
| name|age|        address|salary|
+-----+---+---------------+------+
| John| 30| 60 Wall Street| 150.5|
|Peter| 35|110 Wall Street| 200.3|
+-----+---+---------------+------+
```

### Spark SQL

```scala
// bind table to temporary view
spark.sql(
      s"""CREATE TEMPORARY VIEW person (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING org.apache.spark.sql.redis OPTIONS (path 'person')
         |""".stripMargin)
val loadedDf = spark.sql(s"SELECT * FROM person")
```

## Writing

### Write command

```scala
df.write.format("org.apache.spark.sql.redis").save("person")
```

### Spark SQL

```scala
// bind temporary view to table
spark.sql(
      s"""CREATE TEMPORARY VIEW person (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING org.apache.spark.sql.redis OPTIONS (path 'person')
         |""".stripMargin)
spark.sql(
      s"""INSERT INTO TABLE person
         |  VALUES ('John', 30, '60 Wall Street', 150.5),
         |    ('Peter', 35, '110 Wall Street', 200.3)
         |""".stripMargin)
```

## Options [Link](configuration.md##Dataframe)

### Infer schema

`inferSchema`. Guess schema from data for known types. If Spark-Redis cannot detect
the type of a column, it will fallback to `String`. Disabled by default.
```scala
val loadedDf = spark.read.format("org.apache.spark.sql.redis")
    .option("inferSchema", true)
    .load("person")
```

### User defined key column

`keyColumn`. By default, Spark-Redis generates UUID identifier for each row to ensure
their uniqueness.
However, you can also provide your own column as key, e.g.
```scala
df.write.format("org.apache.spark.sql.redis")
   .option("keyColumn", "name")
   .save("person")
```
When key collision happens on ```SaveMode.Append```, the former row would
be replaced by the new row.

The chosen column also participates in determining data target host in
Redis cluster.

### Persistent model

`model`. Spark-Redis supports 2 persistent models to allow you choosing among key
metrics like performance, compactness and interoperability with another
Redis friendly frameworks. Default to `hash`
  - `binary`. Spark-Redis will choose the most suitable serialization
  method to effectively store you rows in Redis cluster. If you don't need
  to work with any framework other than Spark, this could be more preferable
  for you.
  - `hash`. The row would be stored in [Redis Hashes](https://redislabs.com/ebook/part-1-getting-started/chapter-1-getting-to-know-redis/1-2-what-redis-data-structures-look-like/1-2-4-hashes-in-redis/).
  It allows you to utilize one of the most performance distributed data
  structures to date. You can also later choose to load only specific fields,
  in some cases that will help you reduce a lot of unnecessary traffic.
```scala
df.write.format("org.apache.spark.sql.redis")
    .option("model", "binary").save("person")
val loadedDf = spark.read.format("org.apache.spark.sql.redis")
    .option("model", "binary")
    .load("person")
```
Note: Your read model should match write model. Otherwise, the behavior
is undetermined.

### Number of data partitions

`numPartitions`. Number of partitions for reading/writing collocation
(in cluster mode).
  - `reading`. Spark-Redis will execute scans on multiple
  hosts (including Redis readonly slaves). Hence, the read performance
  could be improved dramatically. It also supports automatic switch to
  active nodes if some targets were terminated during the reading phase.
  Default to `3`
  - `writing`. Spark-Redis lets you choose how to split your data across
  Redis cluster. Default to Dataframe partitions.

### Data time to live

`ttl`. If you don't want your data persist in Redis cluster forever, you
can specify it time to live in `seconds`. Redis will help you clean up all
your expired data. Default to `unexpired`
