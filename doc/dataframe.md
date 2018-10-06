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

### User defined key column

By default, Spark-Redis generates UUID identifier for each row to ensure their uniqueness.
However, you can also provide your own column as key, e.g.
```scala
df.write.format(RedisFormat).option(SqlOptionKeyColumn, "name").save(tableName)
```
When key collision happens on ```SaveMode.Append```, the former row would be replaced by the new row.

## Options [Link](configuration.md##Dataframe)
