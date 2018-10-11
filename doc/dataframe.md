# DataFrame

  - [Writing](#writing)
  - [Reading](#reading)
  - [Known limitations](#known-limitations)


## Writing

### Write command

In order to persist a DataFrame to Redis, specify `org.apache.spark.sql.redis` format and Redis table name with `save(tableName)` function.
The table name is used to organize Redis keys in a namespace. 

```scala
df.write.format("org.apache.spark.sql.redis").save("person")
```

Consider the following example:

```scala
object DataFrameExample {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("redis-df")
      .setMaster("local[*]")
      .set("spark.redis.host", "localhost")
      .set("spark.redis.port", "6379")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val personSeq = Seq(Person("John", 30), Person("Peter", 45))
    val df = spark.createDataFrame(personSeq)

    df.write.format("org.apache.spark.sql.redis").save("person")
  }
}
```

Let's examine the DataFrame in Redis:

```bash
127.0.0.1:6379> keys "person:*"
1) "person:r:254feb0701b24e2e97861dd973025fcd"
2) "person:r:224507e8bd5644d6bd80e640e70a466c"
3) "person:schema"
```

Each row of DataFrame is written as a [Redis Hash](https://redislabs.com/ebook/part-1-getting-started/chapter-1-getting-to-know-redis/1-2-what-redis-data-structures-look-like/1-2-4-hashes-in-redis/) data structure.

```bash
127.0.0.1:6379> hgetall person:r:254feb0701b24e2e97861dd973025fcd
1) "name"
2) "John"
3) "age"
4) "30"
```

The `person:schema` contains a serialized DataFrame schema, it is used by spark-redis internally when reading DataFrame back to Spark memory.

### Specifying Redis key

By default, spark-redis generates UUID identifier for each row to ensure
their uniqueness. However, you can also provide your own column as a key. This is controlled with `key.column` option:

```scala
df.write.format("org.apache.spark.sql.redis").option("key.column", "name").save("person")
```

The keys in Redis:

```bash
127.0.0.1:6379> keys "person:*"
1) "person:r:John"
2) "person:schema"
3) "person:r:Peter
```

### Save Modes

Spark-redis supports all DataFrame [SaveMode](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)'s: `Append`, 
`Overwrite`, `ErrorIfExists` and `Ignore`.

Please note, when key collision happens on `SaveMode.Append`, the former row is replaced with a new one. 

### Spark SQL

When working Spark SQL the data can be written to Redis in the following way:

```scala
spark.sql(
      """
        |CREATE TEMPORARY VIEW person (name STRING, age INT)
        |    USING org.apache.spark.sql.redis OPTIONS (path 'person', key.column 'name')
      """.stripMargin)

spark.sql(
      """
        |INSERT INTO TABLE person
        |VALUES ('John', 30),
        |       ('Peter', 45)
      """.stripMargin)
```


### Persistence model

By default, DataFrames are persisted as Redis Hashes. It allows to write data with Spark and query from non-Spark environment.
It also enables projection query optimization when only a small subset of columns are selected. On the other hand, there is currently 
a limitation with Hash model - it doesn't support nested DataFrame schema. One option to overcome it is making your DataFrame schema flat.
If it is not possible due to some constraints, you may consider using Binary persistence model.

With Binary persistence model, the DataFrame row is serialized into a byte array and stored as a string in Redis. This implies that 
storage model is private to spark-redis library and data cannot be easily queried from non-Spark environments. Another drawback 
of Binary model is a larger memory footprint.   

To enable Binary model use `option("model", "binary")`, e.g.

```scala
df.write
  .format("org.apache.spark.sql.redis")
  .option("key.column", "name")
  .option("model", "binary")
  .save("person")
```

Note: You should read DataFrame with the same model as it was written.

## Reading

### Creating DataFrame using read command

e.g. loading `person` table to Dataframe

```scala
val loadedDf = spark.read.format("org.apache.spark.sql.redis").load("person")
loadedDf.show()
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



## DataFrame specific options

| Name              | Description                                                                              | Type                  | Default |
| ----------------- | -----------------------------------------------------------------------------------------| --------------------- | ------- |
| model             | defines Redis model used to persist DataFrame, see [Persistence model](#persistence-model) | `enum [binary, hash]` | `hash`  |
| partitions.number | number of partitions (applies only when reading dataframe)                               | `Int`                 | `3`     |
| key.column        | specify unique column used as a Redis key, by default a key is auto-generated            | `String`              | -       |
| ttl               | data time to live in `seconds`. Doesn't expire if less than `1`                          | `Int`                 | `0`     |
| infer.schema      | guess schema from data, fallback to strings for unknown types                            | `Boolean`             | `false` |



### Infer schema

`inferSchema`. Guess schema from data for known types. If Spark-Redis cannot detect
the type of a column, it will fallback to `String`. Disabled by default.
```scala
val loadedDf = spark.read.format("org.apache.spark.sql.redis")
    .option("inferSchema", true)
    .load("person")
```

### Number of data partitions

`numPartitions`. Number of partitions for reading collocation (in cluster
mode).
  - `reading`. Spark-Redis will execute scans on multiple
  hosts (including Redis readonly slaves). Hence, the read performance
  could be improved dramatically. It also supports automatic switch to
  active nodes if some targets were terminated during the reading phase.
  Default to `3`

### Data time to live

`ttl`. If you don't want your data persist in Redis cluster forever, you
can specify it time to live in `seconds`. Redis will help you clean up all
your expired data. Default to `unexpired`

## Known limitations

 - Nested DataFrame fields are not currently supported with Hash model. Consider making DataFrame schema flat or using Binary persistence model.