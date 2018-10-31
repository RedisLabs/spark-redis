# DataFrame

  - [Writing](#writing)
  - [Reading](#reading)
  - [DataFrame Options](#dataframe-options)
  - [Known limitations](#known-limitations)


## Writing

### Write command

In order to persist a DataFrame to Redis, specify `org.apache.spark.sql.redis` format and Redis table name with `option("table", tableName)`.
The table name is used to organize Redis keys in a namespace. 

```scala
df.write
  .format("org.apache.spark.sql.redis")
  .option("table", "person")
  .save()
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

    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .save()
  }
}
```

Let's examine the DataFrame in Redis:

```bash
127.0.0.1:6379> keys "person:*"
1) "person:87ed5f22386f4222bad8048327270e69"
2) "person:27e77510a6e546589df64a3caa2245d5"
```

Each row of DataFrame is written as a [Redis Hash](https://redislabs.com/ebook/part-1-getting-started/chapter-1-getting-to-know-redis/1-2-what-redis-data-structures-look-like/1-2-4-hashes-in-redis/) data structure.

```bash
127.0.0.1:6379> hgetall person:87ed5f22386f4222bad8048327270e69
1) "name"
2) "Peter"
3) "age"
4) "45"
```

Spark-redis also writes serialized DataFrame schema:
```bash
127.0.0.1:6379> keys _spark:person:schema
1) "_spark:person:schema"
``` 

It is used by spark-redis internally when reading DataFrame back to Spark memory.

### Specifying Redis key

By default, spark-redis generates UUID identifier for each row to ensure
their uniqueness. However, you can also provide your own column as a key. This is controlled with `key.column` option:

```scala
df.write
  .format("org.apache.spark.sql.redis")
  .option("table", "person")
  .option("key.column", "name")
  .save()
```

The keys in Redis:

```bash
127.0.0.1:6379> keys person:*
1) "person:John"
2) "person:Peter"
```

The keys will not be persisted in Redis hashes

```bash
127.0.0.1:6379> hgetall person:John
1) "age"
2) "30"
```

In order to load the keys back, you also need to specify
the key column parameter while reading

```scala
val df = spark.read
  .format("org.apache.spark.sql.redis")
  .option("table", "person")
  .option("key.column", "name")
  .load()
```

Otherwise, a field with name `_id` of type `String` will be populated

```bash
root
 |-- _id: string (nullable = true)
 |-- age: integer (nullable = false)

+-----+---+
|  _id|age|
+-----+---+
| John| 30|
|Peter| 45|
+-----+---+
```

### Save Modes

Spark-redis supports all DataFrame [SaveMode](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)'s: `Append`, 
`Overwrite`, `ErrorIfExists` and `Ignore`.

Please note, when key collision happens and `SaveMode.Append` is set, the former row is replaced with a new one. 

### Spark SQL

When working with Spark SQL the data can be written to Redis in the following way:

```scala
spark.sql(
      """
        |CREATE TEMPORARY VIEW person (name STRING, age INT)
        |    USING org.apache.spark.sql.redis OPTIONS (table 'person', key.column 'name')
      """.stripMargin)

spark.sql(
      """
        |INSERT INTO TABLE person
        |VALUES ('John', 30),
        |       ('Peter', 45)
      """.stripMargin)
```

### Time to live

If you want to expire your data after certain time, you can specify its time to live in `seconds`. Redis will use 
[Expire](https://redis.io/commands/expire) command to cleanup data. 

For example, expire data after 30 seconds:

```scala
df.write
  .format("org.apache.spark.sql.redis")
  .option("table", "person")
  .option("ttl", 30)
  .save()
```


### Persistence model

By default, DataFrames are persisted as Redis Hashes. It allows to write data with Spark and query from non-Spark environment.
It also enables projection query optimization when only a small subset of columns are selected. On the other hand, there is currently 
a limitation with Hash model - it doesn't support nested DataFrame schema. One option to overcome it is making your DataFrame schema flat.
If it is not possible due to some constraints, you may consider using Binary persistence model.

With the Binary persistence model the DataFrame row is serialized into a byte array and stored as a string in Redis (the default Java Serialization is used).
This implies that storage model is private to spark-redis library and data cannot be easily queried from non-Spark environments. Another drawback 
of Binary model is a larger memory footprint.   

To enable Binary model use `option("model", "binary")`, e.g.

```scala
df.write
  .format("org.apache.spark.sql.redis")
  .option("table", "person")
  .option("key.column", "name")
  .option("model", "binary")
  .save()
```

Note: You should read DataFrame with the same model as it was written.

## Reading

There are two options how you can read a DataFrame:
 - read a DataFrame that was previously saved by spark-redis. The same DataFrame schema is loaded as it was saved.   
 - read pure Redis Hashes providing keys pattern. The DataFrame schema should be explicitly provided or can be inferred from a random row.


### Reading previously saved DataFrame

To read a previously saved DataFrame, specify the table name that was used for saving. Example:

```scala
object DataFrameTests {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("redis-df")
      .setMaster("local[*]")
      .set("spark.redis.host", "localhost")
      .set("spark.redis.port", "6379")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val personSeq = Seq(Person("John", 30), Person("Peter", 45))
    val df = spark.createDataFrame(personSeq)

    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .mode(SaveMode.Overwrite)
      .save()

    val loadedDf = spark.read
                        .format("org.apache.spark.sql.redis")
                        .option("table", "person")
                        .load()
    loadedDf.printSchema()
    loadedDf.show()
  }
}
```

The output is
```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)
 
+-----+---+
| name|age|
+-----+---+
| John| 30|
|Peter| 45|
+-----+---+
```

To read with a Spark SQL:

```scala
spark.sql(
      s"""CREATE TEMPORARY VIEW person (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING org.apache.spark.sql.redis OPTIONS (table 'person')
         |""".stripMargin)
val loadedDf = spark.sql(s"SELECT * FROM person")
```

### Reading Redis Hashes

To read Redis Hashes you have to provide keys pattern with `.option("keys.pattern", keysPattern)` option. The DataFrame schema should be explicitly specified or can be inferred from a random row.

An example of explicit schema:

```scala
 val df = spark.read
               .format("org.apache.spark.sql.redis")
               .schema(
                  StructType(Array(
                    StructField("name", StringType),
                    StructField("age", IntegerType))
                  )
               ) 
               .option("keys.pattern", "person:*")
               .load()
```

Another option is to let spark-redis automatically infer schema based on a random row. In this case all columns will have `String` type. Example:

```scala
    val df = spark.read
                  .format("org.apache.spark.sql.redis")
                  .option("keys.pattern", "person:*")
                  .option("infer.schema", true)
                  .load()
      
    df.printSchema()
```

The output is:
```
root
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- _id: string (nullable = true)
```

Note: If your schema has a field named `_id` or it was inferred. The
Redis key will be stored in that field. Spark Redis will also try to
extract the key based on your pattern. (you can also change the name
of key column, please refer to [Specifying Redis key](#specifying-redis-key))
- if the pattern ends with `*` and it's the only wildcard, all the
trailing value will be extracted, e.g.
    ```scala
    df.show()
    ```
    ```bash
    +-----+---+-----+
    | name|age|  _id|
    +-----+---+-----+
    | John| 30| John|
    |Peter| 45|Peter|
    +-----+---+-----+
    ```
- otherwise, all Redis key will be kept as is, e.g.
    ```scala
    val df = // code ommitted...
                .option("keys.pattern", "p*:*")
                .load()
    df.show()
    ```
    ```bash
    +-----+---+------------+
    | name|age|         _id|
    +-----+---+------------+
    | John| 30| person:John|
    |Peter| 45|person:Peter|
    +-----+---+------------+
    ```

## DataFrame options

| Name              | Description                                                                               | Type                  | Default |
| ----------------- | ------------------------------------------------------------------------------------------| --------------------- | ------- |
| model             | defines Redis model used to persist DataFrame, see [Persistence model](#persistence-model)| `enum [binary, hash]` | `hash`  |
| partitions.number | number of partitions (applies only when reading dataframe)                                | `Int`                 | `3`     |
| key.column        | specify unique column used as a Redis key, by default a key is auto-generated             | `String`              | -       |
| ttl               | data time to live in `seconds`. Data doesn't expire if `ttl` is less than `1`             | `Int`                 | `0`     |
| infer.schema      | infer schema from random row, all columns will have `String` type                         | `Boolean`             | `false` |
| max.pipeline.size | maximum number of commands per pipeline (used to batch commands)                          | `Int`                 | 100     |
| scan.count        | count option of SCAN command (used to iterate over keys)                                  | `Int`                 | 100     |


## Known limitations

 - Nested DataFrame fields are not currently supported with Hash model. Consider making DataFrame schema flat or using Binary persistence model.
 - Key column deserialization relies on pattern prefix, e.g. keysPattern:*, tableName:$key
