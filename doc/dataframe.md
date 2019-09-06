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
    val spark = SparkSession
      .builder()
      .appName("redis-df")
      .master("local[*]")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()

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

Spark-Redis also writes serialized DataFrame schema:
```bash
127.0.0.1:6379> keys _spark:person:schema
1) "_spark:person:schema"
``` 

It is used by Spark-Redis internally when reading DataFrame back to Spark memory.

### Specifying Redis key

By default Spark-Redis generates UUID identifier for each row to ensure
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

### Save Modes

Spark-Redis supports all DataFrame [SaveMode](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)'s: `Append`, 
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

If you want to expire your data after a certain period of time, you can specify its time to live in `seconds`. Redis will use 
[Expire](https://redis.io/commands/expire) command to cleanup data. 

For example, to expire data after 30 seconds:

```scala
df.write
  .format("org.apache.spark.sql.redis")
  .option("table", "person")
  .option("ttl", 30)
  .save()
```

### Persistence model

By default DataFrames are persisted as Redis Hashes. It allows for data to be written with Spark and queried from a non-Spark environment.
It also enables projection query optimization when only a small subset of columns are selected. On the other hand, there is currently 
a limitation with the Hash model - it doesn't support nested DataFrame schemas. One option to overcome this is to make your DataFrame schema flat.
If it is not possible due to some constraints, you may consider using the Binary persistence model.

With the Binary persistence model the DataFrame row is serialized into a byte array and stored as a string in Redis (the default Java Serialization is used).
This implies that storage model is private to Spark-Redis library and data cannot be easily queried from non-Spark environments. Another drawback 
of the Binary model is a larger memory footprint.   

To enable Binary model use `option("model", "binary")`, e.g.

```scala
df.write
  .format("org.apache.spark.sql.redis")
  .option("table", "person")
  .option("key.column", "name")
  .option("model", "binary")
  .save()
```

Note: You should read the DataFrame with the same model as it was written.

## Reading

There are two options for reading a DataFrame:
 - read a DataFrame that was previously saved by Spark-Redis. The same DataFrame schema is loaded as it was saved.   
 - read pure Redis Hashes providing keys pattern. The DataFrame schema should be explicitly provided or can be inferred from a random row.

### Reading previously saved DataFrame

To read a previously saved DataFrame, specify the table name that was used for saving. For example:

```scala
object DataFrameExample {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder()
          .appName("redis-df")
          .master("local[*]")
          .config("spark.redis.host", "localhost")
          .config("spark.redis.port", "6379")
          .getOrCreate()

    val personSeq = Seq(Person("John", 30), Person("Peter", 45)
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

If the `key.column` option was used for writing, then it should be also used for reading table back. See [Specifying Redis Key](#specifying-redis-key) for details.

To read via Spark SQL:

```scala
spark.sql(
      s"""CREATE TEMPORARY VIEW person (name STRING, age INT, address STRING, salary DOUBLE)
         |  USING org.apache.spark.sql.redis OPTIONS (table 'person')
         |""".stripMargin)
val loadedDf = spark.sql(s"SELECT * FROM person")
```

### Reading Redis Hashes

To read Redis Hashes you have to provide a keys pattern with `.option("keys.pattern", keysPattern)` option. The DataFrame schema should be explicitly specified or can be inferred from a random row.

```bash
hset person:1 name John age 30
hset person:2 name Peter age 45
```

An example of providing an explicit schema and specifying `key.column`:

```scala
val df = spark.read
              .format("org.apache.spark.sql.redis")
              .schema(
                StructType(Array(
                  StructField("id", IntegerType),
                  StructField("name", StringType),
                  StructField("age", IntegerType))
                )
              )
              .option("keys.pattern", "person:*")
              .option("key.column", "id")
              .load()
              
df.show()
```

```bash
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1| John| 30|
|  2|Peter| 45|
+---+-----+---+
```

Spark-Redis tries to extract the key based on the key pattern:
- if the pattern ends with `*` and it's the only wildcard, the trailing substring will be extracted
- otherwise there is no extraction - the key is kept as is, e.g.

    ```scala
    val df = // code omitted...
                .option("keys.pattern", "p*:*")
                .option("key.column", "id")
                .load()
    df.show()
    ```

    ```bash
    +-----+---+------------+
    | name|age|          id|
    +-----+---+------------+
    | John| 30| person:John|
    |Peter| 45|person:Peter|
    +-----+---+------------+
    ```

Another option is to let Spark-Redis automatically infer the schema based on a random row. In this case all columns will have `String` type. 
Also we don't specify the `key.column` option in this example, so the column `_id` will be created. 
Example:

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


## DataFrame options

| Name                   | Description                                                                               | Type                  | Default |
| -----------------------| ------------------------------------------------------------------------------------------| --------------------- | ------- |
| model                  | defines the Redis model used to persist DataFrame, see [Persistence model](#persistence-model)| `enum [binary, hash]` | `hash`  |
| filter.keys.by.type    | make sure the underlying data structures match persistence model                          | `Boolean`             | `false` |
| partitions.number      | number of partitions (applies only when reading DataFrame)                                | `Int`                 | `3`     |
| key.column             | when writing - specifies unique column used as a Redis key, by default a key is auto-generated <br/> when reading - specifies column name to store hash key | `String`              | -       |
| ttl                    | data time to live in `seconds`. Data doesn't expire if `ttl` is less than `1`             | `Int`                 | `0`     |
| infer.schema           | infer schema from random row, all columns will have `String` type                         | `Boolean`             | `false` |
| max.pipeline.size      | maximum number of commands per pipeline (used to batch commands)                          | `Int`                 | 100     |
| scan.count             | count option of SCAN command (used to iterate over keys)                                  | `Int`                 | 100     |
| iterator.grouping.size | the number of items to be grouped when iterating over underlying RDD partition            | `Int`                 | 1000    |


## Known limitations

 - Nested DataFrame fields are not currently supported with Hash model. Consider making DataFrame schema flat or using Binary persistence model.
