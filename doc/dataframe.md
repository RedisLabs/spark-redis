# DataFrame

  - [Writing](#writing)
  - [Reading](#reading)
  - [Filters and reading specific keys](#filters-and-reading-specific-keys)
  - [Deleting a table](#deleting-a-table)
  - [Writing a DataFrame as Redis Strings](#writing-a-dataframe-as-redis-strings)
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

Spark-Redis also writes the DataFrame schema:
```bash
127.0.0.1:6379> keys _spark:person:schema
1) "_spark:person:schema"
``` 

It is used by Spark-Redis internally when reading DataFrame back to Spark memory. The schema is stored as Spark's
schema JSON (`StructType.json`), so any Spark and Scala version can read the table back.

Earlier versions of Spark-Redis stored a Java-serialized `StructType` instead. Those schemas can only be read with the
Spark minor version and the Scala version that wrote them: reading one with another version fails with an error that
names the schema key. To read such a table, pass the schema with `.schema(...)`, or read it with the versions that wrote
it. Saving the table again stores its schema as JSON.

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

The values of the key column must be unique. Rows with the same key column value are written to the same Redis key, so
only one of them is kept, and fewer keys than rows end up in Redis.

### Save Modes

Spark-Redis supports all DataFrame [SaveMode](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes)s.
The table's rows are the keys that match `<table>:*`.

| Save mode                   | Behavior                                                                                                                                                                                         |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ErrorIfExists` (default)   | scans for the table's keys and fails with `SaveMode is set to ErrorIfExists and dataframe already exists in Redis and contains data` if there are any; otherwise writes the rows                 |
| `Append`                    | writes the rows without checking for existing data                                                                                                                                              |
| `Overwrite`                 | deletes all the table's keys, then writes the rows                                                                                                                                              |
| `Ignore`                    | scans for the table's keys and writes the rows only if there are none                                                                                                                           |

Please note:

* With `SaveMode.Append`, when a row's key already exists, the new row is merged into the existing Redis hash with `HMSET`.
  Fields of the new row replace the existing fields, but fields that exist only in Redis are kept, for example a column that
  isn't in the new DataFrame or a value that is `null` in the new row (see [Null values](#null-values)). To replace whole
  rows, use `SaveMode.Overwrite` or delete the keys first.
* `SaveMode.Overwrite` isn't atomic: the existing keys are deleted first and the new rows are written afterwards, so readers
  can see an empty or partially written table while the job runs. If the job fails after the delete, the table stays
  incomplete. To switch to new data without a gap, write it to a new table name and switch the readers to it.
* `ErrorIfExists`, `Ignore` and `Overwrite` scan the whole keyspace for `<table>:*` keys before writing, which can take a
  long time on a large database. Use `SaveMode.Append` when you don't need these checks, and see
  [Tuning reads and writes](configuration.md#tuning-reads-and-writes).

### Null values

With the Hash model, `null` values are not written: the field is left out of the row's Redis hash. When you read the table
back with the `table` option, the stored schema is used and missing fields are returned as `null`. When you read Redis
hashes with `infer.schema`, the columns are taken from one random hash, so a column that is `null` in that row is missing
from the DataFrame. Provide the schema explicitly in that case, see [Reading Redis Hashes](#reading-redis-hashes).

### Data types

With the Hash model, every value is stored as a string. When reading, the values are converted back according to the
schema. The following types are supported: `String`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `Boolean`,
`Date` (stored as `yyyy-mm-dd`) and `Timestamp` (stored as `yyyy-mm-dd hh:mm:ss[.fffffffff]`). When reading Redis Hashes
that were written by other clients, `Timestamp` fields also accept ISO-8601 date-times such as `2022-10-14T13:33:15.012345`
or `2022-10-14T13:33:15Z`. Values without an offset are interpreted in the JVM default time zone.

Other types, such as arrays, maps, nested structs or binary columns, are written with their `toString()` value (for example
`WrappedArray(a, b)` or `[B@6d06d69c`) and can't be read back. Convert them first, for example with `to_json()` or
`base64()`, or use the [Binary persistence model](#persistence-model). To store raw bytes that other clients can read, use
the RDD functions `toRedisByteKV`, `toRedisByteHASHes` or `toRedisByteLISTs`, see [RDD](rdd.md#writing-data).

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

The `OPTIONS` clause accepts the same options as the DataFrame API, including the
[connection options](#dataframe-options), without the `spark.redis.` prefix:

```sql
CREATE TEMPORARY VIEW person (name STRING, age INT)
    USING org.apache.spark.sql.redis OPTIONS (table 'person', key.column 'name', host '10.0.0.1', port '6379')
```

`INSERT INTO` appends rows (see [Save Modes](#save-modes)) and `INSERT OVERWRITE` replaces the table.

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

### Filters and reading specific keys

With the Hash model only the selected columns are fetched from Redis (with `HMGET`), but filters such as `WHERE` clauses
or `df.filter(...)` are not pushed down: Spark-Redis scans all the keys of the table or pattern, loads the rows and Spark
filters them afterwards. For selective lookups on large data, design the Redis keys around the value you look up by and
read those keys directly (see below), or use [Redis Query Engine](https://redis.io/docs/latest/develop/interact/search-and-query/)
outside of Spark.

`keys.pattern` can be a single key without wildcards. In that case Spark-Redis reads that key directly, without `SCAN`:

```scala
val df = spark.read
  .format("org.apache.spark.sql.redis")
  .schema(schema)
  .option("keys.pattern", "person:John")
  .load()
```

To read a list of known keys, see [Running custom commands](rdd.md#running-custom-commands) in the RDD documentation.

### Deleting a table

There is no API to drop a table. The table's data is the set of keys that match `<table>:*`, plus the schema key
`_spark:<table>:schema`. You can delete them with the RDD API:

```scala
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.PipelineUtils.foreachWithPipeline
import redis.clients.jedis.PipelineBase

val redisConfig = RedisConfig.fromSparkConf(sc.getConf)
val readWriteConfig = ReadWriteConfig.fromSparkConf(sc.getConf)
val table = "person"

sc.fromRedisKeyPattern(s"$table:*", partitionNum = 12).foreachPartition { keys =>
  implicit val rwConf: ReadWriteConfig = readWriteConfig
  keys.grouped(readWriteConfig.rddWriteIteratorGroupingSize).foreach { batch =>
    // send each key to the node that owns it
    batch.groupBy(key => redisConfig.getHost(key)).foreach { case (node, nodeKeys) =>
      withConnection(node.endpoint.connect()) { conn =>
        foreachWithPipeline(conn, nodeKeys) { (pipeline, key) =>
          (pipeline: PipelineBase).del(key)
        }
      }
    }
  }
}

val schemaKey = s"_spark:$table:schema"
withConnection(redisConfig.connectionForKey(schemaKey)) { conn =>
  conn.del(schemaKey)
}
```

Raise `spark.redis.scan.count` and `spark.redis.max.pipeline.size` to speed it up, see
[Tuning reads and writes](configuration.md#tuning-reads-and-writes). If you need to drop tables regularly, for example one
table per day, consider writing them with the `ttl` option so that Redis expires the keys.

### Writing a DataFrame as Redis Strings

The DataFrame API writes rows as Redis hashes (or as serialized rows with the Binary model). To store one column as the
value of a Redis String, convert the DataFrame to a key-value RDD and use `toRedisKV`:

```scala
import com.redislabs.provider.redis._

val kvRdd = df.select("id", "label").rdd.map(row => (row.get(0).toString, row.get(1).toString))
sc.toRedisKV(kvRdd)
```


## DataFrame options

| Name                   | Description                                                                                                                                                 | Type                  | Default |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------| --------------------- | ------- |
| model                  | defines the Redis model used to persist DataFrame, see [Persistence model](#persistence-model)                                                              | `enum [binary, hash]` | `hash`  |
| filter.keys.by.type    | make sure the underlying data structures match persistence model                                                                                            | `Boolean`             | `false` |
| partitions.number      | number of partitions (applies only when reading DataFrame)                                                                                                  | `Int`                 | `3`     |
| key.column             | when writing - specifies unique column used as a Redis key, by default a key is auto-generated <br/> when reading - specifies column name to store hash key | `String`              | -       |
| ttl                    | data time to live in `seconds`. Data doesn't expire if `ttl` is less than `1`                                                                               | `Int`                 | `0`     |
| infer.schema           | infer schema from random row, all columns will have `String` type                                                                                           | `Boolean`             | `false` |
| max.pipeline.size      | maximum number of commands per pipeline (used to batch commands)                                                                                            | `Int`                 | 100     |
| scan.count             | count option of SCAN command (used to iterate over keys)                                                                                                    | `Int`                 | 100     |
| iterator.grouping.size | the number of rows processed together when reading or writing a partition                                                                                  | `Int`                 | 1000    |
| host                   | overrides `spark.redis.host`                                                                                                                                | `String`              | `localhost`    |
| port                   | overrides `spark.redis.port`                                                                                                                                | `Int`                 | `6379`         |
| user                   | overrides `spark.redis.user`                                                                                                                                | `String`              | -              |
| auth                   | overrides `spark.redis.auth`                                                                                                                                | `String`              | -              |
| ssl                    | overrides `spark.redis.ssl`                                                                                                                                 | `Boolean`             | `false`        |
| dbNum                  | overrides `spark.redis.db`                                                                                                                                  | `Int`                 | `0`            |
| timeout                | overrides `spark.redis.timeout`                                                                                                                             | `Int`                 | `2000`         |
| client.name            | overrides `spark.redis.client.name`: the name this source's connections set with `CLIENT SETNAME`. Empty means no name | `String`              | -              |

`max.pipeline.size`, `scan.count` and `iterator.grouping.size` are explained in
[Tuning reads and writes](configuration.md#tuning-reads-and-writes).

Each connection option (`host`, `port`, `user`, `auth`, `ssl`, `dbNum`, `timeout`, `client.name`) overrides only its own `spark.redis.*`
parameter. The others still come from the Spark configuration. The option names don't have the `spark.redis.` prefix,
see [Configuration](configuration.md#overriding-the-connection-for-a-dataframe-stream-or-rdd).


## Known limitations

 - Nested DataFrame fields are not currently supported with Hash model. Consider making DataFrame schema flat or using Binary persistence model.
 - Filters are not pushed down to Redis, see [Filters and reading specific keys](#filters-and-reading-specific-keys).
 - Writes are not transactional. If Redis rejects a write (for example when `maxmemory` is reached), the task fails, but the
   rows written before the error stay in Redis. See [Redis rejected N of M pipelined commands](troubleshooting.md#redis-rejected-n-of-m-pipelined-commands).
