# Python

  - [Getting started](#getting-started)
  - [What is supported](#what-is-supported)
  - [Reading Redis Hashes](#reading-redis-hashes)
  - [Structured Streaming](#structured-streaming)
  - [Writing other Redis data types](#writing-other-redis-data-types)

## Getting started

1. Run `pyspark` with the Spark-Redis package (see [Getting the library](getting-started.md#getting-the-library) for the versions):

```bash
$ ./bin/pyspark --packages com.redislabs:spark-redis_2.12:3.1.0
```

or with a JAR file that you built:

```bash
$ ./bin/pyspark --jars <path-to>/spark-redis_2.12-<version>-jar-with-dependencies.jar
```

By default it connects to `localhost:6379` without any password, you can change the connection settings in the following manner:

```bash
$ ./bin/pyspark --packages com.redislabs:spark-redis_2.12:3.1.0 --conf "spark.redis.host=localhost" --conf "spark.redis.port=6379" --conf "spark.redis.auth=passwd"
```

2. Read DataFrame from JSON, write/read from Redis:
```python
df = spark.read.json("examples/src/main/resources/people.json")
df.write.format("org.apache.spark.sql.redis").option("table", "people").option("key.column", "name").save()
loadedDf = spark.read.format("org.apache.spark.sql.redis").option("table", "people").option("key.column", "name").load()
loadedDf.show()
```

3. Check the data with redis-cli:

```bash
127.0.0.1:6379> hgetall people:Justin
1) "age"
2) "19"
```

The self-contained application can be configured in the following manner:

```python
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("myApp")\
    .config("spark.redis.host", "localhost")\
    .config("spark.redis.port", "6379")\
    .config("spark.redis.auth", "passwd")\
    .getOrCreate()
```

The `spark.redis.*` settings are only applied when the builder creates the `SparkContext`. In the `pyspark` shell and in
notebooks a `SparkContext` already exists, so set them with `--conf` or in the cluster configuration, or pass them as
DataFrame options (`host`, `port`, `auth`, ...). See [When the configuration is read](configuration.md#when-the-configuration-is-read).

## What is supported

From Python you can use everything that goes through the DataFrame API:

* reading and writing DataFrames and Spark SQL tables, see [DataFrame](dataframe.md),
* reading Redis Streams with Structured Streaming, see [Structured Streaming](structured-streaming.md).

The RDD functions (`sc.fromRedisKV`, `sc.toRedisKV`, `sc.toRedisHASH`, ...) and the DStream functions are Scala methods
added to `SparkContext` by `import com.redislabs.provider.redis._`. They are not available from Python, and there is no
Python module to import.

Option values must be strings, numbers or Python booleans: use `True`, not `true`. For example
`.option("infer.schema", True)` or `.option("infer.schema", "true")`.

## Reading Redis Hashes

To read Redis Hashes that were not written by Spark-Redis, provide a key pattern and a schema, or let Spark-Redis infer the
schema from a random Hash:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
])

df = spark.read.format("org.apache.spark.sql.redis")\
    .schema(schema)\
    .option("keys.pattern", "person:*")\
    .option("key.column", "id")\
    .load()

inferred = spark.read.format("org.apache.spark.sql.redis")\
    .option("keys.pattern", "person:*")\
    .option("infer.schema", True)\
    .load()
```

## Structured Streaming

Read a Redis Stream and write each micro-batch to Redis Hashes with `foreachBatch`:

```python
from pyspark.sql.types import StructType, StructField, StringType, FloatType

sensors = spark.readStream\
    .format("redis")\
    .option("stream.keys", "sensors")\
    .schema(StructType([
        StructField("_id", StringType()),
        StructField("sensor-id", StringType()),
        StructField("temperature", FloatType()),
    ]))\
    .load()

def write_to_redis(batch_df, batch_id):
    batch_df.write\
        .format("org.apache.spark.sql.redis")\
        .option("table", "output")\
        .option("key.column", "_id")\
        .mode("append")\
        .save()

query = sensors.writeStream\
    .foreachBatch(write_to_redis)\
    .option("checkpointLocation", "/tmp/checkpoints/sensors")\
    .start()

query.awaitTermination()
```

## Writing other Redis data types

The DataFrame API writes Redis Hashes. To write Strings, Lists, Sets, Sorted Sets or to run other commands from Python,
you can use a Python Redis client such as [redis-py](https://github.com/redis/redis-py) in `foreachPartition`. The client
must be installed on the executors. Use `redis.cluster.RedisCluster` instead of `redis.Redis` for a Redis Cluster.

```python
def write_partition(rows):
    import redis
    r = redis.Redis(host="localhost", port=6379)
    pipe = r.pipeline(transaction=False)
    for i, row in enumerate(rows, 1):
        pipe.set(row["key"], row["value"])
        if i % 1000 == 0:
            pipe.execute()
    pipe.execute()

df.select("key", "value").foreachPartition(write_partition)
```
