# Python

Python support is currently limited to DataFrames only. Please, refer to Scala [DataFrame documentation](dataframe.md) 
for the complete list of features.

Here is an example:

1. Run `pyspark` providing the Spark-Redis JAR file:

```bash
$ ./bin/pyspark --jars <path-to>/spark-redis-<version>-jar-with-dependencies.jar
```

By default it connects to `localhost:6379` without any password, you can change the connection settings in the following manner:

```bash
$ bin/pyspark --jars <path-to>/spark-redis-<version>-jar-with-dependencies.jar --conf "spark.redis.host=localhost" --conf "spark.redis.port=6379" --conf "spark.redis.auth=passwd"
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
SparkSession\
    .builder\
    .appName("myApp")\ 
    .config("spark.redis.host", "localhost")\ 
    .config("spark.redis.port", "6379")\
    .config("spark.redis.auth", "passwd")\ 
    .getOrCreate()
```


