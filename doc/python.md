# Python

The python support is currently limited to DataFrames only. Please, refer to scala [DataFrame documentation](#dataframe.md) 
for the complete list of features.

Here is an example:

1. Run `pyspark` providing the spark-redis jar file 

```bash
$ ./bin/pyspark --jars /your/path/to/spark-redis-<version>-jar-with-dependencies.jar
```

2. Read DataFrame from json, write/read from Redis:
```python
df = spark.read.json("examples/src/main/resources/people.json")
df.write.format("org.apache.spark.sql.redis").option("table", "people").option("key.column", "name").save()
loadedDf = spark.read.format("org.apache.spark.sql.redis").option("table", "people").load()
loadedDf.show()
```

2. Check the data with redis-cli:

```bash
127.0.0.1:6379> hgetall people:Justin
1) "age"
2) "19"
3) "name"
4) "Justin"
```

