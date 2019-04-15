# Using the library in Java

The library is written in Scala and the API is primarily intended to be used with Scala. But you can also use the library with 
Java because of the Scala/Java interoperability. 

## RDD

Please, refer to the detailed documentation of [RDD support](rdd.md) for the full list of available features.
The RDD functions are available in `RedisContext`. Example:

```java
SparkConf sparkConf = new SparkConf()
                            .setAppName("MyApp")
                            .setMaster("local[*]")
                            .set("spark.redis.host", "localhost")
                            .set("spark.redis.port", "6379");

RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);

JavaSparkContext jsc = new JavaSparkContext(sparkConf);
RedisContext redisContext = new RedisContext(jsc.sc());

JavaRDD<Tuple2<String, String>> rdd = jsc.parallelize(Arrays.asList(Tuple2.apply("myKey", "Hello")));
int ttl = 0;

redisContext.toRedisKV(rdd.rdd(), ttl, redisConfig, readWriteConfig);

``` 

## Datasets and DataFrames

The Dataset/DataFrame API is the same in Java and Scala. Please, refer to [DataFrame page](dataframe.md) for details. Here is an
example with Java:

```Java
public class Person {

    private String name;
    private Integer age;

    public Person() {
    }

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public Integer getAge() {
        return age;
    }
    
    public void setAge(Integer age) {
        this.age = age;
    }
}

```

```Java
SparkSession spark = SparkSession
                .builder()
                .appName("MyApp")
                .master("local[*]")
                .config("spark.redis.host", "localhost")
                .config("spark.redis.port", "6379")
                .getOrCreate();

Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                new Person("John", 35),
                new Person("Peter", 40)), Person.class);

df.write()
  .format("org.apache.spark.sql.redis")
  .option("table", "person")
  .option("key.column", "name")
  .mode(SaveMode.Overwrite)
  .save();
```

## Streaming

The following example demonstrates how to create a stream from Redis list `myList`. Please, refer to [Streaming](streaming.md) for more details.

```java
SparkConf sparkConf = new SparkConf()
            .setAppName("MyApp")
            .setMaster("local[*]")
            .set("spark.redis.host", "localhost")
            .set("spark.redis.port", "6379");

JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

RedisConfig redisConfig = new RedisConfig(new RedisEndpoint(sparkConf));

RedisStreamingContext redisStreamingContext = new RedisStreamingContext(jssc.ssc());
String[] keys = new String[]{"myList"};
RedisInputDStream<Tuple2<String, String>> redisStream =
        redisStreamingContext.createRedisStream(keys, StorageLevel.MEMORY_ONLY(), redisConfig);

redisStream.print();

jssc.start();
jssc.awaitTermination();
```