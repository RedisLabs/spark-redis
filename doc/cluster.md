### Connecting to Multiple Redis Clusters/Instances

```scala
def twoEndpointExample ( sc: SparkContext) = {
  val redisConfig1 = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379, "passwd"))
  val redisConfig2 = new RedisConfig(new RedisEndpoint("127.0.0.1", 7379))
  val rddFromEndpoint1 = {
    // endpoint("127.0.0.1", 6379) as the default connection in this block
    implicit val c = redisConfig1
    sc.fromRedisKV("*")
  }
  val rddFromEndpoint2 = {
    // endpoint("127.0.0.1", 7379) as the default connection in this block
    implicit val c = redisConfig2
    sc.fromRedisKV("*")
  }
}
```
If you want to use multiple Redis clusters/instances, an implicit RedisConfig can be used in a code block to specify the target cluster/instance.

### Connecting to Sentinels
#### Using parameters
```scala
df
  .option("table", "table")
  .option("key.column", "key")
  .option("host", "host1,host2,host3")
  .option("port", "6000")
  .option("dbNum", "0")
  .option("timeout", "2000")
  .option("auth", "pwd")
  .option("ssl", "true")
  .option("sentinel.master", "mymaster")
  .option("sentinel.auth", "sentinelPwd")
```

#### Using sparkContext
```scala
val spark = SparkSession
  .builder()
  .appName("myApp")
  .master("local[*]")
  .config("spark.redis.host", "host1,host2,host3")
  .config("spark.redis.port", "6000")
  .config("spark.redis.auth", "passwd")
  .config("spark.redis.ssl", "true")
  .config("spark.redis.sentinel.master", "mymaster")
  .config("spark.redis.sentinel.auth", "sentinelPwd")
  .getOrCreate()
  
val sc = spark.sparkContext  
```
