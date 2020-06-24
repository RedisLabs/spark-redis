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
