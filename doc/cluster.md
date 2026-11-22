### Redis Cluster

Spark-Redis detects whether the initial node (`spark.redis.host`, `spark.redis.port`) runs in cluster mode
(`cluster_enabled:1` in `INFO`). In cluster mode it reads the topology with `CLUSTER SLOTS`, sends each command to the
master that owns the key's hash slot and splits RDD and DataFrame reads by hash slot ranges. You only need to configure one
node. Reads and writes always go to masters, replicas are not used.

### Node addresses

The topology contains the addresses that the cluster nodes announce, which are often private IPs. The driver connects to
the initial node, but the executors (and the driver, for some operations) connect to these announced addresses. Every
Spark node must therefore reach every Redis node at the address that `CLUSTER SLOTS` returns. A job hangs or fails with
`Could not get a resource from the pool` when this isn't the case, for example when:

* you run Spark on a laptop through a VPN or SSH tunnel to a single node of a cluster in a cloud VPC,
* the nodes run in containers or Kubernetes and announce internal addresses (see the `cluster-announce-ip` setting).

Check the addresses with `redis-cli -h <host> -p <port> CLUSTER SLOTS`.

### Connecting to Multiple Redis Clusters/Instances

```scala
def twoEndpointExample ( sc: SparkContext) = {
  val redisConfig1 = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379, auth = "passwd"))
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
