The supported configuration parameters are:

## Spark Context configuration parameters

> **NOTE**: Prior to Spark-Redis 2.3.1 the configuration parameters are not prefixed with `spark.`, i.e. use `redis.host` instead of `spark.redis.host`.

* `spark.redis.host` - host or IP of the initial node we connect to. The connector will read the cluster
topology from the initial node, so there is no need to provide the rest of the cluster nodes.
* `spark.redis.port` - the inital node's TCP redis port.
* `spark.redis.auth` - the initial node's AUTH password
* `spark.redis.db` - optional DB number. Avoid using this, especially in cluster mode.
* `spark.redis.timeout` - connection timeout in ms, 2000 ms by default
* `spark.redis.max.pipeline.size` - the maximum number of commands per pipeline (used to batch commands). The default value is 100.
* `spark.redis.scan.count` - count option of SCAN command (used to iterate over keys). The default value is 100.



