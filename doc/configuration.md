The supported configuration parameters are:

## Spark Context configuration parameters

* `spark.redis.host` - host or IP of the initial node we connect to. The connector will read the cluster
topology from the initial node, so there is no need to provide the rest of the cluster nodes. For sentinel mode all sentinels should be add comma separated `sentinel1,sentinel2,...`
* `spark.redis.port` - the initial node's TCP redis port.
* `spark.redis.auth` - the initial node's AUTH password
* `spark.redis.db` - optional DB number. Avoid using this, especially in cluster mode.
* `spark.redis.timeout` - connection timeout in ms, 2000 ms by default
* `spark.redis.max.pipeline.size` - the maximum number of commands per pipeline (used to batch commands). The default value is 100.
* `spark.redis.scan.count` - count option of SCAN command (used to iterate over keys). The default value is 100.
* `spark.redis.ssl` - set to true to use tls
* `spark.redis.sentinel.master` - master node name in Sentinel mode
* `spark.redis.sentinel.auth` - the sentinel's password
 



