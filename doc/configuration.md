The supported configuration keys are:

* `spark.redis.host` - host or IP of the initial node we connect to. The connector will read the cluster
topology from the initial node, so there is no need to provide the rest of the cluster nodes.
* `spark.redis.port` - the inital node's TCP redis port.
* `spark.redis.auth` - the initial node's AUTH password
* `spark.redis.db` - optional DB number. Avoid using this, especially in cluster mode.
