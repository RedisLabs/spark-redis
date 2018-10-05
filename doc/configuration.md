The supported configuration keys are:

## Context

* `spark.redis.host` - host or IP of the initial node we connect to. The connector will read the cluster
topology from the initial node, so there is no need to provide the rest of the cluster nodes.
* `spark.redis.port` - the inital node's TCP redis port.
* `spark.redis.auth` - the initial node's AUTH password
* `spark.redis.db` - optional DB number. Avoid using this, especially in cluster mode.

## Dataframe

| Name          | Description                                                     | Type                  | Default |
| ------------- | --------------------------------------------------------------- | --------------------- | ------- |
| inferSchema   | Guess schema from data, fallback to strings for unknown types   | `Boolean`             | `false` |
| keyColumn     | Specify unique column, e.g. for selective overriding            | `String`              | -       |
| model         | Persistent model for performance vs interoperability            | `enum [binary, hash]` | `hash`  |
| numPartitions | Number of data partitions                                       | `Int`                 | `3`     |                  |
| ttl           | Data time to live in `seconds`. Doesn't expire if less than `1` | `Int`                 | `0`     |
