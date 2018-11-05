# STANDALONE REDIS NODE
define REDIS_STANDALONE_NODE_CONF
daemonize yes
port 6379
pidfile /tmp/redis_standalone_node_for_spark-redis.pid
logfile /tmp/redis_standalone_node_for_spark-redis.log
save ""
appendonly no
requirepass passwd
endef

# CLUSTER REDIS NODES
define REDIS_CLUSTER_NODE1_CONF
daemonize yes
port 7379
pidfile /tmp/redis_cluster_node1_for_spark-redis.pid
logfile /tmp/redis_cluster_node1_for_spark-redis.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node1_for_spark-redis.conf
endef

define REDIS_CLUSTER_NODE2_CONF
daemonize yes
port 7380
pidfile /tmp/redis_cluster_node2_for_spark-redis.pid
logfile /tmp/redis_cluster_node2_for_spark-redis.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node2_for_spark-redis.conf
endef

define REDIS_CLUSTER_NODE3_CONF
daemonize yes
port 7381
pidfile /tmp/redis_cluster_node3_for_spark-redis.pid
logfile /tmp/redis_cluster_node3_for_spark-redis.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node3_for_spark-redis.conf
endef

export REDIS_STANDALONE_NODE_CONF
export REDIS_CLUSTER_NODE1_CONF
export REDIS_CLUSTER_NODE2_CONF
export REDIS_CLUSTER_NODE3_CONF

start-standalone:
	echo "$$REDIS_STANDALONE_NODE_CONF" | redis-server -


start-cluster:
	echo "$$REDIS_CLUSTER_NODE1_CONF" | redis-server -
	echo "$$REDIS_CLUSTER_NODE2_CONF" | redis-server -
	echo "$$REDIS_CLUSTER_NODE3_CONF" | redis-server -
	redis-cli -p 7380 cluster meet 127.0.0.1 7379 > /dev/null
	redis-cli -p 7381 cluster meet 127.0.0.1 7379 > /dev/null
	slots=$$(seq 0 2047); slots=$$(echo $$slots | tr '\n' ' '); redis-cli -p 7379 cluster addslots $$slots > /dev/null
	slots=$$(seq 2048 3333); slots=$$(echo $$slots | tr '\n' ' '); redis-cli -p 7380 cluster addslots $$slots > /dev/null
	slots=$$(seq 3334 5460); slots=$$(echo $$slots | tr '\n' ' '); redis-cli -p 7379 cluster addslots $$slots > /dev/null
	slots=$$(seq 5461 7777); slots=$$(echo $$slots | tr '\n' ' '); redis-cli -p 7380 cluster addslots $$slots > /dev/null
	slots=$$(seq 7778 9999); slots=$$(echo $$slots | tr '\n' ' '); redis-cli -p 7381 cluster addslots $$slots > /dev/null
	slots=$$(seq 10000 10922); slots=$$(echo $$slots | tr '\n' ' '); redis-cli -p 7380 cluster addslots $$slots > /dev/null
	slots=$$(seq 10923 16383); slots=$$(echo $$slots | tr '\n' ' '); redis-cli -p 7381 cluster addslots $$slots > /dev/null

start:
	make start-standalone
	make start-cluster

stop-standalone:
	kill `cat /tmp/redis_standalone_node_for_spark-redis.pid`

stop-cluster:
	kill `cat /tmp/redis_cluster_node1_for_spark-redis.pid` || true
	kill `cat /tmp/redis_cluster_node2_for_spark-redis.pid` || true
	kill `cat /tmp/redis_cluster_node3_for_spark-redis.pid` || true
	rm -f /tmp/redis_cluster_node1_for_spark-redis.conf
	rm -f /tmp/redis_cluster_node2_for_spark-redis.conf
	rm -f /tmp/redis_cluster_node3_for_spark-redis.conf

stop:
	make stop-standalone
	make stop-cluster

restart:
	make stop
	make start

test:
	make start
	# with --batch-mode maven doesn't print 'Progress: 125/150kB', the progress lines take up 90% of the log and causes
	# Travis build to fail with 'The job exceeded the maximum log length, and has been terminated'
	mvn clean test -B
	make stop

benchmark:
	make start
	mvn clean test -B -Pbenchmark
	make stop

deploy:
	make start
	mvn --batch-mode clean deploy
	make stop

package:
	make start
	mvn --batch-mode clean package
	make stop

.PHONY: test
