#!/bin/sh
# Three-primary Redis Cluster inside one container, for the cluster suites.
#
# Usage: start-cluster.sh <port1> <port2> <port3>
#
# The ports are free host ports chosen by RedisContainers. Each node listens on the same port number inside the
# container as on the host, and announces 127.0.0.1:<port>, so clients on the host can follow the cluster's
# redirects. The cluster bus uses fixed internal ports (17001-17003) that are never published.
# The slot ranges match the old Makefile, so slot-dependent tests behave the same.
#
# Needs Redis 7.4 or later. The options that depend on the version come from version-options.sh.
set -e
[ "$#" -eq 3 ] || { echo "usage: $0 <port1> <port2> <port3>" >&2; exit 2; }
P1=$1; P2=$2; P3=$3
CONF_DIR=/usr/local/etc/redis
. "$CONF_DIR/version-options.sh"
mkdir -p /data/cluster

bus=17001
for port in "$P1" "$P2" "$P3"; do
  redis-server --port "$port" --bind 0.0.0.0 --protected-mode no \
    --cluster-enabled yes --cluster-config-file "/data/cluster/nodes-$port.conf" \
    --cluster-port "$bus" --cluster-announce-ip 127.0.0.1 $CLUSTER_VERSION_OPTIONS \
    --aclfile "$CONF_DIR/users-cluster.acl" \
    --save "" --appendonly no \
    --dir /data/cluster --logfile "/data/cluster/redis-$port.log" --daemonize yes
  bus=$((bus + 1))
done
for port in "$P1" "$P2" "$P3"; do
  until redis-cli -p "$port" ping >/dev/null 2>&1; do sleep 0.1; done
done

redis-cli -p "$P2" cluster meet 127.0.0.1 "$P1" 17001 >/dev/null
redis-cli -p "$P3" cluster meet 127.0.0.1 "$P1" 17001 >/dev/null
redis-cli -p "$P1" cluster addslotsrange 0 2047 3334 5460 >/dev/null
redis-cli -p "$P2" cluster addslotsrange 2048 3333 5461 7777 10000 10922 >/dev/null
redis-cli -p "$P3" cluster addslotsrange 7778 9999 10923 16383 >/dev/null
for port in "$P1" "$P2" "$P3"; do
  until redis-cli -p "$port" cluster info | grep -q 'cluster_state:ok'; do sleep 0.2; done
done

echo "CLUSTER READY"
exec tail -F "/data/cluster/redis-$P1.log" "/data/cluster/redis-$P2.log" "/data/cluster/redis-$P3.log"
