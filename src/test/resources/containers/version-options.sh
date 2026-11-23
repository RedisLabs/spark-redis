# Sourced by start-cluster.sh: the redis-server options that depend on the Redis version, so that the same script
# works on every Redis version the tests support (7.4 and later).
#
# Sets CLUSTER_VERSION_OPTIONS.

version=$(redis-server --version | sed -n 's/.* v=\([0-9][0-9]*\)\.\([0-9][0-9]*\)\..*/\1 \2/p')
REDIS_MAJOR=${version% *}
REDIS_MINOR=${version#* }
if [ -z "$REDIS_MAJOR" ] || [ -z "$REDIS_MINOR" ]; then
  echo "version-options.sh: can't read the version from: $(redis-server --version)" >&2
  exit 1
fi

CLUSTER_VERSION_OPTIONS=""
# The cluster bus runs without TLS. Redis 8.2 to 8.10 know this option and default it to "no". Redis 8.12 defaults it
# to "yes" and then refuses to start a node whose bus has no TLS. Earlier versions don't know the option at all, so it
# is passed only from 8.2 on. "no" is safe here because the bus ports never leave the container.
if [ "$REDIS_MAJOR" -gt 8 ] || { [ "$REDIS_MAJOR" -eq 8 ] && [ "$REDIS_MINOR" -ge 2 ]; }; then
  CLUSTER_VERSION_OPTIONS="--cluster-bus-port-protected-mode no"
fi
