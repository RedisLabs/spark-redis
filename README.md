[![license](https://img.shields.io/github/license/RedisLabs/spark-redis.svg)](https://github.com/RedisLabs/spark-redis)
[![GitHub issues](https://img.shields.io/github/release/RedisLabs/spark-redis.svg)](https://github.com/RedisLabs/spark-redis/releases/latest)
[![Build Status](https://travis-ci.org/RedisLabs/spark-redis.svg)](https://travis-ci.org/RedisLabs/spark-redis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/spark-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/spark-redis)
[![Javadocs](https://www.javadoc.io/badge/com.redislabs/spark-redis.svg)](https://www.javadoc.io/doc/com.redislabs/spark-redis)
<!--[![Codecov](https://codecov.io/gh/RedisLabs/spark-redis/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisLabs/spark-redis)-->

# Spark-Redis
A library for reading and writing data in [Redis](http://redis.io) using [Apache Spark](http://spark.apache.org/).

Spark-Redis provides access to all of Redis' data structures - String, Hash, List, Set and Sorted Set - from Spark as RDDs. It also supports reading and writing with DataFrames and Spark SQL syntax.

The library can be used both with Redis stand-alone as well as clustered databases. When used with Redis cluster, Spark-Redis is aware of its partitioning scheme and adjusts in response to resharding and node failure events.

Spark-Redis also supports Spark Streaming (DStreams) and Structured Streaming.

## Version compatibility and branching

The library has several branches, each corresponds to a different supported Spark version. For example, 'branch-2.3' works with any Spark 2.3.x version.
The master branch contains the recent development for the next release.

| Spark-Redis                                                     | Spark         | Redis            | Supported Scala Versions | 
| ----------------------------------------------------------------| ------------- | ---------------- | ------------------------ |
| [2.4](https://github.com/RedisLabs/spark-redis/tree/branch-2.4) | 2.4.x         | >=2.9.0          | 2.11                     | 
| [2.3](https://github.com/RedisLabs/spark-redis/tree/branch-2.3) | 2.3.x         | >=2.9.0          | 2.11                     | 
| [1.4](https://github.com/RedisLabs/spark-redis/tree/branch-1.4) | 1.4.x         |                  | 2.10                     | 


## Known limitations

* Java, Python and R API bindings are not provided at this time

## Additional considerations
This library is a work in progress so the API may change before the official release.

## Documentation

Please make sure you use documentation from the correct branch ([2.4](https://github.com/RedisLabs/spark-redis/tree/branch-2.4#documentation), [2.3](https://github.com/RedisLabs/spark-redis/tree/branch-2.3#documentation), etc). 

  - [Getting Started](doc/getting-started.md)
  - [RDD](doc/rdd.md)
  - [Dataframe](doc/dataframe.md)
  - [Streaming](doc/streaming.md)
  - [Structured Streaming](doc/structured-streaming.md)
  - [Cluster](doc/cluster.md)
  - [Java](doc/java.md)
  - [Python](doc/python.md)
  - [Configuration](doc/configuration.md)
  - [Dev environment](doc/dev.md)

## Contributing

You're encouraged to contribute to the Spark-Redis project. 

There are two ways you can do so:

### Submit Issues

If you encounter an issue while using the library, please report it via the project's [issues tracker](https://github.com/RedisLabs/spark-redis/issues).

### Author Pull Requests

Code contributions to the Spark-Redis project can be made using [pull requests](https://github.com/RedisLabs/spark-redis/pulls). To submit a pull request:

 1. Fork this project.
 2. Make and commit your changes.
 3. Submit your changes as a pull request.
