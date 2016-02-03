// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

scalaVersion := "2.10.4"

sparkVersion := "1.4.0"

spName := "RedisLabs/spark-redis"

description := "A library for reading and writing data from and to Redis with Apache Spark, for Spark SQL and DataFrames."

// Don't forget to set the version
version := "0.1.1"

homepage := Some(url("https://github.com/RedisLabs/spark-redis"))

// All Spark Packages need a license
licenses := Seq("BSD 3-Clause" -> url("http://opensource.org/licenses/BSD-3-Clause"))

organization := "com.redislabs"

organizationName := "Redis Labs, Inc."

organizationHomepage := Some(url("https://redislabs.com"))

// Add Spark components this package depends on, e.g, "mllib", ....
// sparkComponents ++= Seq("sql", "mllib")

libraryDependencies ++= Seq( "redis.clients" % "jedis" % "2.7.2")

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials") // A file containing credentials
