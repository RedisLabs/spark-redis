package com.redislabs.provider.redis.env

import java.net.{InetAddress, ServerSocket}
import java.time.Duration

import javax.net.ssl.SSLContext

import scala.util.{Failure, Success, Try}

import org.testcontainers.containers.{FixedHostPortGenericContainer, GenericContainer}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.MountableFile

/**
  * Runs the Redis servers the test suites need, in Docker, on **free host ports chosen at run time**. Each test JVM
  * gets its own servers, so any number of test runs can happen on one machine at the same time. Each server starts
  * the first time a suite asks for its port, and Testcontainers removes it when the JVM exits.
  *
  * The env traits read their ports from here (see `Env.redisPort`), so suites never hard-code a port.
  *
  * System properties:
  *  - `redis.image`: the Redis image to run, Redis 7.4 or later (default `redis:7.4`, see `pom.xml`)
  *  - `redis.containers`: set to `false` to use Redis servers started outside the tests instead, on the old
  *    fixed ports (6379 standalone, 6380 TLS, 7379-7381 cluster)
  */
object RedisContainers {

  val Image: String = sys.props.getOrElse("redis.image", "redis:7.4")

  private val Enabled: Boolean = sys.props.get("redis.containers").forall(_.toBoolean)
  private val ConfDir = "/usr/local/etc/redis"
  private val StartupTimeout = Duration.ofSeconds(90)
  private val ReadOnly: Int = Integer.parseInt("644", 8)
  private val Executable: Int = Integer.parseInt("755", 8)
  private val StandalonePort = 6379
  private val TlsPort = 6380
  private val ClusterNodes = 3
  private val ClusterStartAttempts = 3

  private class RedisContainer extends GenericContainer[RedisContainer](Image)

  /** The cluster's nodes announce their own port numbers to clients, so container and host ports must match. */
  private class RedisClusterContainer extends FixedHostPortGenericContainer[RedisClusterContainer](Image)

  private def copy(container: GenericContainer[_], resource: String, target: String, mode: Int = ReadOnly): Unit = {
    container.withCopyFileToContainer(MountableFile.forClasspathResource(resource, mode), target)
  }

  private def startedWhen[C <: GenericContainer[C]](container: C, logLine: String): C = {
    container.waitingFor(Wait.forLogMessage(s".*$logLine.*\\n", 1).withStartupTimeout(StartupTimeout))
    container.start()
    container
  }

  /** Ports the OS reports as free right now. Another process could take one before Docker binds it. */
  private def freePorts(count: Int): Seq[Int] = {
    val sockets = (1 to count).map(_ => new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")))
    try sockets.map(_.getLocalPort) finally sockets.foreach(_.close())
  }

  // Each server is started once per JVM, and the outcome is kept even when the start failed. A plain lazy val would
  // run its initializer again on every access after a failure, so each suite would wait out StartupTimeout (and the
  // cluster's retries) again: with 22 cluster suites that turned a broken image into a run of about 100 minutes.

  private lazy val standalone: Try[RedisContainer] = Try {
    val c = new RedisContainer
    copy(c, "containers/standalone.conf", s"$ConfDir/redis.conf")
    copy(c, "containers/users.acl", s"$ConfDir/users.acl")
    c.withExposedPorts(Int.box(StandalonePort))
    c.withCommand("redis-server", s"$ConfDir/redis.conf")
    startedWhen(c, "Ready to accept connections")
  }

  // A Redis that reports itself as a replica of the standalone server. It is told to replicate 127.0.0.1 on the
  // standalone server's host port, which nothing listens on inside its own container, so its replication link stays
  // down. INFO replication still reports role:slave with that master address, which is all a RedisConfig reads to
  // find the master, and from the test JVM that address is the standalone server.
  private lazy val replica: Try[RedisContainer] = Try {
    val c = new RedisContainer
    copy(c, "containers/standalone.conf", s"$ConfDir/redis.conf")
    copy(c, "containers/users.acl", s"$ConfDir/users.acl")
    c.withExposedPorts(Int.box(StandalonePort))
    c.withCommand("redis-server", s"$ConfDir/redis.conf", "--replicaof", "127.0.0.1", standalonePort.toString)
    startedWhen(c, "Ready to accept connections")
  }

  // The certificates are generated for this JVM (see TestCertificates) and handed to the container from memory. Then
  // the JVM's default SSLContext, which Spark-Redis connects with when `ssl` is set, trusts their CA only.
  private lazy val tls: Try[RedisContainer] = Try {
    val c = new RedisContainer
    copy(c, "containers/standalone-tls.conf", s"$ConfDir/redis.conf")
    copy(c, "containers/users.acl", s"$ConfDir/users.acl")
    Seq(
      "redis.crt" -> TestCertificates.serverCertificatePem,
      "redis.key" -> TestCertificates.serverKeyPem,
      "ca.crt" -> TestCertificates.caCertificatePem
    ).foreach { case (name, pem) =>
      c.withCopyToContainer(Transferable.of(pem, ReadOnly), s"$ConfDir/tls/$name")
    }
    c.withExposedPorts(Int.box(TlsPort))
    c.withCommand("redis-server", s"$ConfDir/redis.conf")
    startedWhen(c, "Ready to accept connections")
    SSLContext.setDefault(TestCertificates.clientContext)
    c
  }

  private def startCluster(ports: Seq[Int]): RedisClusterContainer = {
    val c = new RedisClusterContainer
    copy(c, "containers/users-cluster.acl", s"$ConfDir/users-cluster.acl")
    copy(c, "containers/version-options.sh", s"$ConfDir/version-options.sh")
    copy(c, "containers/start-cluster.sh", s"$ConfDir/start-cluster.sh", Executable)
    ports.foreach(port => c.withFixedExposedPort(port, port))
    c.withCommand(Seq("sh", s"$ConfDir/start-cluster.sh") ++ ports.map(_.toString): _*)
    startedWhen(c, "CLUSTER READY")
  }

  /** The cluster's node ports, with a few attempts in case another process takes a chosen port first. */
  private lazy val cluster: Try[(RedisClusterContainer, Seq[Int])] = {
    def attempt(left: Int): (RedisClusterContainer, Seq[Int]) = {
      val ports = freePorts(ClusterNodes)
      Try(startCluster(ports)) match {
        case Success(container) => (container, ports)
        case Failure(_) if left > 1 => attempt(left - 1)
        case Failure(e) => throw e
      }
    }
    Try(attempt(ClusterStartAttempts))
  }

  /** Standalone Redis, with ACL users `default`/`passwd` and `alice`/`p1pp0`. */
  def standalonePort: Int = if (Enabled) standalone.get.getMappedPort(StandalonePort) else StandalonePort

  /**
    * A Redis that reports itself as a replica of the standalone server (see `replica`), with the same ACL users. None
    * with `redis.containers=false`, since no such server exists then.
    */
  def replicaPort: Option[Int] = if (Enabled) Some(replica.get.getMappedPort(StandalonePort)) else None

  /**
    * TLS-only Redis with the same ACL users, whose certificate the JVM's default SSLContext trusts once this returns
    * (see `tls`). With `redis.containers=false` the default SSLContext is left as it is, so the test JVM needs a
    * truststore for that server's certificate (see `test.jvm.options` in `pom.xml`).
    */
  def tlsPort: Int = if (Enabled) tls.get.getMappedPort(TlsPort) else TlsPort

  /** Port of the first of three cluster primaries, with `default` (no password) and `alice`/`p1pp0`. */
  def clusterPort: Int = if (Enabled) cluster.get._2.head else 7379
}
