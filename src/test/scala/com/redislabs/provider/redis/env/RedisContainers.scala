package com.redislabs.provider.redis.env

import java.net.{InetAddress, ServerSocket}
import java.time.Duration

import scala.util.{Failure, Success, Try}

import org.testcontainers.containers.{FixedHostPortGenericContainer, GenericContainer}
import org.testcontainers.containers.wait.strategy.Wait
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

  private lazy val tls: Try[RedisContainer] = Try {
    val c = new RedisContainer
    copy(c, "containers/standalone-tls.conf", s"$ConfDir/redis.conf")
    copy(c, "containers/users.acl", s"$ConfDir/users.acl")
    Seq("redis.crt", "redis.key", "ca.crt", "redis.dh").foreach { name =>
      copy(c, s"tls/$name", s"$ConfDir/tls/$name")
    }
    c.withExposedPorts(Int.box(TlsPort))
    c.withCommand("redis-server", s"$ConfDir/redis.conf")
    startedWhen(c, "Ready to accept connections")
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

  /** TLS-only Redis with the same ACL users, using the certificates in `src/test/resources/tls`. */
  def tlsPort: Int = if (Enabled) tls.get.getMappedPort(TlsPort) else TlsPort

  /** Port of the first of three cluster primaries, with `default` (no password) and `alice`/`p1pp0`. */
  def clusterPort: Int = if (Enabled) cluster.get._2.head else 7379
}
