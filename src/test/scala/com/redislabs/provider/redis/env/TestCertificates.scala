package com.redislabs.provider.redis.env

import java.math.BigInteger
import java.nio.charset.StandardCharsets.US_ASCII
import java.security.{KeyPair, KeyPairGenerator, KeyStore, SecureRandom}
import java.security.cert.X509Certificate
import java.time.{Duration, Instant}
import java.util.{Base64, Date}

import javax.net.ssl.{SSLContext, TrustManagerFactory}

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.{BasicConstraints, ExtendedKeyUsage, Extension, GeneralName, GeneralNames,
  KeyPurposeId, KeyUsage}
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509v3CertificateBuilder}
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder

/**
  * A certificate authority and a server certificate signed by it, for the TLS Redis that `RedisContainers` runs. They
  * are generated in memory, once per test JVM, so the repository holds no private keys and no key file is written on
  * the host: the server's key goes straight into its container.
  *
  * Bouncy Castle builds the certificates, since Java 8, which this build targets, has no public API for that. It only
  * builds them: the keys, the signatures and the TLS connections use the JDK's own providers.
  */
object TestCertificates {

  private val KeyAlgorithm = "RSA"
  private val KeySize = 2048
  private val SignatureAlgorithm = "SHA256withRSA"
  // Long enough for a test JVM that an IDE keeps running, and the certificates start an hour early in case the
  // container's clock is a little behind the host's.
  private val Validity = Duration.ofDays(7)
  private val ClockSkew = Duration.ofHours(1)

  private val random = new SecureRandom()

  private val caName = new X500Name("CN=spark-redis test CA")
  private val caKeys = keyPair()
  private val serverKeys = keyPair()

  /** The CA the server certificate is signed with, and the only one the client trusts (see `clientContext`). */
  val caCertificate: X509Certificate = {
    val builder = certificateBuilder(serial = 1, caName, caKeys)
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
    builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign))
    signedByCa(builder)
  }

  /** The TLS Redis's certificate, for 127.0.0.1 and localhost, the addresses the suites connect to. */
  val serverCertificate: X509Certificate = {
    val builder = certificateBuilder(serial = 2, new X500Name("CN=localhost"), serverKeys)
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false))
    builder.addExtension(Extension.keyUsage, true,
      new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment))
    builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth))
    builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(Array(
      new GeneralName(GeneralName.iPAddress, "127.0.0.1"),
      new GeneralName(GeneralName.dNSName, "localhost"))))
    signedByCa(builder)
  }

  /** PEM files for the server's `tls-cert-file`, `tls-key-file` and `tls-ca-cert-file`. */
  def serverCertificatePem: Array[Byte] = pem("CERTIFICATE", serverCertificate.getEncoded)

  def serverKeyPem: Array[Byte] = pem("PRIVATE KEY", serverKeys.getPrivate.getEncoded)

  def caCertificatePem: Array[Byte] = pem("CERTIFICATE", caCertificate.getEncoded)

  /** A client SSLContext whose truststore, kept in memory, holds the test CA only. */
  def clientContext: SSLContext = {
    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(null, null)
    trustStore.setCertificateEntry("spark-redis-test-ca", caCertificate)
    val trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagers.init(trustStore)
    val context = SSLContext.getInstance("TLS")
    context.init(null, trustManagers.getTrustManagers, random)
    context
  }

  private def keyPair(): KeyPair = {
    val generator = KeyPairGenerator.getInstance(KeyAlgorithm)
    generator.initialize(KeySize, random)
    generator.generateKeyPair()
  }

  private def certificateBuilder(serial: Long, subject: X500Name, keys: KeyPair): JcaX509v3CertificateBuilder = {
    val now = Instant.now()
    new JcaX509v3CertificateBuilder(caName, BigInteger.valueOf(serial), Date.from(now.minus(ClockSkew)),
      Date.from(now.plus(Validity)), subject, keys.getPublic)
  }

  private def signedByCa(builder: JcaX509v3CertificateBuilder): X509Certificate = {
    val signer = new JcaContentSignerBuilder(SignatureAlgorithm).build(caKeys.getPrivate)
    new JcaX509CertificateConverter().getCertificate(builder.build(signer))
  }

  private def pem(kind: String, der: Array[Byte]): Array[Byte] = {
    val body = Base64.getMimeEncoder(64, "\n".getBytes(US_ASCII)).encodeToString(der)
    s"-----BEGIN $kind-----\n$body\n-----END $kind-----\n".getBytes(US_ASCII)
  }
}
