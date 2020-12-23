package redis.clients.jedis.tests;

import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static org.junit.Assert.*;

public class SSLJedisTest {

  @BeforeClass
  public static void setupTrustStore() {
    setJvmTrustStore("src/test/resources/truststore.jceks", "jceks");
  }

  private static void setJvmTrustStore(String trustStoreFilePath, String trustStoreType) {
    assertTrue(String.format("Could not find trust store at '%s'.", trustStoreFilePath),
        new File(trustStoreFilePath).exists());
    System.setProperty("javax.net.ssl.trustStore", trustStoreFilePath);
    System.setProperty("javax.net.ssl.trustStoreType", trustStoreType);
  }

  /**
   * Tests opening a default SSL/TLS connection to redis using "rediss://" scheme url.
   */
  @Test
  public void connectWithUrl() {
    // The "rediss" scheme instructs jedis to open a SSL/TLS connection.
    Jedis jedis = new Jedis("rediss://127.0.0.1:6380");
    jedis.auth("foobared");
    assertEquals("PONG", jedis.ping());
    jedis.close();
  }

  /**
   * Tests opening a default SSL/TLS connection to redis.
   */
  @Test
  public void connectWithoutShardInfo() {
    // The "rediss" scheme instructs jedis to open a SSL/TLS connection.
    Jedis jedis = new Jedis(URI.create("rediss://127.0.0.1:6380"));
    jedis.auth("foobared");
    assertEquals("PONG", jedis.ping());
    jedis.close();
  }

  /**
   * Tests opening an SSL/TLS connection to redis.
   * NOTE: This test relies on a feature that is only available as of Java 7 and later.
   * It is commented out but not removed in case support for Java 6 is dropped or
   * we find a way to have the CI run a specific set of tests on Java 7 and above.
   */
  @Test
  public void connectWithShardInfo() throws Exception {
    final URI uri = URI.create("rediss://127.0.0.1:6380");
    final SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
    // These SSL parameters ensure that we use the same hostname verifier used
    // for HTTPS.
    // Note: this options is only available in Java 7.
    final SSLParameters sslParameters = new SSLParameters();
    sslParameters.setEndpointIdentificationAlgorithm("HTTPS");

    JedisShardInfo shardInfo = new JedisShardInfo(uri, sslSocketFactory, sslParameters, null);
    shardInfo.setPassword("foobared");

    Jedis jedis = new Jedis(shardInfo);
    assertEquals("PONG", jedis.ping());
    jedis.disconnect();
    jedis.close();
  }

  /**
   * Tests opening an SSL/TLS connection to redis using the loopback address of
   * 127.0.0.1. This test should fail because "127.0.0.1" does not match the
   * certificate subject common name and there are no subject alternative names
   * in the certificate.
   * 
   * NOTE: This test relies on a feature that is only available as of Java 7 and later.
   * It is commented out but not removed in case support for Java 6 is dropped or
   * we find a way to have the CI run a specific set of tests on Java 7 and above.
   */
  @Test
  public void connectWithShardInfoByIpAddress() throws Exception {
    final URI uri = URI.create("rediss://127.0.0.1:6380");
    final SSLSocketFactory sslSocketFactory = createTrustStoreSslSocketFactory();
    // These SSL parameters ensure that we use the same hostname verifier used
    // for HTTPS.
    // Note: this options is only available in Java 7.
    final SSLParameters sslParameters = new SSLParameters();
    sslParameters.setEndpointIdentificationAlgorithm("HTTPS");

    JedisShardInfo shardInfo = new JedisShardInfo(uri, sslSocketFactory, sslParameters, null);
    shardInfo.setPassword("foobared");

    Jedis jedis = new Jedis(shardInfo);
    try {
      assertEquals("PONG", jedis.ping());
      fail("The code did not throw the expected JedisConnectionException.");
    } catch (JedisConnectionException e) {
      assertEquals("Unexpected first inner exception.",
          SSLHandshakeException.class, e.getCause().getClass());
      assertEquals("Unexpected second inner exception.",
          CertificateException.class, e.getCause().getCause().getClass());
    }

    try {
      jedis.close();
    } catch (Throwable e1) {
      // Expected.
    }
  }

  /**
   * Tests opening an SSL/TLS connection to redis with a custom hostname
   * verifier.
   */
  @Test
  public void connectWithShardInfoAndCustomHostnameVerifier() {
    final URI uri = URI.create("rediss://127.0.0.1:6380");
    final SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
    final SSLParameters sslParameters = new SSLParameters();

    HostnameVerifier hostnameVerifier = new BasicHostnameVerifier();
    JedisShardInfo shardInfo = new JedisShardInfo(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    shardInfo.setPassword("foobared");

    Jedis jedis = new Jedis(shardInfo);
    assertEquals("PONG", jedis.ping());
    jedis.disconnect();
    jedis.close();
  }

  /**
   * Tests opening an SSL/TLS connection to redis with a custom socket factory.
   */
  @Test
  public void connectWithShardInfoAndCustomSocketFactory() throws Exception {
    final URI uri = URI.create("rediss://127.0.0.1:6380");
    final SSLSocketFactory sslSocketFactory = createTrustStoreSslSocketFactory();
    final SSLParameters sslParameters = new SSLParameters();

    HostnameVerifier hostnameVerifier = new BasicHostnameVerifier();
    JedisShardInfo shardInfo = new JedisShardInfo(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    shardInfo.setPassword("foobared");

    Jedis jedis = new Jedis(shardInfo);
    assertEquals("PONG", jedis.ping());
    jedis.disconnect();
    jedis.close();
  }

  /**
   * Tests opening an SSL/TLS connection to redis with a custom hostname
   * verifier. This test should fail because "127.0.0.1" does not match the
   * certificate subject common name and there are no subject alternative names
   * in the certificate.
   */
  @Test
  public void connectWithShardInfoAndCustomHostnameVerifierByIpAddress() {
    final URI uri = URI.create("rediss://127.0.0.1:6380");
    final SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
    final SSLParameters sslParameters = new SSLParameters();

    HostnameVerifier hostnameVerifier = new BasicHostnameVerifier();
    JedisShardInfo shardInfo = new JedisShardInfo(uri, sslSocketFactory, sslParameters, hostnameVerifier);
    shardInfo.setPassword("foobared");

    Jedis jedis = new Jedis(shardInfo);
    try {
      assertEquals("PONG", jedis.ping());
      fail("The code did not throw the expected JedisConnectionException.");
    } catch (JedisConnectionException e) {
      assertEquals("The JedisConnectionException does not contain the expected message.",
          "The connection to '127.0.0.1' failed ssl/tls hostname verification.", e.getMessage());
    }

    try {
      jedis.close();
    } catch (Throwable e1) {
      // Expected.
    }
  }

  /**
   * Tests opening an SSL/TLS connection to redis with an empty certificate
   * trust store. This test should fail because there is no trust anchor for the
   * redis server certificate.
   * 
   * @throws Exception
   */
  @Test
  public void connectWithShardInfoAndEmptyTrustStore() throws Exception {

    final URI uri = URI.create("rediss://127.0.0.1:6380");
    final SSLSocketFactory sslSocketFactory = createTrustNoOneSslSocketFactory();

    JedisShardInfo shardInfo = new JedisShardInfo(uri, sslSocketFactory, null, null);
    shardInfo.setPassword("foobared");

    Jedis jedis = new Jedis(shardInfo);
    try {
      assertEquals("PONG", jedis.ping());
      fail("The code did not throw the expected JedisConnectionException.");
    } catch (JedisConnectionException e) {
      assertEquals("Unexpected first inner exception.", SSLException.class,
          e.getCause().getClass());
      assertEquals("Unexpected second inner exception.", RuntimeException.class,
          e.getCause().getCause().getClass());
      assertEquals("Unexpected third inner exception.", InvalidAlgorithmParameterException.class,
          e.getCause().getCause().getCause().getClass());
    }

    try {
      jedis.close();
    } catch (Throwable e1) {
      // Expected.
    }
  }

  /**
   * Creates an SSLSocketFactory that trusts all certificates in
   * truststore.jceks.
   */
  static SSLSocketFactory createTrustStoreSslSocketFactory() throws Exception {

    KeyStore trustStore = KeyStore.getInstance("jceks");
    InputStream inputStream = null;
    try {
      inputStream = new FileInputStream("src/test/resources/truststore.jceks");
      trustStore.load(inputStream, null);
    } finally {
      inputStream.close();
    }

    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("PKIX");
    trustManagerFactory.init(trustStore);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagers, new SecureRandom());
    return sslContext.getSocketFactory();
  }

  /**
   * Creates an SSLSocketFactory with a trust manager that does not trust any
   * certificates.
   */
  static SSLSocketFactory createTrustNoOneSslSocketFactory() throws Exception {
    TrustManager[] unTrustManagers = new TrustManager[] {
      new X509TrustManager() {
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }
  
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
          throw new RuntimeException(new InvalidAlgorithmParameterException());
        }
  
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
          throw new RuntimeException(new InvalidAlgorithmParameterException());
        }
      }
    };
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, unTrustManagers, new SecureRandom());
    return sslContext.getSocketFactory();
  }

  /**
   * Very basic hostname verifier implementation for testing. NOT recommended
   * for production.
   * 
   */
  static class BasicHostnameVerifier implements HostnameVerifier {

    private static final String COMMON_NAME_RDN_PREFIX = "CN=";

    @Override
    public boolean verify(String hostname, SSLSession session) {
      X509Certificate peerCertificate;
      try {
        peerCertificate = (X509Certificate) session.getPeerCertificates()[0];
      } catch (SSLPeerUnverifiedException e) {
        throw new IllegalStateException("The session does not contain a peer X.509 certificate.",  e);
      }
      String peerCertificateCN = getCommonName(peerCertificate);
      return hostname.equals(peerCertificateCN);
    }

    private String getCommonName(X509Certificate peerCertificate) {
      String subjectDN = peerCertificate.getSubjectDN().getName();
      String[] dnComponents = subjectDN.split(",");
      for (String dnComponent : dnComponents) {
        dnComponent = dnComponent.trim();
        if (dnComponent.startsWith(COMMON_NAME_RDN_PREFIX)) {
          return dnComponent.substring(COMMON_NAME_RDN_PREFIX.length());
        }
      }
      throw new IllegalArgumentException("The certificate has no common name.");
    }
  }
}
