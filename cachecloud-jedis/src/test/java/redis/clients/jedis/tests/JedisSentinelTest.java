package redis.clients.jedis.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.tests.utils.JedisSentinelTestUtil;

public class JedisSentinelTest {
  private static final String MASTER_NAME = "sentinel-10.19.121.226-6380";
  private static final String MONITOR_MASTER_NAME = "mymastermonitor";
  private static final String REMOVE_MASTER_NAME = "mymasterremove";
  private static final String FAILOVER_MASTER_NAME = "sentinel-10.19.121.226-6380";
  private static final String MASTER_IP = "10.19.121.73";

//  protected static HostAndPort master = HostAndPortUtil.getRedisServers().get(0);
  private static HostAndPort master = new HostAndPort("10.19.121.226", 6380);
//  protected static HostAndPort slave = HostAndPortUtil.getRedisServers().get(4);
  protected static HostAndPort slave = new HostAndPort("10.19.123.134", 6380);

  protected static HostAndPort sentinel = HostAndPortUtil.getSentinelServers().get(0);

  protected static HostAndPort sentinelForFailover = HostAndPortUtil.getSentinelServers().get(1);
//  protected static HostAndPort masterForFailover = HostAndPortUtil.getRedisServers().get(5);

  @Before
  public void setup() throws InterruptedException {
  }

  @After
  public void clear() throws InterruptedException {
    // New Sentinel (after 2.8.1)
    // when slave promoted to master (slave of no one), New Sentinel force
    // to restore it (demote)
    // so, promote(slaveof) slave to master has no effect, not same to old
    // Sentinel's behavior
    ensureRemoved(MONITOR_MASTER_NAME);
    ensureRemoved(REMOVE_MASTER_NAME);
  }

  @Test
  public void sentinel() {
    Jedis j = new Jedis(sentinel.getHost(), sentinel.getPort());

    try {
      List<Map<String, String>> masters = j.sentinelMasters();

      boolean inMasters = false;
      for (Map<String, String> master : masters)
        if (MASTER_NAME.equals(master.get("name"))) inMasters = true;

      assertTrue(inMasters);

      List<String> masterHostAndPort = j.sentinelGetMasterAddrByName(MASTER_NAME);
      HostAndPort masterFromSentinel = new HostAndPort(masterHostAndPort.get(0),
          Integer.parseInt(masterHostAndPort.get(1)));
      assertEquals(master, masterFromSentinel);

      List<Map<String, String>> slaves = j.sentinelSlaves(MASTER_NAME);
      assertTrue(!slaves.isEmpty());
      assertEquals(master.getPort(), Integer.parseInt(slaves.get(0).get("master-port")));

      // DO NOT RE-RUN TEST TOO FAST, RESET TAKES SOME TIME TO... RESET
      assertEquals(Long.valueOf(1), j.sentinelReset(MASTER_NAME));
      assertEquals(Long.valueOf(0), j.sentinelReset("woof" + MASTER_NAME));
    } finally {
      j.close();
    }
  }

  @Test
  public void sentinelFailover() throws InterruptedException {
    Jedis j = new Jedis(sentinelForFailover.getHost(), sentinelForFailover.getPort());
    Jedis j2 = new Jedis(sentinelForFailover.getHost(), sentinelForFailover.getPort());

    try {
      List<String> masterHostAndPort = j.sentinelGetMasterAddrByName(FAILOVER_MASTER_NAME);
      HostAndPort currentMaster = new HostAndPort(masterHostAndPort.get(0),
          Integer.parseInt(masterHostAndPort.get(1)));

      //will change the role
      JedisSentinelTestUtil.waitForNewPromotedMaster(FAILOVER_MASTER_NAME, j, j2);

      masterHostAndPort = j.sentinelGetMasterAddrByName(FAILOVER_MASTER_NAME);
      HostAndPort newMaster = new HostAndPort(masterHostAndPort.get(0),
          Integer.parseInt(masterHostAndPort.get(1)));

      assertNotEquals(newMaster, currentMaster);
    } finally {
      j.close();
      j2.close();
    }

  }

  @Test
  public void sentinelMonitor() {
    Jedis j = new Jedis(sentinel.getHost(), sentinel.getPort());

    try {
      // monitor new master
      String result = j.sentinelMonitor(MONITOR_MASTER_NAME, MASTER_IP, 6383, 1);
      assertEquals("OK", result);

      // already monitored
      try {
        j.sentinelMonitor(MONITOR_MASTER_NAME, MASTER_IP, 6383, 1);
        fail();
      } catch (JedisDataException e) {
        // pass
      }
    } finally {
      j.close();
    }
  }

  @Test
  public void sentinelRemove() {
    Jedis j = new Jedis(sentinel.getHost(), sentinel.getPort());

    try {
      ensureMonitored(sentinel, REMOVE_MASTER_NAME, MASTER_IP, 6383, 1);

      String result = j.sentinelRemove(REMOVE_MASTER_NAME);
      assertEquals("OK", result);

      // not exist
      try {
        result = j.sentinelRemove(REMOVE_MASTER_NAME);
        assertNotEquals("OK", result);
        fail();
      } catch (JedisDataException e) {
        // pass
      }
    } finally {
      j.close();
    }
  }

  @Test
  public void sentinelSet() {
    Jedis j = new Jedis(sentinel.getHost(), sentinel.getPort());

    try {
      Map<String, String> parameterMap = new HashMap<String, String>();
      parameterMap.put("down-after-milliseconds", String.valueOf(1234));
      parameterMap.put("parallel-syncs", String.valueOf(3));
      parameterMap.put("quorum", String.valueOf(2));
      j.sentinelSet(MASTER_NAME, parameterMap);

      List<Map<String, String>> masters = j.sentinelMasters();
      for (Map<String, String> master : masters) {
        if (master.get("name").equals(MASTER_NAME)) {
          assertEquals(1234, Integer.parseInt(master.get("down-after-milliseconds")));
          assertEquals(3, Integer.parseInt(master.get("parallel-syncs")));
          assertEquals(2, Integer.parseInt(master.get("quorum")));
        }
      }

      parameterMap.put("quorum", String.valueOf(1));
      j.sentinelSet(MASTER_NAME, parameterMap);
    } finally {
      j.close();
    }
  }

  private void ensureMonitored(HostAndPort sentinel, String masterName, String ip, int port,
      int quorum) {
    Jedis j = new Jedis(sentinel.getHost(), sentinel.getPort());
    try {
      j.sentinelMonitor(masterName, ip, port, quorum);
    } catch (JedisDataException e) {
    } finally {
      j.close();
    }
  }

  private void ensureRemoved(String masterName) {
    Jedis j = new Jedis(sentinel.getHost(), sentinel.getPort());
    try {
      j.sentinelRemove(masterName);
    } catch (JedisDataException e) {
    } finally {
      j.close();
    }
  }

}
