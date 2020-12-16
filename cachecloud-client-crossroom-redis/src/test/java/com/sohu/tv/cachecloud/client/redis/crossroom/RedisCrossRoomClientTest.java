package com.sohu.tv.cachecloud.client.redis.crossroom;

import com.sohu.tv.builder.ClientBuilder;
import com.sohu.tv.cachecloud.client.redis.crossroom.builder.RedisCrossRoomClientBuilder;
import com.sohu.tv.cachecloud.client.redis.crossroom.command.CrossRoomClusterCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.PipelineCluster;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author wenruiwu
 * @create 2020/11/19 11:13
 * @description
 */
public class RedisCrossRoomClientTest {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private PipelineCluster majorPipelineCluster;
    private PipelineCluster minorPipelineCluster;
    //private RedisCrossRoomClient redisCrossRoomClient;
    private CrossRoomCluster crossRoomCluster;
    private long majorAppId = 11005L;
    private long minorAppId = 11006L;

    @Before
    public void setUp() {
        majorPipelineCluster = ClientBuilder.redisCluster(majorAppId).build();
        minorPipelineCluster = ClientBuilder.redisCluster(minorAppId).build();
        crossRoomCluster = RedisCrossRoomClientBuilder
                .redisCluster(majorAppId, majorPipelineCluster, minorAppId, minorPipelineCluster)
                .build();
    }

    @Test
    public void testBasic() {
        String key1 = "test-key-" + "20201130";
        long start = System.currentTimeMillis();
        String value1 = crossRoomCluster.get(key1);
        logger.info("get key={},value={},cost={} ms", key1, value1, (System.currentTimeMillis() - start));
        Random random = new Random();
        int index = random.nextInt(100);
        String key2 = "test-key-" + "20201130-" + index;
        String value2 = "test-value-" + index;
        start = System.currentTimeMillis();
        crossRoomCluster.set(key2, value2);
        logger.info("set key={}, cost={} ms", key2, (System.currentTimeMillis() - start));

        logger.info("delete key={}, result={}", key2, crossRoomCluster.del(key2));
    }

    @Test
    public void testSetWithResult() throws InterruptedException, ExecutionException, TimeoutException {
        String key1 = "test-key-" + "20201130";
        long start = System.currentTimeMillis();
        String majorResult = crossRoomCluster.set(key1, "hello world");
        Future minorFuture = CrossRoomClusterCommand.threadLocal.get();
        Object minorResult = minorFuture.get(20, TimeUnit.MILLISECONDS);
        logger.info("set key={}, majorResult={}, minorResult={}, cost={} ms", key1, majorResult, minorResult, (System.currentTimeMillis() - start));
    }

    @Test
    public void testDelWithResult() throws InterruptedException, ExecutionException, TimeoutException {
        String key1 = "test-key-" + "20201130";
        long start = System.currentTimeMillis();
        Long majorResult = crossRoomCluster.del(key1);
        Future minorFuture = CrossRoomClusterCommand.threadLocal.get();
        Object minorResult = minorFuture.get(20, TimeUnit.MILLISECONDS);
        logger.info("set key={}, majorResult={}, minorResult={}, cost={} ms", key1, majorResult, minorResult, (System.currentTimeMillis() - start));
    }

    @Test
    public void testRead() {
        int count = 100;
        while (count-- > 0) {
            String key1 = "personal:rc:topiccode:videos:pgc12992";
            long start = System.currentTimeMillis();
            String value1 = crossRoomCluster.get(key1);
            logger.info("get key={},value={},cost={}", key1, value1, (System.currentTimeMillis() - start));
        }
    }

    @After
    public void tearDown() {
        if (majorPipelineCluster != null) {
            majorPipelineCluster.close();
        }
        if (minorPipelineCluster != null) {
            minorPipelineCluster.close();
        }
    }
}
