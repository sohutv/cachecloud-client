package com.sohu.tv;

import com.sohu.tv.builder.ClientBuilder;
import com.sohu.tv.builder.crossroom.CrossRoomSentinelConfig;
import com.sohu.tv.builder.crossroom.RedisCrossRoomClientBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.CrossRoomCluster;
import redis.clients.jedis.PipelineCluster;

/**
 * @author wenruiwu
 * @create 2020/12/1 10:55
 * @description
 */
public class BuilderTest {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private PipelineCluster majorPipelineCluster;
    private PipelineCluster minorPipelineCluster;
    private CrossRoomCluster crossRoomCluster;
    private long majorAppId = 11005L;
    private long minorAppId = 11006L;

    @Before
    public void setUp() {
        majorPipelineCluster = ClientBuilder.redisCluster(majorAppId).build();
        minorPipelineCluster = ClientBuilder.redisCluster(minorAppId).build();
    }

    @Test
    public void crossRoomBuilderTest() {
        crossRoomCluster = RedisCrossRoomClientBuilder
                .redisCluster(majorAppId, majorPipelineCluster, minorAppId, minorPipelineCluster)
                .build();
    }

    @Test
    public void crossRoomBuilderWithConfigTest() {
        CrossRoomSentinelConfig crossRoomSentinelConfig = CrossRoomSentinelConfig.builder()
                .setMajorSlowRatioRt(500)
                .setMajorSlowRatioThreshold(0.2D)
                .build();
        crossRoomCluster = RedisCrossRoomClientBuilder
                .redisCluster(majorAppId, majorPipelineCluster, minorAppId, minorPipelineCluster)
                .setSentinelConfig(crossRoomSentinelConfig)
                .build();
    }
}
