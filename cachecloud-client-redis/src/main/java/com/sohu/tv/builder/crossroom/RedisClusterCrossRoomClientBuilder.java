package com.sohu.tv.builder.crossroom;

import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import redis.clients.jedis.CrossRoomCluster;
import redis.clients.jedis.PipelineCluster;

/**
 * redis跨机房客户端builder
 *
 * @author leifu
 * @Date 2016年4月26日
 * @Time 下午5:01:41
 */
public class RedisClusterCrossRoomClientBuilder {

    /**
     * 主
     */
    private PipelineCluster majorPipelineCluster;

    /**
     * 备
     */
    private PipelineCluster minorPipelineCluster;

    /**
     * 主appid
     */
    private long majorAppId;

    /**
     * 备appid
     */
    private long minorAppId;

    /**
     * sentinel config
     */
    private CrossRoomSentinelConfig sentinelConfig = CrossRoomSentinelConfig.builder().build();


    public CrossRoomCluster build() {
        //加载sentinel规则
        DegradeRuleManager.loadRules(sentinelConfig.getDegradeRules());
        return new CrossRoomCluster(majorAppId, majorPipelineCluster, minorAppId, minorPipelineCluster);
    }

    public RedisClusterCrossRoomClientBuilder setSentinelConfig(CrossRoomSentinelConfig sentinelConfig) {
        this.sentinelConfig = sentinelConfig;
        return this;
    }

    public RedisClusterCrossRoomClientBuilder(long majorAppId, PipelineCluster majorPipelineCluster,
                                              long minorAppId, PipelineCluster minorPipelineCluster) {
        this.majorAppId = majorAppId;
        this.majorPipelineCluster = majorPipelineCluster;
        this.minorAppId = minorAppId;
        this.minorPipelineCluster = minorPipelineCluster;
    }

}
