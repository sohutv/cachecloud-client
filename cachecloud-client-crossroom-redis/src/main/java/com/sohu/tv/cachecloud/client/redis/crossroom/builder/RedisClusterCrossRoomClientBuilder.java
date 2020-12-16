package com.sohu.tv.cachecloud.client.redis.crossroom.builder;

import com.sohu.tv.cachecloud.client.redis.crossroom.CrossRoomCluster;
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

    public CrossRoomCluster build() {
        return new CrossRoomCluster(majorAppId, majorPipelineCluster, minorAppId, minorPipelineCluster);
    }

    public RedisClusterCrossRoomClientBuilder(long majorAppId, PipelineCluster majorPipelineCluster,
                                              long minorAppId, PipelineCluster minorPipelineCluster) {
        this.majorAppId = majorAppId;
        this.majorPipelineCluster = majorPipelineCluster;
        this.minorAppId = minorAppId;
        this.minorPipelineCluster = minorPipelineCluster;
    }

}
