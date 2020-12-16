package com.sohu.tv.cachecloud.client.redisson;

import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.SentinelServersConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yijunzhang
 */
public class RedissonSentinelClientBuilderTest {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void build() {
        long appId = 10130L;
        String password = "73b682e6bac4bf7a84e9b7d67cbfb5d2";
        RedissonSentinelClientBuilder redissonSentinelClientBuilder = RedissonClientBuilder
                .redisSentinel(appId, password);
        SentinelServersConfig sentinelServersConfig = redissonSentinelClientBuilder.getSentinelServersConfig();
        sentinelServersConfig.setConnectTimeout(2000);
        
        RedissonClient redissonClient = redissonSentinelClientBuilder.build();
        String key = "redisson:client";
        RBucket<String> bucket = redissonClient.getBucket(key);
        bucket.set("hello:redisson");
        String result = bucket.get();
        logger.info("result={} del={}", result, bucket.delete());
        redissonClient.shutdown();
    }

}
