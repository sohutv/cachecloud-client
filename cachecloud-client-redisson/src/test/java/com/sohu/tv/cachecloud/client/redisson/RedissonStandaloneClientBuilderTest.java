package com.sohu.tv.cachecloud.client.redisson;

import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.codec.SerializationCodec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yijunzhang
 */
public class RedissonStandaloneClientBuilderTest {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void build() {
        long appId = 10666L;
        String password = "60a8b9e8d495e17606ebf3c9dc003e24";
        RedissonStandaloneClientBuilder redissonStandaloneClientBuilder = RedissonClientBuilder
                .redisStandalone(appId, password);

        Config config = redissonStandaloneClientBuilder.getConfig();
        config.setCodec(new SerializationCodec());

        SingleServerConfig singleServerConfig = redissonStandaloneClientBuilder.getSingleServerConfig();
        singleServerConfig.setConnectTimeout(2000);

        RedissonClient redissonClient = redissonStandaloneClientBuilder.build();
        String key = "redisson:client";
        RBucket<String> bucket = redissonClient.getBucket(key);
        bucket.set("hello:redisson");
        String result = bucket.get();
        logger.info("result={} del={}", result, bucket.delete());
        redissonClient.shutdown();
    }

}
