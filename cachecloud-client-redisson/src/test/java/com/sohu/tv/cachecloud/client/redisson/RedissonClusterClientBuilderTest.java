package com.sohu.tv.cachecloud.client.redisson;

import org.junit.Test;
import org.redisson.api.*;
import org.redisson.config.ClusterServersConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yijunzhang
 */
public class RedissonClusterClientBuilderTest {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void build() {
        long appId = 10828L;
        String password = "d27ab5d0cf779fdb163b753b892bc7f4";
        RedissonClusterClientBuilder redissonClusterClientBuilder = RedissonClientBuilder.redisCluster(appId, password);
        ClusterServersConfig clusterServersConfig = redissonClusterClientBuilder.getClusterServersConfig();
        clusterServersConfig.setConnectTimeout(2000);

        RedissonClient redissonClient = redissonClusterClientBuilder.build();
        String lockKey = "redisson:lock";
        String key = "redisson:atomicLong:1";
        RLock lock = redissonClient.getLock(lockKey);
        RKeys keys = redissonClient.getKeys();
        try {
            lock.lock();
            logger.warn("get lock");
            RAtomicLong atomicLong = redissonClient.getAtomicLong(key);
            RFuture<Long> future = atomicLong.incrementAndGetAsync();
            future.thenAccept(r -> logger.info("result={}", r))
                    .exceptionally(e -> {
                        logger.error(e.getMessage(), e);
                        return null;
                    });

            logger.info("lockKey={}", keys.getType(lockKey));
            logger.info("key={}", keys.getType(key));
        } finally {
            lock.unlock();
            logger.warn("unlock");
        }
        long delete = keys.delete(lockKey, key);
        logger.warn("delete={}", delete);
        redissonClient.shutdown();
    }
}
