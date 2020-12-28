package com.sohu.tv.builder.noappid;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * redis sentinel 客户端的builder
 *
 * Created by wenruiwu
 */
public class JedisSentinelPoolBuilder {
    private static Logger logger = LoggerFactory.getLogger(JedisSentinelPoolBuilder.class);

    /**
     * redis sentinel实例集合
     */
    private final Set<String> sentinels;
    /**
     * redis实例密码
     */
    private final String password;
    /**
     * redis-sentinel master name
     */
    private final String masterName;

    /**
     * jedis对象池配置
     */
    private GenericObjectPoolConfig poolConfig;

    /**
     * jedis连接超时(单位:毫秒)
     */
    private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;

    /**
     * jedis读写超时(单位:毫秒)
     */
    private int soTimeout = Protocol.DEFAULT_TIMEOUT;

    /**
     * jedis sentinel连接池
     */
    private volatile JedisSentinelPool sentinelPool;

    /**
     * 构建锁
     */
    private final Lock lock = new ReentrantLock();

    /**
     * 构造函数package访问域，package外不能直接构造实例；
     *
     * @param sentinels
     * @param password
     * @param masterName
     */
    JedisSentinelPoolBuilder(final Set<String> sentinels, final String password, final String masterName) {
        this.sentinels = sentinels;
        this.password = password;
        this.masterName = masterName;
        poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL * 3);
        poolConfig.setMaxIdle(GenericObjectPoolConfig.DEFAULT_MAX_IDLE * 2);
        poolConfig.setMinIdle(GenericObjectPoolConfig.DEFAULT_MIN_IDLE);
        poolConfig.setMaxWaitMillis(Protocol.DEFAULT_TIMEOUT);
        poolConfig.setJmxNamePrefix("jedis-sentinel-pool");
        poolConfig.setJmxEnabled(true);
    }

    public JedisSentinelPool build() {
        if (sentinelPool == null) {
            while (true) {
                try {
                    lock.tryLock(10, TimeUnit.SECONDS);
                    if (sentinelPool == null) {
                        sentinelPool = new JedisSentinelPool(masterName, sentinels, poolConfig, connectionTimeout,
                                soTimeout, password, Protocol.DEFAULT_DATABASE);
                        return sentinelPool;
                    }
                } catch (Throwable e) {//容错
                    logger.error(e.getMessage(), e);
                } finally {
                    lock.unlock();
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(200 + new Random().nextInt(1000));//活锁
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return sentinelPool;
    }

    /**
     * 设置配置参数
     *
     * @param poolConfig
     * @return
     */
    public JedisSentinelPoolBuilder setPoolConfig(GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
        return this;
    }

    /**
     * 设置jedis连接超时时间
     *
     * @param connectionTimeout
     */
    public JedisSentinelPoolBuilder setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    /**
     * 设置jedis读写超时时间
     *
     * @param soTimeout
     */
    public JedisSentinelPoolBuilder setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
        return this;
    }

    /**
     * (兼容老客户端)
     *
     * @param timeout 单位:毫秒
     * @return
     */
    public JedisSentinelPoolBuilder setTimeout(int timeout) {
        //兼容老版本
        this.connectionTimeout = timeout;
        this.soTimeout = timeout;
        return this;
    }
}
