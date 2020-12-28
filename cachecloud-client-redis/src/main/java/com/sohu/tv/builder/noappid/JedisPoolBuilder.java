package com.sohu.tv.builder.noappid;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 构造redis单机的builder；
 *
 * Created by wenruiwu
 */
public class JedisPoolBuilder {
    private Logger logger = LoggerFactory.getLogger(JedisPoolBuilder.class);
    /**
     * redis实例
     */
    private final String host;
    /**
     * redis实例密码
     */
    private final int port;
    /**
     * redis实例密码
     */
    private final String password;
    /**
     * 构建锁
     */
    private final Lock lock = new ReentrantLock();
    /**
     * jedis连接池
     */
    private volatile JedisPool jedisPool;
    /**
     * jedis对象池配置
     */
    private GenericObjectPoolConfig poolConfig;
    /**
     * jedis超时时间(单位:毫秒)
     */
    private int timeout = Protocol.DEFAULT_TIMEOUT;

    /**
     * 构造函数package访问域，package外直接构造实例；
     *
     * @param host
     * @param port
     * @param password
     */
    JedisPoolBuilder(final String host, final int port, final String password) {
        this.host = host;
        this.port = port;
        this.password = password;
        poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL * 3);
        poolConfig.setMaxIdle(GenericObjectPoolConfig.DEFAULT_MAX_IDLE * 2);
        poolConfig.setMinIdle(GenericObjectPoolConfig.DEFAULT_MIN_IDLE);
        poolConfig.setJmxNamePrefix("jedis-pool");
    }

    public JedisPool build() {
        if (jedisPool == null) {
            while (true) {
                try {
                    lock.tryLock(10, TimeUnit.SECONDS);
                    if (jedisPool == null) {
                        jedisPool = new JedisPool(poolConfig, host, port, timeout, password);
                        return jedisPool;
                    }
                } catch (Throwable e) {
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
        } else {
            return jedisPool;
        }
    }

    /**
     * 配置
     *
     * @param poolConfig
     * @return
     */
    public JedisPoolBuilder setPoolConfig(GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
        return this;
    }

    /**
     * @param timeout
     * @return
     */
    public JedisPoolBuilder setTimeout(int timeout) {
        this.timeout = compatibleTimeout(timeout);
        return this;
    }

    /**
     * redis操作超时时间:默认2秒
     * 如果timeout小于0 超时:200微秒
     * 如果timeout小于100 超时:timeout*10000微秒
     * 如果timeout大于100 超时:timeout微秒
     */
    private int compatibleTimeout(int paramTimeOut) {
        if (paramTimeOut <= 0) {
            return Protocol.DEFAULT_TIMEOUT;
        } else if (paramTimeOut < 100) {
            return paramTimeOut * 1000;
        } else {
            return paramTimeOut;
        }
    }
}
