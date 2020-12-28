package com.sohu.tv.builder.noappid;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * redis pipeline cluster builder
 *
 * Created by wenruiwu
 */
public class PipelineClusterBuilder {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * redis实例列表
     */
    private final Set<HostAndPort> nodeList;
    /**
     * redis实例密码
     */
    private final String password;
    /**
     * jedis对象池配置
     */
    private GenericObjectPoolConfig jedisPoolConfig;
    /**
     * jedis集群对象
     */
    private volatile PipelineCluster pipelineCluster;

    /**
     * jedis连接超时(单位:毫秒)
     */
    private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;

    /**
     * jedis读写超时(单位:毫秒)
     */
    private int soTimeout = Protocol.DEFAULT_TIMEOUT;

    /**
     * 节点定位重试次数:默认3次
     */
    private int maxAttempts = 5;

    /**
     * 是否为每个JeidsPool初始化Jedis对象
     */
    private boolean whetherInitIdleJedis = false;

    /**
     * 构建锁
     */
    private final Lock lock = new ReentrantLock();

    /**
     * 构造函数package访问域，package外不能直接构造实例；
     *
     * @param nodeList
     * @param password
     */
    PipelineClusterBuilder(final Set<HostAndPort> nodeList, final String password) {
        this.nodeList = nodeList;
        this.password = password;
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL * 5);
        poolConfig.setMaxIdle(GenericObjectPoolConfig.DEFAULT_MAX_IDLE * 2);
        poolConfig.setMinIdle(GenericObjectPoolConfig.DEFAULT_MAX_IDLE);
        //JedisPool.borrowObject最大等待时间
        poolConfig.setMaxWaitMillis(Protocol.DEFAULT_TIMEOUT);
        poolConfig.setJmxNamePrefix("jedis-pool");
        this.jedisPoolConfig = poolConfig;
    }

    public PipelineCluster build() {
        if (pipelineCluster == null) {
            while (true) {
                try {
                    lock.tryLock(10, TimeUnit.SECONDS);
                    if (pipelineCluster != null) {
                        return pipelineCluster;
                    }
                    pipelineCluster = new PipelineCluster(jedisPoolConfig, nodeList, connectionTimeout, soTimeout,
                            maxAttempts, password, whetherInitIdleJedis);

                    //启动主动刷新集群拓扑线程
                    ClusterAdaptiveRefreshScheduler scheduler = new ClusterAdaptiveRefreshScheduler(pipelineCluster);
                    scheduler.start();

                    return pipelineCluster;
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
            return pipelineCluster;
        }
    }

    /**
     * 设置配置
     *
     * @param jedisPoolConfig
     * @return
     */
    public PipelineClusterBuilder setJedisPoolConfig(GenericObjectPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
        return this;
    }

    /**
     * 兼容老版本参数
     *
     * @param timeout
     * @return
     */
    public PipelineClusterBuilder setTimeout(final int timeout) {
        this.connectionTimeout = compatibleTimeout(timeout);
        this.soTimeout = compatibleTimeout(timeout);
        return this;
    }

    /**
     * 设置jedis连接超时时间
     *
     * @param connectionTimeout
     */
    public PipelineClusterBuilder setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = compatibleTimeout(connectionTimeout);
        return this;
    }

    /**
     * 设置jedis读写超时时间
     *
     * @param soTimeout
     */
    public PipelineClusterBuilder setSoTimeout(int soTimeout) {
        this.soTimeout = compatibleTimeout(soTimeout);
        return this;
    }

    /**
     * 是否为每个JedisPool创建空闲Jedis
     *
     * @param whetherInitIdleJedis
     * @return
     */
    public PipelineClusterBuilder setWhetherInitIdleJedis(boolean whetherInitIdleJedis) {
        this.whetherInitIdleJedis = whetherInitIdleJedis;
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

    /**
     * 兼容老的api
     *
     * @param maxRedirections
     * @return
     */
    public PipelineClusterBuilder setMaxRedirections(final int maxRedirections) {
        return setMaxAttempts(maxRedirections);
    }

    /**
     * 节点定位重试次数:默认5次
     */
    public PipelineClusterBuilder setMaxAttempts(final int maxAttempts) {
        this.maxAttempts = maxAttempts;
        return this;
    }
}
