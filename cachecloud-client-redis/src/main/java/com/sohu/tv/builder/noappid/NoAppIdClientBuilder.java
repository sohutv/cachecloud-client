package com.sohu.tv.builder.noappid;

import redis.clients.jedis.HostAndPort;

import java.util.Set;

/**
 * 客户端builder
 *
 * @author wenruiwu
 */
public class NoAppIdClientBuilder {

    /**
     * 构造redis cluster的builder
     *
     * @param nodeList
     * @param password
     * @return
     */
    public static PipelineClusterBuilder pipelineClusterBuilder(final Set<HostAndPort> nodeList, final String password) {
        return new PipelineClusterBuilder(nodeList, password);
    }

    /**
     * 构造redis sentinel的builder
     *
     * @param sentinels
     * @param password
     * @param masterName
     * @return
     */
    public static JedisSentinelPoolBuilder jedisSentinelPoolBuilder(final Set<String> sentinels, final String password, final String masterName) {
        return new JedisSentinelPoolBuilder(sentinels, password, masterName);
    }

    /**
     * 构造redis standalone的builder
     *
     * @param host
     * @param port
     * @param password
     * @return
     */
    public static JedisPoolBuilder jedisPoolBuilder(final String host, final int port, final String password) {
        return new JedisPoolBuilder(host, port, password);
    }
}
