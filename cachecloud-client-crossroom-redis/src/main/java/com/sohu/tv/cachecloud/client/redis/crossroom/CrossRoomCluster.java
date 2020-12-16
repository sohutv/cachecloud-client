package com.sohu.tv.cachecloud.client.redis.crossroom;

import com.sohu.tv.cachecloud.client.redis.crossroom.command.CrossRoomClusterCommand;
import com.sohu.tv.cachecloud.client.redis.crossroom.entity.RedisCrossRoomTopology;
import com.sohu.tv.cachecloud.client.redis.crossroom.enums.OpEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.JedisClusterCommands;
import redis.clients.jedis.commands.JedisClusterScriptingCommands;
import redis.clients.jedis.commands.MultiKeyJedisClusterCommands;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wenruiwu
 */
public class CrossRoomCluster implements JedisClusterCommands, MultiKeyJedisClusterCommands, JedisClusterScriptingCommands {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
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

    public CrossRoomCluster(long majorAppId, PipelineCluster majorPipelineCluster,
                            long minorAppId, PipelineCluster minorPipelineCluster) {
        this.majorAppId = majorAppId;
        this.majorPipelineCluster = majorPipelineCluster;
        this.minorAppId = minorAppId;
        this.minorPipelineCluster = minorPipelineCluster;
    }

    @Override
    public String get(final String key) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.get(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("get key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String set(final String key, final String value) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.set(key, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("set key %s value %s", key, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long del(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.del(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("del key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Boolean exists(String key) {
        return null;
    }

    @Override
    public Long persist(String key) {
        return null;
    }

    @Override
    public String type(String key) {
        return null;
    }

    @Override
    public byte[] dump(String key) {
        return new byte[0];
    }

    @Override
    public String restore(String key, int ttl, byte[] serializedValue) {
        return null;
    }

    @Override
    public Long expire(String key, int seconds) {
        return null;
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        return null;
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return null;
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        return null;
    }

    @Override
    public Long ttl(String key) {
        return null;
    }

    @Override
    public Long pttl(String key) {
        return null;
    }

    @Override
    public Long touch(String key) {
        return null;
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return null;
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        return null;
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return null;
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return null;
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return null;
    }

    @Override
    public String getSet(String key, String value) {
        return null;
    }

    @Override
    public Long setnx(String key, String value) {
        return null;
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return null;
    }

    @Override
    public String psetex(String key, long milliseconds, String value) {
        return null;
    }

    @Override
    public Long decrBy(String key, long decrement) {
        return null;
    }

    @Override
    public Long decr(String key) {
        return null;
    }

    @Override
    public Long incrBy(String key, long increment) {
        return null;
    }

    @Override
    public Double incrByFloat(String key, double increment) {
        return null;
    }

    @Override
    public Long incr(String key) {
        return null;
    }

    @Override
    public Long append(String key, String value) {
        return null;
    }

    @Override
    public String substr(String key, int start, int end) {
        return null;
    }

    @Override
    public Long hset(String key, String field, String value) {
        return null;
    }

    @Override
    public Long hset(String key, Map<String, String> hash) {
        return null;
    }

    @Override
    public String hget(String key, String field) {
        return null;
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return null;
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return null;
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return null;
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return null;
    }

    @Override
    public Boolean hexists(String key, String field) {
        return null;
    }

    @Override
    public Long hdel(String key, String... field) {
        return null;
    }

    @Override
    public Long hlen(String key) {
        return null;
    }

    @Override
    public Set<String> hkeys(String key) {
        return null;
    }

    @Override
    public List<String> hvals(String key) {
        return null;
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return null;
    }

    @Override
    public Long rpush(String key, String... string) {
        return null;
    }

    @Override
    public Long lpush(String key, String... string) {
        return null;
    }

    @Override
    public Long llen(String key) {
        return null;
    }

    @Override
    public List<String> lrange(String key, long start, long stop) {
        return null;
    }

    @Override
    public String ltrim(String key, long start, long stop) {
        return null;
    }

    @Override
    public String lindex(String key, long index) {
        return null;
    }

    @Override
    public String lset(String key, long index, String value) {
        return null;
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return null;
    }

    @Override
    public String lpop(String key) {
        return null;
    }

    @Override
    public String rpop(String key) {
        return null;
    }

    @Override
    public Long sadd(String key, String... member) {
        return null;
    }

    @Override
    public Set<String> smembers(String key) {
        return null;
    }

    @Override
    public Long srem(String key, String... member) {
        return null;
    }

    @Override
    public String spop(String key) {
        return null;
    }

    @Override
    public Set<String> spop(String key, long count) {
        return null;
    }

    @Override
    public Long scard(String key) {
        return null;
    }

    @Override
    public Boolean sismember(String key, String member) {
        return null;
    }

    @Override
    public String srandmember(String key) {
        return null;
    }

    @Override
    public List<String> srandmember(String key, int count) {
        return null;
    }

    @Override
    public Long strlen(String key) {
        return null;
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return null;
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        return null;
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return null;
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
        return null;
    }

    @Override
    public Set<String> zrange(String key, long start, long stop) {
        return null;
    }

    @Override
    public Long zrem(String key, String... members) {
        return null;
    }

    @Override
    public Double zincrby(String key, double increment, String member) {
        return null;
    }

    @Override
    public Double zincrby(String key, double increment, String member, ZIncrByParams params) {
        return null;
    }

    @Override
    public Long zrank(String key, String member) {
        return null;
    }

    @Override
    public Long zrevrank(String key, String member) {
        return null;
    }

    @Override
    public Set<String> zrevrange(String key, long start, long stop) {
        return null;
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long stop) {
        return null;
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long stop) {
        return null;
    }

    @Override
    public Long zcard(String key) {
        return null;
    }

    @Override
    public Double zscore(String key, String member) {
        return null;
    }

    @Override
    public Tuple zpopmax(String key) {
        return null;
    }

    @Override
    public Set<Tuple> zpopmax(String key, int count) {
        return null;
    }

    @Override
    public Tuple zpopmin(String key) {
        return null;
    }

    @Override
    public Set<Tuple> zpopmin(String key, int count) {
        return null;
    }

    @Override
    public List<String> sort(String key) {
        return null;
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return null;
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return null;
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return null;
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return null;
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return null;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return null;
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return null;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return null;
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return null;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return null;
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return null;
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return null;
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return null;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return null;
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return null;
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return null;
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return null;
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return null;
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return null;
    }

    @Override
    public Long zremrangeByRank(String key, long start, long stop) {
        return null;
    }

    @Override
    public Long zremrangeByScore(String key, double min, double max) {
        return null;
    }

    @Override
    public Long zremrangeByScore(String key, String min, String max) {
        return null;
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        return null;
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        return null;
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        return null;
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        return null;
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        return null;
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        return null;
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value) {
        return null;
    }

    @Override
    public Long lpushx(String key, String... string) {
        return null;
    }

    @Override
    public Long rpushx(String key, String... string) {
        return null;
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return null;
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return null;
    }

    @Override
    public Long unlink(String key) {
        return null;
    }

    @Override
    public String echo(String string) {
        return null;
    }

    @Override
    public Long bitcount(String key) {
        return null;
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return null;
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return null;
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        return null;
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        return null;
    }

    @Override
    public Long pfadd(String key, String... elements) {
        return null;
    }

    @Override
    public long pfcount(String key) {
        return 0;
    }

    @Override
    public Long geoadd(String key, double longitude, double latitude, String member) {
        return null;
    }

    @Override
    public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
        return null;
    }

    @Override
    public Double geodist(String key, String member1, String member2) {
        return null;
    }

    @Override
    public Double geodist(String key, String member1, String member2, GeoUnit unit) {
        return null;
    }

    @Override
    public List<String> geohash(String key, String... members) {
        return null;
    }

    @Override
    public List<GeoCoordinate> geopos(String key, String... members) {
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit) {
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        return null;
    }

    @Override
    public List<Long> bitfield(String key, String... arguments) {
        return null;
    }

    @Override
    public List<Long> bitfieldReadonly(String key, String... arguments) {
        return null;
    }

    @Override
    public Long hstrlen(String key, String field) {
        return null;
    }

    @Override
    public StreamEntryID xadd(String key, StreamEntryID id, Map<String, String> hash) {
        return null;
    }

    @Override
    public StreamEntryID xadd(String key, StreamEntryID id, Map<String, String> hash, long maxLen, boolean approximateLength) {
        return null;
    }

    @Override
    public Long xlen(String key) {
        return null;
    }

    @Override
    public List<StreamEntry> xrange(String key, StreamEntryID start, StreamEntryID end, int count) {
        return null;
    }

    @Override
    public List<StreamEntry> xrevrange(String key, StreamEntryID end, StreamEntryID start, int count) {
        return null;
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xread(int count, long block, Map.Entry<String, StreamEntryID>... streams) {
        return null;
    }

    @Override
    public Long xack(String key, String group, StreamEntryID... ids) {
        return null;
    }

    @Override
    public String xgroupCreate(String key, String groupname, StreamEntryID id, boolean makeStream) {
        return null;
    }

    @Override
    public String xgroupSetID(String key, String groupname, StreamEntryID id) {
        return null;
    }

    @Override
    public Long xgroupDestroy(String key, String groupname) {
        return null;
    }

    @Override
    public Long xgroupDelConsumer(String key, String groupname, String consumername) {
        return null;
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xreadGroup(String groupname, String consumer, int count, long block, boolean noAck, Map.Entry<String, StreamEntryID>... streams) {
        return null;
    }

    @Override
    public List<StreamPendingEntry> xpending(String key, String groupname, StreamEntryID start, StreamEntryID end, int count, String consumername) {
        return null;
    }

    @Override
    public Long xdel(String key, StreamEntryID... ids) {
        return null;
    }

    @Override
    public Long xtrim(String key, long maxLen, boolean approximateLength) {
        return null;
    }

    @Override
    public List<StreamEntry> xclaim(String key, String group, String consumername, long minIdleTime, long newIdleTime, int retries, boolean force, StreamEntryID... ids) {
        return null;
    }

    @Override
    public Long waitReplicas(String key, int replicas, long timeout) {
        return null;
    }

    @Override
    public String set(String key, String value, SetParams params) {
        return null;
    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        return null;
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        return null;
    }

    @Override
    public Object eval(String script, String sampleKey) {
        return null;
    }

    @Override
    public Object evalsha(String sha1, String sampleKey) {
        return null;
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        return null;
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        return null;
    }

    @Override
    public Boolean scriptExists(String sha1, String sampleKey) {
        return null;
    }

    @Override
    public List<Boolean> scriptExists(String sampleKey, String... sha1) {
        return null;
    }

    @Override
    public String scriptLoad(String script, String sampleKey) {
        return null;
    }

    @Override
    public String scriptFlush(String sampleKey) {
        return null;
    }

    @Override
    public String scriptKill(String sampleKey) {
        return null;
    }

    @Override
    public Long del(String... keys) {
        return null;
    }

    @Override
    public Long unlink(String... keys) {
        return null;
    }

    @Override
    public Long exists(String... keys) {
        return null;
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        return null;
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        return null;
    }

    @Override
    public List<String> mget(String... keys) {
        return null;
    }

    @Override
    public String mset(String... keysvalues) {
        return null;
    }

    @Override
    public Long msetnx(String... keysvalues) {
        return null;
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return null;
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return null;
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        return null;
    }

    @Override
    public Set<String> sdiff(String... keys) {
        return null;
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        return null;
    }

    @Override
    public Set<String> sinter(String... keys) {
        return null;
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        return null;
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        return null;
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        return null;
    }

    @Override
    public Long sort(String key, String dstkey) {
        return null;
    }

    @Override
    public Set<String> sunion(String... keys) {
        return null;
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        return null;
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        return null;
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        return null;
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        return null;
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        return null;
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        return null;
    }

    @Override
    public Long publish(String channel, String message) {
        return null;
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {

    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {

    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        return null;
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        return null;
    }

    @Override
    public long pfcount(String... keys) {
        return 0;
    }

    @Override
    public Long touch(String... keys) {
        return null;
    }

    @Override
    public ScanResult<String> scan(String cursor, ScanParams params) {
        return null;
    }

    @Override
    public Set<String> keys(String pattern) {
        return null;
    }

    public RedisCrossRoomTopology getRedisClusterCrossRoomInfo() {
        RedisCrossRoomTopology redisCrossRoomTopology = new RedisCrossRoomTopology();
        try {
            List<String> majorInstanceList = new ArrayList<String>(majorPipelineCluster.getClusterNodes().keySet());
            List<String> minorInstanceList = new ArrayList<String>(minorPipelineCluster.getClusterNodes().keySet());
            redisCrossRoomTopology.setMajorAppId(majorAppId);
            redisCrossRoomTopology.setMinorAppId(minorAppId);
            redisCrossRoomTopology.setMajorInstanceList(majorInstanceList);
            redisCrossRoomTopology.setMinorInstanceList(minorInstanceList);
            return redisCrossRoomTopology;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return redisCrossRoomTopology;
    }

    public PipelineCluster getMajorPipelineCluster() {
        return majorPipelineCluster;
    }

    public PipelineCluster getMinorPipelineCluster() {
        return minorPipelineCluster;
    }
}
