package redis.clients.jedis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.commands.JedisClusterCommands;
import redis.clients.jedis.commands.JedisClusterScriptingCommands;
import redis.clients.jedis.commands.MultiKeyJedisClusterCommands;
import redis.clients.jedis.commands.SohuPipelineCommands;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;
import redis.clients.jedis.util.OpEnum;
import redis.clients.jedis.util.RedisCrossRoomTopology;
import redis.clients.jedis.valueobject.BitOffsetValue;
import redis.clients.jedis.valueobject.RangeRankVO;
import redis.clients.jedis.valueobject.RangeScoreVO;
import redis.clients.jedis.valueobject.SortedSetVO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wenruiwu
 */
public class CrossRoomCluster implements JedisClusterCommands, MultiKeyJedisClusterCommands,
        JedisClusterScriptingCommands, SohuPipelineCommands {

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
    public byte[] getBytes(final String key) {
        return new CrossRoomClusterCommand<byte[]>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public byte[] execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.getBytes(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("getBytes key %s", key);
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
        return new CrossRoomClusterCommand<Boolean>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Boolean execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.exists(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("exists key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long persist(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.persist(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("persist key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String type(String key) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.type(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("type key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public byte[] dump(String key) {
        return new CrossRoomClusterCommand<byte[]>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public byte[] execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.dump(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("dump key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String restore(String key, int ttl, byte[] serializedValue) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.restore(key, ttl, serializedValue);
            }

            @Override
            public String getCommandParam() {
                return String.format("restore key %s, ttl %s, serializedValue %s", key, ttl, serializedValue);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long expire(String key, int seconds) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.expire(key, seconds);
            }

            @Override
            public String getCommandParam() {
                return String.format("expire key %s, seconds %s", key, seconds);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.pexpire(key, milliseconds);
            }

            @Override
            public String getCommandParam() {
                return String.format("pexpire key %s, milliseconds %s", key, milliseconds);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.expireAt(key, unixTime);
            }

            @Override
            public String getCommandParam() {
                return String.format("expireAt key %s, unixTime %s", key, unixTime);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.pexpireAt(key, millisecondsTimestamp);
            }

            @Override
            public String getCommandParam() {
                return String.format("pexpireAt key %s, unixTime %s", key, millisecondsTimestamp);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long ttl(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.ttl(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("ttl key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long pttl(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.pttl(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("pttl key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long touch(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.touch(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("touch key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return new CrossRoomClusterCommand<Boolean>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Boolean execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.setbit(key, offset, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("setbit key %s, offset %s, value %s", key, offset, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        return new CrossRoomClusterCommand<Boolean>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Boolean execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.setbit(key, offset, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("setbit key %s, offset %s, value %s", key, offset, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return new CrossRoomClusterCommand<Boolean>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Boolean execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.getbit(key, offset);
            }

            @Override
            public String getCommandParam() {
                return String.format("getbit key %s, offset %s", key, offset);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.setrange(key, offset, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("setrange key %s, offset %s, value %s", key, offset, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.getrange(key, startOffset, endOffset);
            }

            @Override
            public String getCommandParam() {
                return String.format("getrange key %s, startOffset %s, endOffset %s", key, startOffset, endOffset);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String getSet(String key, String value) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.getSet(key, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("getSet key %s, value %s", key, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long setnx(String key, String value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.setnx(key, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("setnx key %s, value %s", key, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.setex(key, seconds, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("setex key %s, seconds %s, value %s", key, seconds, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String psetex(String key, long milliseconds, String value) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.psetex(key, milliseconds, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("psetex key %s, milliseconds %s, value %s", key, milliseconds, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long decrBy(String key, long decrement) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.decrBy(key, decrement);
            }

            @Override
            public String getCommandParam() {
                return String.format("decrBy key %s, decrement %s", key, decrement);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long decr(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.decr(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("decr key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long incrBy(String key, long increment) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.incrBy(key, increment);
            }

            @Override
            public String getCommandParam() {
                return String.format("incrBy key %s, increment %s", key, increment);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Double incrByFloat(String key, double increment) {
        return new CrossRoomClusterCommand<Double>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Double execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.incrByFloat(key, increment);
            }

            @Override
            public String getCommandParam() {
                return String.format("incrByFloat key %s, increment %s", key, increment);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long incr(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.incr(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("incr key %s,", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long append(String key, String value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.append(key, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("append key %s, value %s", key, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String substr(String key, int start, int end) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.substr(key, start, end);
            }

            @Override
            public String getCommandParam() {
                return String.format("substr key %s, start %s, end %s", key, start, end);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long hset(String key, String field, String value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hset(key, field, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("hset key %s, field %s, value %s", key, field, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long hset(String key, Map<String, String> hash) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hset(key, hash);
            }

            @Override
            public String getCommandParam() {
                return String.format("hset key %s, hash %s", key, hash);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String hget(String key, String field) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hget(key, field);
            }

            @Override
            public String getCommandParam() {
                return String.format("hget key %s, field %s", key, field);
            }
        }.run(OpEnum.READ);
    }

    public byte[] hgetBytes(String key, String field) {
        return new CrossRoomClusterCommand<byte[]>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public byte[] execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hgetBytes(key, field);
            }

            @Override
            public String getCommandParam() {
                return String.format("hgetBytes key %s, field %s", key, field);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hsetnx(key, field, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("hsetnx key %s, field %s, value %s", key, field, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hmset(key, hash);
            }

            @Override
            public String getCommandParam() {
                return String.format("hmset key %s, hash %s", key, hash);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hmget(key, fields);
            }

            @Override
            public String getCommandParam() {
                return String.format("hmget key %s, field %s", key, fields);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hincrBy(key, field, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("hincrBy key %s, field %s, value %s", key, field, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Boolean hexists(String key, String field) {
        return new CrossRoomClusterCommand<Boolean>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Boolean execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hexists(key, field);
            }

            @Override
            public String getCommandParam() {
                return String.format("hexists key %s, field %s", key, field);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long hdel(String key, String... field) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hdel(key, field);
            }

            @Override
            public String getCommandParam() {
                return String.format("hdel key %s, field %s", key, field);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long hlen(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hlen(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("hlen key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> hkeys(String key) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hkeys(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("hkeys key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<String> hvals(String key) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hvals(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("hvals key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return new CrossRoomClusterCommand<Map<String, String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hgetAll(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("hgetAll key %s", key);
            }
        }.run(OpEnum.READ);
    }

    public Map<byte[], byte[]> hgetAllBytes(String key) {
        return new CrossRoomClusterCommand<Map<byte[], byte[]>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<byte[], byte[]> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hgetAllBytes(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("hgetAllBytes key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long rpush(String key, String... string) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.rpush(key, string);
            }

            @Override
            public String getCommandParam() {
                return String.format("rpush key %s, string %s", key, string);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long lpush(String key, String... string) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lpush(key, string);
            }

            @Override
            public String getCommandParam() {
                return String.format("lpush key %s, string %s", key, string);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long llen(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.llen(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("llen key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<String> lrange(String key, long start, long stop) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lrange(key, start, stop);
            }

            @Override
            public String getCommandParam() {
                return String.format("lrange key %s, start %s, stop %s", key, start, stop);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String ltrim(String key, long start, long stop) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.ltrim(key, start, stop);
            }

            @Override
            public String getCommandParam() {
                return String.format("ltrim key %s, start %s, stop %s", key, start, stop);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String lindex(String key, long index) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lindex(key, index);
            }

            @Override
            public String getCommandParam() {
                return String.format("lindex key %s, index %s", key, index);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String lset(String key, long index, String value) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lset(key, index, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("lset key %s, index %s, value %s", key, index, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lrem(key, count, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("lrem key %s, count %s, value %s", key, count, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String lpop(String key) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lpop(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("lpop key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String rpop(String key) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.rpop(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("rpop key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long sadd(String key, String... member) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sadd(key, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("sadd key %s, member %s", key, member);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Set<String> smembers(String key) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.smembers(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("smembers key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long srem(String key, String... member) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.srem(key, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("srem key %s, member %s", key, member);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String spop(String key) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.spop(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("spop key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Set<String> spop(String key, long count) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.spop(key, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("spop key %s, count %s", key, count);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long scard(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.scard(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("scard key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Boolean sismember(String key, String member) {
        return new CrossRoomClusterCommand<Boolean>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Boolean execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sismember(key, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("sismember key %s, member %s", key, member);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String srandmember(String key) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.srandmember(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("srandmember key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<String> srandmember(String key, int count) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.srandmember(key, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("srandmember key %s, count %s", key, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long strlen(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.strlen(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("strlen key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zadd(key, score, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("zadd key %s, score %s, member %s", key, score, member);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zadd(key, score, member, params);
            }

            @Override
            public String getCommandParam() {
                return String.format("zadd key %s, score %s, member %s, params %s", key, score, member, params);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zadd(key, scoreMembers);
            }

            @Override
            public String getCommandParam() {
                return String.format("zadd key %s, scoreMembers %s", key, scoreMembers);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zadd(key, scoreMembers, params);
            }

            @Override
            public String getCommandParam() {
                return String.format("zadd key %s, scoreMembers %s, params %s", key, scoreMembers, params);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Set<String> zrange(String key, long start, long stop) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrange(key, start, stop);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrange key %s, start %s, stop %s", key, start, stop);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long zrem(String key, String... members) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrem(key, members);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrem key %s, members %s", key, members);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Double zincrby(String key, double increment, String member) {
        return new CrossRoomClusterCommand<Double>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Double execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zincrby(key, increment, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("zincrby key %s, increment %s, member %s", key, increment, member);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Double zincrby(String key, double increment, String member, ZIncrByParams params) {
        return new CrossRoomClusterCommand<Double>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Double execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zincrby(key, increment, member, params);
            }

            @Override
            public String getCommandParam() {
                return String.format("zincrby key %s, increment %s, member %s, params %s", key, increment, member, params);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zrank(String key, String member) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrank(key, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrank key %s, member %s", key, member);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long zrevrank(String key, String member) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrank(key, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrank key %s, member %s", key, member);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrevrange(String key, long start, long stop) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrange(key, start, stop);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrange key %s, start %s, stop %s", key, start, stop);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long stop) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeWithScores(key, start, stop);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeWithScores key %s, start %s, stop %s", key, start, stop);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long stop) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeWithScores(key, start, stop);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeWithScores key %s, start %s, stop %s", key, start, stop);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long zcard(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zcard(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("zcard key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Double zscore(String key, String member) {
        return new CrossRoomClusterCommand<Double>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Double execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zscore(key, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("zscore key %s, member %s", key, member);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Tuple zpopmax(String key) {
        return new CrossRoomClusterCommand<Tuple>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Tuple execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zpopmax(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("zpopmax key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zpopmax(String key, int count) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zpopmax(key, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zpopmax key %s, count %s", key, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Tuple zpopmin(String key) {
        return new CrossRoomClusterCommand<Tuple>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Tuple execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zpopmin(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("zpopmin key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zpopmin(String key, int count) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zpopmin(key, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zpopmin key %s, count %s", key, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<String> sort(String key) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sort(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("sort key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sort(key, sortingParameters);
            }

            @Override
            public String getCommandParam() {
                return String.format("sort key %s, sortingParameters %s", key, sortingParameters);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zcount(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zcount key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zcount(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zcount key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByScore(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByScore key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByScore(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByScore key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByScore(key, max, min);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByScore key %s, max %s, min %s", key, max, min);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByScore(key, min, max, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByScore key %s, min %s, max %s, offset %s, count %s", key, min, max, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByScore(key, max, min);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByScore key %s, max %s, min %s", key, max, min);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByScore(key, min, max, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByScore key %s, min %s, max %s, offset %s, count %s", key, min, max, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByScore(key, min, max, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByScore key %s, min %s, max %s, offset %s, count %s", key, min, max, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByScoreWithScores(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByScoreWithScores key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByScoreWithScores(key, max, min);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByScoreWithScores key %s, max %s, min %s", key, max, min);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByScoreWithScores(key, min, max, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByScoreWithScores key %s, min %s, max %s, offset %s, count %s", key, min, max, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByScore(key, max, min, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByScore key %s, max %s, min %s, offset %s, count %s", key, max, min, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByScoreWithScores(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByScoreWithScores key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByScoreWithScores(key, max, min);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByScoreWithScores key %s, max %s, min %s", key, max, min);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByScoreWithScores(key, min, max, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByScoreWithScores key %s, min %s, max %s, offset %s, count %s", key, min, max, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByScoreWithScores key %s, max %s, min %s, offset %s, count %s", key, max, min, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return new CrossRoomClusterCommand<Set<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByScoreWithScores key %s, max %s, min %s, offset %s, count %s", key, max, min, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long zremrangeByRank(String key, long start, long stop) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zremrangeByRank(key, start, stop);
            }

            @Override
            public String getCommandParam() {
                return String.format("zremrangeByRank key %s, start %s, stop %s", key, start, stop);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zremrangeByScore(String key, double min, double max) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zremrangeByScore(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zremrangeByScore key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zremrangeByScore(String key, String min, String max) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zremrangeByScore(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zremrangeByScore key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zlexcount(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zlexcount key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByLex(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByLex key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrangeByLex(key, min, max, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrangeByLex key %s, min %s, max %s, offset %s, count %s", key, min, max, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByLex(key, max, min);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByLex key %s, max %s, min %s", key, max, min);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zrevrangeByLex(key, max, min, offset, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("zrevrangeByLex key %s, max %s, min %s, offset %s, count %s", key, max, min, offset, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zremrangeByLex(key, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("zremrangeByLex key %s, min %s, max %s", key, min, max);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.linsert(key, where, pivot, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("linsert key %s, where %s, pivot %s, value %s", key, where, pivot, value);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long lpushx(String key, String... string) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lpushx(key, string);
            }

            @Override
            public String getCommandParam() {
                return String.format("lpushx key %s, string %s", key, string);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long rpushx(String key, String... string) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.rpushx(key, string);
            }

            @Override
            public String getCommandParam() {
                return String.format("rpushx key %s, string %s", key, string);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.blpop(timeout, key);
            }

            @Override
            public String getCommandParam() {
                return String.format("blpop timeout %s, key %s", timeout, key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.brpop(timeout, key);
            }

            @Override
            public String getCommandParam() {
                return String.format("brpop timeout %s, key %s", timeout, key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long unlink(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.unlink(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("unlink key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String echo(String string) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.echo(string);
            }

            @Override
            public String getCommandParam() {
                return String.format("echo string %s", string);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long bitcount(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.bitcount(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("bitcount key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.bitcount(key, start, end);
            }

            @Override
            public String getCommandParam() {
                return String.format("bitcount key %s, start %s, end %s", key, start, end);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return new CrossRoomClusterCommand<ScanResult<Map.Entry<String, String>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public ScanResult<Map.Entry<String, String>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hscan(key, cursor);
            }

            @Override
            public String getCommandParam() {
                return String.format("hscan key %s, cursor %s", key, cursor);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        return new CrossRoomClusterCommand<ScanResult<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public ScanResult<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sscan(key, cursor);
            }

            @Override
            public String getCommandParam() {
                return String.format("sscan key %s, cursor %s", key, cursor);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        return new CrossRoomClusterCommand<ScanResult<Tuple>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public ScanResult<Tuple> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zscan(key, cursor);
            }

            @Override
            public String getCommandParam() {
                return String.format("zscan key %s, cursor %s", key, cursor);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long pfadd(String key, String... elements) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.pfadd(key, elements);
            }

            @Override
            public String getCommandParam() {
                return String.format("pfadd key %s, elements %s", key, elements);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public long pfcount(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.pfadd(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("pfadd key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long geoadd(String key, double longitude, double latitude, String member) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.geoadd(key, longitude, latitude, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("geoadd key %s, longitude %s, latitude %s, member %s", key, longitude, latitude, member);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.geoadd(key, memberCoordinateMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("geoadd key %s, memberCoordinateMap %s", key, memberCoordinateMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Double geodist(String key, String member1, String member2) {
        return new CrossRoomClusterCommand<Double>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Double execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.geodist(key, member1, member2);
            }

            @Override
            public String getCommandParam() {
                return String.format("geodist key %s, member1 %s, member2 %s", key, member1, member2);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Double geodist(String key, String member1, String member2, GeoUnit unit) {
        return new CrossRoomClusterCommand<Double>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Double execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.geodist(key, member1, member2, unit);
            }

            @Override
            public String getCommandParam() {
                return String.format("geodist key %s, member1 %s, member2 %s, unit %s", key, member1, member2, unit);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<String> geohash(String key, String... members) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.geohash(key, members);
            }

            @Override
            public String getCommandParam() {
                return String.format("geohash key %s, members %s", key, members);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoCoordinate> geopos(String key, String... members) {
        return new CrossRoomClusterCommand<List<GeoCoordinate>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoCoordinate> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.geopos(key, members);
            }

            @Override
            public String getCommandParam() {
                return String.format("geopos key %s, members %s", key, members);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        return new CrossRoomClusterCommand<List<GeoRadiusResponse>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoRadiusResponse> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.georadius(key, longitude, latitude, radius, unit);
            }

            @Override
            public String getCommandParam() {
                return String.format("georadius key %s, longitude %s, latitude %s, radius %s, unit %s", key, longitude, latitude, radius, unit);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        return new CrossRoomClusterCommand<List<GeoRadiusResponse>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoRadiusResponse> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.georadiusReadonly(key, longitude, latitude, radius, unit);
            }

            @Override
            public String getCommandParam() {
                return String.format("georadiusReadonly key %s, longitude %s, latitude %s, radius %s, unit %s", key, longitude, latitude, radius, unit);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        return new CrossRoomClusterCommand<List<GeoRadiusResponse>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoRadiusResponse> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.georadius(key, longitude, latitude, radius, unit, param);
            }

            @Override
            public String getCommandParam() {
                return String.format("georadius key %s, longitude %s, latitude %s, radius %s, unit %s, param %s", key, longitude, latitude, radius, unit, param);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        return new CrossRoomClusterCommand<List<GeoRadiusResponse>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoRadiusResponse> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.georadiusReadonly(key, longitude, latitude, radius, unit, param);
            }

            @Override
            public String getCommandParam() {
                return String.format("georadiusReadonly key %s, longitude %s, latitude %s, radius %s, unit %s, param %s", key, longitude, latitude, radius, unit, param);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
        return new CrossRoomClusterCommand<List<GeoRadiusResponse>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoRadiusResponse> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.georadiusByMember(key, member, radius, unit);
            }

            @Override
            public String getCommandParam() {
                return String.format("georadiusByMember key %s, member %s, radius %s, unit %s", key, member, radius, unit);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit) {
        return new CrossRoomClusterCommand<List<GeoRadiusResponse>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoRadiusResponse> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.georadiusByMemberReadonly(key, member, radius, unit);
            }

            @Override
            public String getCommandParam() {
                return String.format("georadiusByMemberReadonly key %s, member %s, radius %s, unit %s", key, member, radius, unit);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        return new CrossRoomClusterCommand<List<GeoRadiusResponse>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoRadiusResponse> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.georadiusByMember(key, member, radius, unit, param);
            }

            @Override
            public String getCommandParam() {
                return String.format("georadiusByMember key %s, member %s, radius %s, unit %s, param %s", key, member, radius, unit, param);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        return new CrossRoomClusterCommand<List<GeoRadiusResponse>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<GeoRadiusResponse> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.georadiusByMemberReadonly(key, member, radius, unit, param);
            }

            @Override
            public String getCommandParam() {
                return String.format("georadiusByMemberReadonly key %s, member %s, radius %s, unit %s, param %s", key, member, radius, unit, param);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<Long> bitfield(String key, String... arguments) {
        return new CrossRoomClusterCommand<List<Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.bitfield(key, arguments);
            }

            @Override
            public String getCommandParam() {
                return String.format("bitfield key %s, arguments %s", key, arguments);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public List<Long> bitfieldReadonly(String key, String... arguments) {
        return new CrossRoomClusterCommand<List<Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.bitfieldReadonly(key, arguments);
            }

            @Override
            public String getCommandParam() {
                return String.format("bitfieldReadonly key %s, arguments %s", key, arguments);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long hstrlen(String key, String field) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hstrlen(key, field);
            }

            @Override
            public String getCommandParam() {
                return String.format("hstrlen key %s, field %s", key, field);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public StreamEntryID xadd(String key, StreamEntryID id, Map<String, String> hash) {
        return new CrossRoomClusterCommand<StreamEntryID>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public StreamEntryID execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xadd(key, id, hash);
            }

            @Override
            public String getCommandParam() {
                return String.format("xadd key %s, id %s, hash %s", key, id, hash);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public StreamEntryID xadd(String key, StreamEntryID id, Map<String, String> hash, long maxLen, boolean approximateLength) {
        return new CrossRoomClusterCommand<StreamEntryID>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public StreamEntryID execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xadd(key, id, hash, maxLen, approximateLength);
            }

            @Override
            public String getCommandParam() {
                return String.format("xadd key %s, id %s, hash %s, maxLen %s, approximateLength %s", key, id, hash);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long xlen(String key) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xlen(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("xlen key %s", key);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<StreamEntry> xrange(String key, StreamEntryID start, StreamEntryID end, int count) {
        return new CrossRoomClusterCommand<List<StreamEntry>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<StreamEntry> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xrange(key, start, end, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("xrange key %s, start %s, end %s, count %s", key, start, end, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<StreamEntry> xrevrange(String key, StreamEntryID end, StreamEntryID start, int count) {
        return new CrossRoomClusterCommand<List<StreamEntry>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<StreamEntry> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xrevrange(key, end, start, count);
            }

            @Override
            public String getCommandParam() {
                return String.format("xrevrange key %s, end %s, start %s, count %s", key, end, start, count);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xread(int count, long block, Map.Entry<String, StreamEntryID>... streams) {
        return new CrossRoomClusterCommand<List<Map.Entry<String, List<StreamEntry>>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<Map.Entry<String, List<StreamEntry>>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xread(count, block, streams);
            }

            @Override
            public String getCommandParam() {
                return String.format("xread count %s, block %s, streams %s", count, block, streams);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long xack(String key, String group, StreamEntryID... ids) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xack(key, group, ids);
            }

            @Override
            public String getCommandParam() {
                return String.format("xack key %s, group %s, ids %s", key, group, ids);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String xgroupCreate(String key, String groupname, StreamEntryID id, boolean makeStream) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xgroupCreate(key, groupname, id, makeStream);
            }

            @Override
            public String getCommandParam() {
                return String.format("xgroupCreate key %s, groupname %s, id %s, makeStream %s", key, groupname, id, makeStream);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String xgroupSetID(String key, String groupname, StreamEntryID id) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xgroupSetID(key, groupname, id);
            }

            @Override
            public String getCommandParam() {
                return String.format("xgroupSetID key %s, groupname %s, id %s", key, groupname, id);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long xgroupDestroy(String key, String groupname) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xgroupDestroy(key, groupname);
            }

            @Override
            public String getCommandParam() {
                return String.format("xgroupDestroy key %s, groupname %s", key, groupname);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long xgroupDelConsumer(String key, String groupname, String consumername) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xgroupDelConsumer(key, groupname, consumername);
            }

            @Override
            public String getCommandParam() {
                return String.format("xgroupDelConsumer key %s, groupname %s, consumername %s", key, groupname, consumername);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xreadGroup(String groupname, String consumer, int count, long block, boolean noAck, Map.Entry<String, StreamEntryID>... streams) {
        return new CrossRoomClusterCommand<List<Map.Entry<String, List<StreamEntry>>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<Map.Entry<String, List<StreamEntry>>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xreadGroup(groupname, consumer, count, block, noAck, streams);
            }

            @Override
            public String getCommandParam() {
                return String.format("xreadGroup groupname %s, consumer %s, count %s, block %s, noAck %s, streams %s", groupname, consumer, count, block, noAck, streams);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<StreamPendingEntry> xpending(String key, String groupname, StreamEntryID start, StreamEntryID end, int count, String consumername) {
        return new CrossRoomClusterCommand<List<StreamPendingEntry>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<StreamPendingEntry> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xpending(key, groupname, start, end, count, consumername);
            }

            @Override
            public String getCommandParam() {
                return String.format("xpending key %s, groupname %s, start %s, end %s, count %s, consumername %s", key, groupname, start, end, count, consumername);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long xdel(String key, StreamEntryID... ids) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xdel(key, ids);
            }

            @Override
            public String getCommandParam() {
                return String.format("xdel key %s, ids %s", key, ids);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long xtrim(String key, long maxLen, boolean approximateLength) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xtrim(key, maxLen, approximateLength);
            }

            @Override
            public String getCommandParam() {
                return String.format("xtrim key %s, maxLen %s, approximateLength %s", key, maxLen, approximateLength);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public List<StreamEntry> xclaim(String key, String group, String consumername, long minIdleTime, long newIdleTime, int retries, boolean force, StreamEntryID... ids) {
        return new CrossRoomClusterCommand<List<StreamEntry>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<StreamEntry> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.xclaim(key, group, consumername, minIdleTime, newIdleTime, retries, force, ids);
            }

            @Override
            public String getCommandParam() {
                return String.format("xclaim key %s, group %s, consumername %s, minIdleTime %s, newIdleTime %s, retries %s, force %s, ids %s",
                        key, group, consumername, minIdleTime, newIdleTime, retries, force, ids);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long waitReplicas(String key, int replicas, long timeout) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.waitReplicas(key, replicas, timeout);
            }

            @Override
            public String getCommandParam() {
                return String.format("waitReplicas key %s, replicas %s, timeout %s", key, replicas, timeout);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String set(String key, String value, SetParams params) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.set(key, value, params);
            }

            @Override
            public String getCommandParam() {
                return String.format("set key %s value %s, params %s", key, value, params);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        return new CrossRoomClusterCommand<Object>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Object execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.eval(script, keyCount, params);
            }

            @Override
            public String getCommandParam() {
                return String.format("eval script %s keyCount %s, params %s", script, keyCount, params);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        return new CrossRoomClusterCommand<Object>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Object execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.eval(script, keys, args);
            }

            @Override
            public String getCommandParam() {
                return String.format("eval script %s keys %s, args %s", script, keys, args);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Object eval(String script, String sampleKey) {
        return new CrossRoomClusterCommand<Object>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Object execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.eval(script, sampleKey);
            }

            @Override
            public String getCommandParam() {
                return String.format("eval script %s sampleKey %s", script, sampleKey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Object evalsha(String sha1, String sampleKey) {
        return new CrossRoomClusterCommand<Object>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Object execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.evalsha(sha1, sampleKey);
            }

            @Override
            public String getCommandParam() {
                return String.format("evalsha sha1 %s sampleKey %s", sha1, sampleKey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        return new CrossRoomClusterCommand<Object>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Object execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.evalsha(sha1, keys, args);
            }

            @Override
            public String getCommandParam() {
                return String.format("evalsha sha1 %s keys %s, args %s", sha1, keys, args);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        return new CrossRoomClusterCommand<Object>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Object execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.evalsha(sha1, keyCount, params);
            }

            @Override
            public String getCommandParam() {
                return String.format("evalsha sha1 %s keyCount %s, params %s", sha1, keyCount, params);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Boolean scriptExists(String sha1, String sampleKey) {
        return new CrossRoomClusterCommand<Boolean>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Boolean execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.scriptExists(sha1, sampleKey);
            }

            @Override
            public String getCommandParam() {
                return String.format("scriptExists sha1 %s sampleKey %s", sha1, sampleKey);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<Boolean> scriptExists(String sampleKey, String... sha1) {
        return new CrossRoomClusterCommand<List<Boolean>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<Boolean> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.scriptExists(sampleKey, sha1);
            }

            @Override
            public String getCommandParam() {
                return String.format("scriptExists sha1 %s sampleKey %s", sha1, sampleKey);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String scriptLoad(String script, String sampleKey) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.scriptLoad(script, sampleKey);
            }

            @Override
            public String getCommandParam() {
                return String.format("scriptLoad script %s sampleKey %s", script, sampleKey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String scriptFlush(String sampleKey) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.scriptFlush(sampleKey);
            }

            @Override
            public String getCommandParam() {
                return String.format("scriptFlush sampleKey %s", sampleKey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String scriptKill(String sampleKey) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.scriptKill(sampleKey);
            }

            @Override
            public String getCommandParam() {
                return String.format("scriptKill sampleKey %s", sampleKey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long del(String... keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.del(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("del keys %s", keys);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long unlink(String... keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.unlink(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("unlink keys %s", keys);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long exists(String... keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.exists(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("exists keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.blpop(timeout, keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("blpop timeout %s, keys %s", timeout, keys);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.brpop(timeout, keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("brpop timeout %s, key %s", timeout, keys);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.rename(oldkey, newkey);
            }

            @Override
            public String getCommandParam() {
                return String.format("rename oldkey %s, newKey %s", oldkey, newkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.renamenx(oldkey, newkey);
            }

            @Override
            public String getCommandParam() {
                return String.format("renamenx oldkey %s, newKey %s", oldkey, newkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.rpoplpush(srckey, dstkey);
            }

            @Override
            public String getCommandParam() {
                return String.format("rpoplpush srckey %s, dstkey %s", srckey, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Set<String> sdiff(String... keys) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sdiff(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("sdiff keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sdiffstore(dstkey, keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("sdiffstore keys %s, dstkey %s", keys, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Set<String> sinter(String... keys) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sinter(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("sinter keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sinterstore(dstkey, keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("sinterstore keys %s, dstkey %s", keys, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.smove(srckey, dstkey, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("sinterstore srckey %s, dstkey %s, member %s", srckey, dstkey, member);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sort(key, sortingParameters, dstkey);
            }

            @Override
            public String getCommandParam() {
                return String.format("sort key %s, sortingParameters %s, dstkey %s", key, sortingParameters, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long sort(String key, String dstkey) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sort(key, dstkey);
            }

            @Override
            public String getCommandParam() {
                return String.format("sort key %s, dstkey %s", key, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Set<String> sunion(String... keys) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sunion(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("sunion keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sunionstore(dstkey, keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("sunionstore keys %s, dstkey %s", keys, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zinterstore(dstkey, sets);
            }

            @Override
            public String getCommandParam() {
                return String.format("zinterstore sets %s, dstkey %s", sets, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zinterstore(dstkey, params, sets);
            }

            @Override
            public String getCommandParam() {
                return String.format("zinterstore sets %s, params %s, dstkey %s", sets, params, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zunionstore(dstkey, sets);
            }

            @Override
            public String getCommandParam() {
                return String.format("zunionstore sets %s, dstkey %s", sets, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zunionstore(dstkey, params, sets);
            }

            @Override
            public String getCommandParam() {
                return String.format("zunionstore sets %s, params %s, dstkey %s", sets, params, dstkey);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.brpoplpush(source, destination, timeout);
            }

            @Override
            public String getCommandParam() {
                return String.format("brpoplpush source %s, destination %s, timeout %s",
                        source, destination, timeout);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long publish(String channel, String message) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.publish(channel, message);
            }

            @Override
            public String getCommandParam() {
                return String.format("publish channel %s, message %s",
                        channel, message);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        new CrossRoomClusterCommand<Integer>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Integer execute(PipelineCluster pipelineCluster) {
                pipelineCluster.subscribe(jedisPubSub, channels);
                return 0;
            }

            @Override
            public String getCommandParam() {
                return String.format("subscribe jedisPubSub %s, channels %s",
                        jedisPubSub, channels);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        new CrossRoomClusterCommand<Integer>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Integer execute(PipelineCluster pipelineCluster) {
                pipelineCluster.psubscribe(jedisPubSub, patterns);
                return 0;
            }

            @Override
            public String getCommandParam() {
                return String.format("psubscribe jedisPubSub %s, patterns %s",
                        jedisPubSub, patterns);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.bitop(op, destKey, srcKeys);
            }

            @Override
            public String getCommandParam() {
                return String.format("bitop op %s, destKey %s, srcKeys %s", op, destKey, srcKeys);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.pfmerge(destkey, sourcekeys);
            }

            @Override
            public String getCommandParam() {
                return String.format("pfmerge destkey %s, sourcekeys %s", destkey, sourcekeys);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public long pfcount(String... keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.pfcount(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("pfcount keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Long touch(String... keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.touch(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("touch keys %s", keys);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public ScanResult<String> scan(String cursor, ScanParams params) {
        return new CrossRoomClusterCommand<ScanResult<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public ScanResult<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.scan(cursor, params);
            }

            @Override
            public String getCommandParam() {
                return String.format("scan cursor %s, params %s", cursor, params);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Set<String> keys(String pattern) {
        return new CrossRoomClusterCommand<Set<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.keys(pattern);
            }

            @Override
            public String getCommandParam() {
                return String.format("keys key %s", pattern);
            }
        }.run(OpEnum.READ);
    }

    @Deprecated
    @Override
    public List<String> mget(String... keys) {
        return new CrossRoomClusterCommand<List<String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mget(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("mget keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, String> mget(final List<String> keys) {
        return new CrossRoomClusterCommand<Map<String, String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mget(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("mget keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Deprecated
    @Override
    public String mset(String... keysvalues) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mset(keysvalues);
            }

            @Override
            public String getCommandParam() {
                return String.format("mset keysvalues %s", keysvalues);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String mset(final Map<String, String> keyValueMap) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mset(keyValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mset keyValueMap %s", keyValueMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String msetBytes(final Map<String, byte[]> keyValueMap) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.msetBytes(keyValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("msetBytes keyValueMap %s", keyValueMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Deprecated
    @Override
    public Long msetnx(String... keysvalues) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.msetnx(keysvalues);
            }

            @Override
            public String getCommandParam() {
                return String.format("msetnx keysvalues %s", keysvalues);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String msetnx(final Map<String, String> keyValueMap) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.msetnx(keyValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("msetnx keyValueMap %s", keyValueMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Boolean> mGetbit(final Map<String, Long> keyOffsetMap) {
        return new CrossRoomClusterCommand<Map<String, Boolean>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Boolean> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mGetbit(keyOffsetMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mGetbit keyOffsetMap %s", keyOffsetMap);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, Boolean> mSetbit(final Map<String, BitOffsetValue> keyOffsetValueMap) {
        return new CrossRoomClusterCommand<Map<String, Boolean>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Boolean> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mSetbit(keyOffsetValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mSetbit keyOffsetValueMap %s", keyOffsetValueMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Long> mexpire(final Map<String, Integer> keyTimeMap) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mexpire(keyTimeMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mexpire keyTimeMap %s", keyTimeMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Map<String, String>> mHgetAll(List<String> keys) {
        return new CrossRoomClusterCommand<Map<String, Map<String, String>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Map<String, String>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mHgetAll(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("mHgetAll keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, List<String>> mhmget(final Map<String, List<String>> keyValueMap) {
        return new CrossRoomClusterCommand<Map<String, List<String>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, List<String>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mhmget(keyValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mhmget keys %s", keyValueMap);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, String> mhmset(final Map<String, Map<String, String>> keyValueMap) {
        return new CrossRoomClusterCommand<Map<String, String>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, String> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mhmset(keyValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mhmset keyValueMap %s", keyValueMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Long> msetnxs(final Map<String, String> keyValueMap) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.msetnxs(keyValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("msetnxs keyValueMap %s", keyValueMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Long> mhsetnx(final Map<String, Map<String, String>> keyValueMap) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mhsetnx(keyValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mhsetnx keyValueMap %s", keyValueMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String msetex(final Map<String, String> keyValueMap, final int seconds) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.msetex(keyValueMap, seconds);
            }

            @Override
            public String getCommandParam() {
                return String.format("msetex keyValueMap %s, seconds %s", keyValueMap, seconds);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public String mset(final Map<String, String> keyValueMap, final SetParams params) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mset(keyValueMap, params);
            }

            @Override
            public String getCommandParam() {
                return String.format("mset keyValueMap %s, params %s", keyValueMap, params);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Long> mincrBy(final Map<String, Long> keyValueMap) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mincrBy(keyValueMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mincrBy keyValueMap %s", keyValueMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Long mdel(final List<String> keys) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mdel(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("mdel keys %s", keys);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Long> mzadd(final Map<String, SortedSetVO> map) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzadd(map);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzadd map %s", map);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Long> mzadds(final Map<String, Map<String, Double>> map) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzadds(map);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzadds map %s", map);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Long> mzrem(final Map<String, String[]> map) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzrem(map);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzrem map %s", map);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, Long> mzcard(final List<String> keys) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzcard(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzcard keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, Long> mzrank(final Map<String, String> map) {
        return new CrossRoomClusterCommand<Map<String, Long>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Long> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzrank(map);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzrank map %s", map);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, Boolean> msismember(final Map<String, List<String>> map) {
        return new CrossRoomClusterCommand<Map<String, Boolean>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Boolean> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.msismember(map);
            }

            @Override
            public String getCommandParam() {
                return String.format("msismember map %s", map);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, Set<String>> mzrangeByScore(final List<String> keys, final double min,
                                                   final double max) {
        return new CrossRoomClusterCommand<Map<String, Set<String>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Set<String>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzrangeByScore(keys, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzrangeByScore keys %s, min %s, max %s", keys, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, Set<Tuple>> mzrevrangeWithScores(final Map<String, RangeRankVO> keyRankMap) {
        return new CrossRoomClusterCommand<Map<String, Set<Tuple>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Set<Tuple>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzrevrangeWithScores(keyRankMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzrevrangeWithScores keyRankMap %s", keyRankMap);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, Set<String>> mzrangeByScore(final Map<String, RangeScoreVO> keyScoreMap) {
        return new CrossRoomClusterCommand<Map<String, Set<String>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Set<String>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzrangeByScore(keyScoreMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzrangeByScore keyScoreMap %s", keyScoreMap);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, Set<String>> mzrangeByScore(final List<String> keys, final String min,
                                                   final String max) {
        return new CrossRoomClusterCommand<Map<String, Set<String>>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Set<String>> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzrangeByScore(keys, min, max);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzrangeByScore keys %s, min %s, max %s", keys, min, max);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public String mzremrangeByScore(final Map<String, RangeScoreVO> keyScoreMap) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzremrangeByScore(keyScoreMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzremrangeByScore keyScoreMap %s", keyScoreMap);
            }
        }.run(OpEnum.WRITE);
    }

    @Override
    public Map<String, byte[]> mgetBytes(final List<String> keys) {
        return new CrossRoomClusterCommand<Map<String, byte[]>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, byte[]> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mgetBytes(keys);
            }

            @Override
            public String getCommandParam() {
                return String.format("mgetBytes keys %s", keys);
            }
        }.run(OpEnum.READ);
    }

    @Override
    public Map<String, Double> mzscore(final Map<String, String> keyMemberMap) {
        return new CrossRoomClusterCommand<Map<String, Double>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Map<String, Double> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.mzscore(keyMemberMap);
            }

            @Override
            public String getCommandParam() {
                return String.format("mzscore keyMemberMap %s", keyMemberMap);
            }
        }.run(OpEnum.READ);
    }

    public String set(final String key, final byte[] value) {
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

    public String set(final String key, final byte[] value, final String expx, final long time) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.set(key, value, expx, time);
            }

            @Override
            public String getCommandParam() {
                return String.format("set key %s value %s, exps %s, time %s", key, value, expx, time);
            }
        }.run(OpEnum.WRITE);
    }

    public String setex(String key, int seconds, byte[] value) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.setex(key, seconds, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("setex key %s, seconds %s, value %s", key, seconds, value);
            }
        }.run(OpEnum.WRITE);
    }

    public Long setnx(String key, byte[] value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.setnx(key, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("setnx key %s, value %s", key, value);
            }
        }.run(OpEnum.WRITE);
    }

    public Long hset(String key, String field, byte[] value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hset(key, field, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("hset key %s, field %s, value %s", key, field, value);
            }
        }.run(OpEnum.WRITE);
    }

    public Long hsetnx(String key, String field, byte[] value) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hsetnx(key, field, value);
            }

            @Override
            public String getCommandParam() {
                return String.format("hsetnx key %s, field %s, value %s", key, field, value);
            }
        }.run(OpEnum.WRITE);
    }

    public Long zadd(String key, double score, byte[] member) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.zadd(key, score, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("zadd key %s, score %s, member %s", key, score, member);
            }
        }.run(OpEnum.WRITE);
    }

    public Long lpush(String key, byte[]... string) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lpush(key, string);
            }

            @Override
            public String getCommandParam() {
                return String.format("lpush key %s, string %s", key, string);
            }
        }.run(OpEnum.WRITE);
    }

    public Long rpush(String key, byte[]... string) {
        return new CrossRoomClusterCommand<Long>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Long execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.rpush(key, string);
            }

            @Override
            public String getCommandParam() {
                return String.format("rpush key %s, string %s", key, string);
            }
        }.run(OpEnum.WRITE);
    }

    public Set<byte[]> hkeysBytes(final String key) {
        return new CrossRoomClusterCommand<Set<byte[]>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<byte[]> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hkeysBytes(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("hkeysBytes key %s", key);
            }
        }.run(OpEnum.READ);
    }
    public List<byte[]> hvalsBytes(final String key) {
        return new CrossRoomClusterCommand<List<byte[]>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<byte[]> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hvalsBytes(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("hvalsBytes key %s", key);
            }
        }.run(OpEnum.READ);
    }

    public String hmsetBytes(final String key, final Map<byte[], byte[]> hash) {
        return new CrossRoomClusterCommand<String>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public String execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.hmsetBytes(key, hash);
            }

            @Override
            public String getCommandParam() {
                return String.format("hmsetBytes key %s, hash %s", key, hash);
            }
        }.run(OpEnum.WRITE);
    }

    public List<byte[]> lrangeBytes(final String key, final long start, final long end) {
        return new CrossRoomClusterCommand<List<byte[]>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public List<byte[]> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lrangeBytes(key, start, end);
            }

            @Override
            public String getCommandParam() {
                return String.format("lrangeBytes key %s, start %s, end %s", key, start, end);
            }
        }.run(OpEnum.READ);
    }

    public byte[] lindexBytes(final String key, final long index) {
        return new CrossRoomClusterCommand<byte[]>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public byte[] execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lindexBytes(key, index);
            }

            @Override
            public String getCommandParam() {
                return String.format("lindexBytes key %s, index %s", key, index);
            }
        }.run(OpEnum.READ);
    }

    public byte[] lpopBytes(final String key) {
        return new CrossRoomClusterCommand<byte[]>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public byte[] execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.lpopBytes(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("lpopBytes key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    public byte[] rpopBytes(final String key) {
        return new CrossRoomClusterCommand<byte[]>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public byte[] execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.rpopBytes(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("rpopBytes key %s", key);
            }
        }.run(OpEnum.WRITE);
    }

    public Set<byte[]> smembersBytes(final String key) {
        return new CrossRoomClusterCommand<Set<byte[]>>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Set<byte[]> execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.smembersBytes(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("smembersBytes key %s", key);
            }
        }.run(OpEnum.READ);
    }

    public Boolean sismember(final String key, final byte[] member) {
        return new CrossRoomClusterCommand<Boolean>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public Boolean execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.sismember(key, member);
            }

            @Override
            public String getCommandParam() {
                return String.format("sismember key %s, member %s", key, member);
            }
        }.run(OpEnum.READ);
    }

    public String set(final byte[] key, final byte[] value) {
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

    public byte[] get(final byte[] key) {
        return new CrossRoomClusterCommand<byte[]>(majorPipelineCluster, minorPipelineCluster) {
            @Override
            public byte[] execute(PipelineCluster pipelineCluster) {
                return pipelineCluster.get(key);
            }

            @Override
            public String getCommandParam() {
                return String.format("get key %s", key);
            }
        }.run(OpEnum.READ);
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
