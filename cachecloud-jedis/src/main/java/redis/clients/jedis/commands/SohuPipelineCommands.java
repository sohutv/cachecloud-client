package redis.clients.jedis.commands;

import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.valueobject.BitOffsetValue;
import redis.clients.jedis.valueobject.RangeRankVO;
import redis.clients.jedis.valueobject.RangeScoreVO;
import redis.clients.jedis.valueobject.SortedSetVO;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SohuPipelineCommands {

    Map<String, String> mget(final List<String> keys);

    Map<String, Boolean> mGetbit(final Map<String, Long> keyOffsetMap);

    Map<String, Boolean> mSetbit(final Map<String, BitOffsetValue> keyOffsetValueMap);

    Map<String, Long> mexpire(final Map<String, Integer> keyTimeMap);

    Map<String, Map<String, String>> mHgetAll(List<String> keys);

    Map<String, List<String>> mhmget(final Map<String, List<String>> keyValueMap);

    Map<String, String> mhmset(final Map<String, Map<String, String>> keyValueMap);

    String mset(final Map<String, String> keyValueMap);

    String msetnx(final Map<String, String> keyValueMap);

    Map<String, Long> msetnxs(final Map<String, String> keyValueMap);

    Map<String, Long> mhsetnx(final Map<String, Map<String, String>> keyValueMap);

    String msetex(final Map<String, String> keyValueMap, final int seconds);

    String mset(final Map<String, String> keyValueMap, final SetParams params);

    Map<String, Long> mincrBy(final Map<String, Long> keyValueMap);

    Long mdel(final List<String> keys);

    Map<String, Long> mzadd(final Map<String, SortedSetVO> map);

    Map<String, Long> mzadds(final Map<String, Map<String, Double>> map);

    Map<String, Long> mzrem(final Map<String, String[]> map);

    Map<String, Long> mzcard(final List<String> keys);

    Map<String, Long> mzrank(final Map<String,String> map);

    Map<String, Boolean> msismember(final Map<String,List<String>> map);

    Map<String, Set<String>> mzrangeByScore(final List<String> keys, final double min,
                                            final double max);

    Map<String, Set<Tuple>> mzrevrangeWithScores(final Map<String, RangeRankVO> keyRankMap);

    Map<String, Set<String>> mzrangeByScore(final Map<String, RangeScoreVO> keyScoreMap);

    Map<String, Set<String>> mzrangeByScore(final List<String> keys, final String min, final String max);

    String mzremrangeByScore(final Map<String, RangeScoreVO> keyScoreMap);

    Map<String, byte[]> mgetBytes(final List<String> keys);

    String msetBytes(final Map<String, byte[]> keyValueMap);

    Map<String, Double> mzscore(final Map<String, String> keyMemberMap);

    byte[] getBytes(final String key);

}
