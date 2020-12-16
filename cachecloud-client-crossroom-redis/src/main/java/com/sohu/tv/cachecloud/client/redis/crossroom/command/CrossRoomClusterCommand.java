package com.sohu.tv.cachecloud.client.redis.crossroom.command;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.Tracer;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.slots.block.degrade.circuitbreaker.CircuitBreakerStrategy;
import com.google.common.collect.Lists;
import com.sohu.tv.cachecloud.client.redis.crossroom.enums.OpEnum;
import com.sohu.tv.cachecloud.client.redis.crossroom.exception.RedisCrossRoomReadMinorFallbackException;
import com.sohu.tv.cachecloud.client.redis.crossroom.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.PipelineCluster;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by wenruiwu
 */
public abstract class CrossRoomClusterCommand<T> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * 定义sentinel resource
     */
    private static final String MAJOR_READ_COMMAND_KEY = "major_read_command";
    private static final String MAJOR_WRITE_COMMAND_KEY = "major_write_command";
    private static final String MINOR_READ_COMMAND_KEY = "minor_read_command";
    private static final String MINOR_WRITE_COMMAND_KEY = "minor_write_command";
    /**
     * 定义sentinel规则阈值
     */
    private static final int SLOW_RATIO_RT = 1000;               //慢调用阈值1000ms
    private static final double SLOW_RATIO_THRESHOLD = 0.5D;     //慢调用比例50%
    private static final double ERROR_RATIO = 0.5D;             //异常比例
    private static final int TIME_WINDOW = 5;                   //熔断窗口5s
    /**
     * 定义写操作线程池
     */
    private static final int WRITE_POOL_SIZE = 50;
    private static final ThreadPoolExecutor MINOR_WRITE_POOL = new ThreadPoolExecutor(WRITE_POOL_SIZE, WRITE_POOL_SIZE,
            0L, TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            new NamedThreadFactory("write-minor-thread-"));
    /**
     * 主
     */
    private final PipelineCluster majorPipelineCluster;
    /**
     * 副
     */
    private final PipelineCluster minorPipelineCluster;
    /**
     * 副机房写操作结果
     */
    public final static ThreadLocal<Future> threadLocal = new ThreadLocal<>();

    public CrossRoomClusterCommand(PipelineCluster majorPipelineCluster, PipelineCluster minorPipelineCluster) {
        this.majorPipelineCluster = majorPipelineCluster;
        this.minorPipelineCluster = minorPipelineCluster;
    }

    public abstract T execute(PipelineCluster pipelineCluster);

    public abstract String getCommandParam();

    public T run(OpEnum opType) {
        //读操作
        if (opType == OpEnum.READ) {
            return read();
        } else {    //写操作
            threadLocal.set(null);
            return write();
        }
    }

    private T read() {
        Entry majorEntry = null;
        long start = System.currentTimeMillis();
        try {
            majorEntry = SphU.entry(MAJOR_READ_COMMAND_KEY);
            return execute(majorPipelineCluster);
        } catch (BlockException e1) {    //主机房熔断，从副机房读
            logger.error("Read from major redis blocked, cost {} ms. Will read from minor redis in the next {}s window. Params {}",
                    (System.currentTimeMillis() - start), TIME_WINDOW, getCommandParam(), e1);
            return handleReadFallback();
        } catch (Throwable e4) {    //其他异常
            Tracer.traceEntry(e4, majorEntry);
            logger.error("Read from major redis error, cost {} ms. This read will fallback to minor. Params {}",
                    (System.currentTimeMillis() - start), e4, getCommandParam(), e4);
            //从副机房读
            return handleReadFallback();
        } finally {
            if (majorEntry != null) {
                majorEntry.exit();
            }
        }
    }

    private T handleReadFallback() {
        Entry minorEntry = null;
        long start = System.currentTimeMillis();
        try {
            minorEntry = SphU.entry(MINOR_READ_COMMAND_KEY);
            return execute(minorPipelineCluster);
        } catch (BlockException e2) {
            logger.error("Read from minor redis blocked!");
            throw new RedisCrossRoomReadMinorFallbackException("MinorFallbackException");
        } catch (Throwable e3) {
            Tracer.traceEntry(e3, minorEntry);
            logger.error("Read from minor redis error, cost {} ms. Params {}",
                    (System.currentTimeMillis() - start), e3, getCommandParam());
            throw e3;
        } finally {
            if (minorEntry != null) {
                minorEntry.exit();
            }
        }
    }

    private T write() {
        //异步写副机房
        Future<T> minorFuture = MINOR_WRITE_POOL.submit(() -> {
            T minorResult = null;
            Entry minorEntry = null;
            long start0 = System.currentTimeMillis();
            try {
                minorEntry = SphU.entry(MINOR_WRITE_COMMAND_KEY);
                minorResult = execute(minorPipelineCluster);
            } catch (BlockException e1) {
                logger.error("Write minor redis blocked, cost {} ms. Params {}",
                        (System.currentTimeMillis() - start0), getCommandParam());
            } catch (Throwable e2) {
                Tracer.traceEntry(e2, minorEntry);
                logger.error("Write minor redis error, cost {} ms. Params {}",
                        (System.currentTimeMillis() - start0), getCommandParam());
            } finally {
                if (minorEntry != null) {
                    minorEntry.exit();
                }
            }
            return minorResult;
        });
        threadLocal.set(minorFuture);
        //同步写主机房
        T majorResult = null;
        Entry majorEntry = null;
        long start = System.currentTimeMillis();
        try {
            majorEntry = SphU.entry(MAJOR_WRITE_COMMAND_KEY);
            majorResult = execute(majorPipelineCluster);
        } catch (BlockException e1) {
            logger.error("Write major redis blocked, cost {} ms. Params {}",
                    (System.currentTimeMillis() - start), getCommandParam(), e1);
        } catch (Throwable e2) {
            Tracer.traceEntry(e2, majorEntry);
            logger.error("Write major redis error, cost {} ms. Params {}",
                    (System.currentTimeMillis() - start), getCommandParam(), e2);
        } finally {
            if (majorEntry != null) {
                majorEntry.exit();
            }
        }
        return majorResult;
    }

    static {
        initRules();
    }

    static void initRules() {
        //主机房读：异常比例规则
        DegradeRule majorReadExpRule = new DegradeRule();
        majorReadExpRule.setResource(MAJOR_READ_COMMAND_KEY);
        majorReadExpRule.setGrade(CircuitBreakerStrategy.ERROR_RATIO.getType());
        majorReadExpRule.setCount(ERROR_RATIO);
        majorReadExpRule.setTimeWindow(TIME_WINDOW);
        //主机房读：慢调用规则
        DegradeRule majorReadSlowRatioRule = new DegradeRule();
        majorReadSlowRatioRule.setResource(MAJOR_READ_COMMAND_KEY);
        majorReadSlowRatioRule.setGrade(CircuitBreakerStrategy.SLOW_REQUEST_RATIO.getType());
        majorReadSlowRatioRule.setCount(SLOW_RATIO_RT);
        majorReadSlowRatioRule.setSlowRatioThreshold(SLOW_RATIO_THRESHOLD);
        majorReadSlowRatioRule.setTimeWindow(TIME_WINDOW);
        //主机房写：异常比例规则
        DegradeRule majorWriteExpRule = new DegradeRule();
        majorWriteExpRule.setResource(MAJOR_WRITE_COMMAND_KEY);
        majorWriteExpRule.setGrade(CircuitBreakerStrategy.ERROR_RATIO.getType());
        majorWriteExpRule.setCount(ERROR_RATIO);
        majorWriteExpRule.setTimeWindow(TIME_WINDOW);
        //主机房写：慢调用规则
        DegradeRule majorWriteSlowRatioRule = new DegradeRule();
        majorWriteSlowRatioRule.setResource(MAJOR_WRITE_COMMAND_KEY);
        majorWriteSlowRatioRule.setGrade(CircuitBreakerStrategy.SLOW_REQUEST_RATIO.getType());
        majorWriteSlowRatioRule.setCount(SLOW_RATIO_RT);
        majorWriteSlowRatioRule.setSlowRatioThreshold(SLOW_RATIO_THRESHOLD);
        majorWriteSlowRatioRule.setTimeWindow(TIME_WINDOW);
        //副机房读：异常比例规则
        DegradeRule minorReadExpRule = new DegradeRule();
        minorReadExpRule.setResource(MINOR_READ_COMMAND_KEY);
        minorReadExpRule.setGrade(CircuitBreakerStrategy.ERROR_RATIO.getType());
        minorReadExpRule.setCount(ERROR_RATIO);
        minorReadExpRule.setTimeWindow(TIME_WINDOW);
        //副机房读：慢调用规则
        DegradeRule minorReadSlowRatioRule = new DegradeRule();
        minorReadSlowRatioRule.setResource(MINOR_READ_COMMAND_KEY);
        minorReadSlowRatioRule.setGrade(CircuitBreakerStrategy.SLOW_REQUEST_RATIO.getType());
        minorReadSlowRatioRule.setCount(SLOW_RATIO_RT);
        minorReadSlowRatioRule.setSlowRatioThreshold(SLOW_RATIO_THRESHOLD);
        minorReadSlowRatioRule.setTimeWindow(TIME_WINDOW);
        //副机房写：异常比例规则
        DegradeRule minorWriteExpRule = new DegradeRule();
        minorWriteExpRule.setResource(MINOR_WRITE_COMMAND_KEY);
        minorWriteExpRule.setGrade(CircuitBreakerStrategy.ERROR_RATIO.getType());
        minorWriteExpRule.setCount(ERROR_RATIO);
        minorWriteExpRule.setTimeWindow(TIME_WINDOW);
        //副机房写：慢调用规则
        DegradeRule minorWriteSlowRatioRule = new DegradeRule();
        minorWriteSlowRatioRule.setResource(MINOR_WRITE_COMMAND_KEY);
        minorWriteSlowRatioRule.setGrade(CircuitBreakerStrategy.SLOW_REQUEST_RATIO.getType());
        minorWriteSlowRatioRule.setCount(SLOW_RATIO_RT);
        minorWriteSlowRatioRule.setSlowRatioThreshold(SLOW_RATIO_THRESHOLD);
        minorWriteSlowRatioRule.setTimeWindow(TIME_WINDOW);

        List<DegradeRule> rules = Lists.newArrayList(
                majorReadExpRule,
                majorReadSlowRatioRule,
                majorWriteExpRule,
                majorWriteSlowRatioRule,
                minorReadExpRule,
                minorReadSlowRatioRule,
                minorWriteExpRule,
                minorWriteSlowRatioRule
        );
        DegradeRuleManager.loadRules(rules);
    }
}
