package redis.clients.jedis;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.Tracer;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.RedisCrossRoomReadMinorFallbackException;
import redis.clients.jedis.util.NamedThreadFactory;
import redis.clients.jedis.util.OpEnum;
import redis.clients.jedis.util.SentinelResources;

import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by wenruiwu
 */
public abstract class CrossRoomClusterCommand<T> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
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
            majorEntry = SphU.entry(SentinelResources.MAJOR_READ_COMMAND_KEY);
            return execute(majorPipelineCluster);
        } catch (BlockException e1) {    //主机房熔断，从副机房读
            logger.error("Read from major redis blocked, cost {} ms. Will read from minor redis. Params {}",
                    (System.currentTimeMillis() - start), getCommandParam(), e1);
            return handleReadFallback();
        } catch (Throwable e4) {    //其他异常
            Tracer.traceEntry(e4, majorEntry);
            logger.error("Read from major redis error, cost {} ms. This read will fallback to minor. Params {}",
                    (System.currentTimeMillis() - start), getCommandParam(), e4);
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
            minorEntry = SphU.entry(SentinelResources.MINOR_READ_COMMAND_KEY);
            return execute(minorPipelineCluster);
        } catch (BlockException e2) {
            logger.error("Read from minor redis blocked!");
            throw new RedisCrossRoomReadMinorFallbackException("MinorFallbackException");
        } catch (Throwable e3) {
            Tracer.traceEntry(e3, minorEntry);
            logger.error("Read from minor redis error, cost {} ms. Params {}",
                    (System.currentTimeMillis() - start), getCommandParam());
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
                minorEntry = SphU.entry(SentinelResources.MINOR_WRITE_COMMAND_KEY);
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
            majorEntry = SphU.entry(SentinelResources.MAJOR_WRITE_COMMAND_KEY);
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
}
