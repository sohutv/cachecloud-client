package com.sohu.tv.cachecloud.client.redis.crossroom.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yijunzhang on 14-7-18.
 */
public class NamedThreadFactory implements ThreadFactory {
    private final AtomicInteger sequence = new AtomicInteger(1);
    private final String prefix;

    public NamedThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        int seq = sequence.getAndIncrement();
        thread.setName(prefix + (seq > 1 ? "-" + seq : ""));
        if (!thread.isDaemon())
            thread.setDaemon(true);
        return thread;
    }
}
