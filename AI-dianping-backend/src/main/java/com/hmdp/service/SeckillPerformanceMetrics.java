package com.hmdp.service;

import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Local performance counters for the seckill request's synchronous path.
 *
 * <p>Histograms keep one bucket per millisecond up to five seconds. They are
 * deliberately lightweight and are exposed only when the perf endpoint is
 * explicitly enabled. Redis timings include the client round trip; RocketMQ
 * timings end when {@code syncSend} returns from the broker acknowledgement.</p>
 */
@Component
public class SeckillPerformanceMetrics {
    private final LatencyHistogram redisLua = new LatencyHistogram();
    private final LatencyHistogram rocketMqSend = new LatencyHistogram();
    private final LatencyHistogram service = new LatencyHistogram();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();

    public void requestStarted() {
        int current = inFlight.incrementAndGet();
        maxInFlight.accumulateAndGet(current, Math::max);
    }

    public void requestFinished(long startedNanos) {
        service.recordSince(startedNanos);
        inFlight.decrementAndGet();
    }

    public void redisLuaFinished(long startedNanos) {
        redisLua.recordSince(startedNanos);
    }

    public void rocketMqSendFinished(long startedNanos) {
        rocketMqSend.recordSince(startedNanos);
    }

    /** Called before a new isolated pressure run. Refuses to discard live samples. */
    public synchronized boolean resetIfIdle() {
        if (inFlight.get() != 0) {
            return false;
        }
        redisLua.reset();
        rocketMqSend.reset();
        service.reset();
        maxInFlight.set(0);
        return true;
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("inFlight", inFlight.get());
        result.put("maxInFlight", maxInFlight.get());
        result.put("redisLua", redisLua.snapshot());
        result.put("rocketMqSend", rocketMqSend.snapshot());
        result.put("service", service.snapshot());
        result.put("jvmThreads", threadSnapshot());
        return result;
    }

    private Map<String, Integer> threadSnapshot() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        Map<String, Integer> states = new LinkedHashMap<>();
        for (Thread.State state : Thread.State.values()) {
            states.put(state.name().toLowerCase(), 0);
        }
        ThreadInfo[] infos = bean.getThreadInfo(bean.getAllThreadIds());
        for (ThreadInfo info : infos) {
            if (info != null) {
                String key = info.getThreadState().name().toLowerCase();
                states.put(key, states.get(key) + 1);
            }
        }
        states.put("live", bean.getThreadCount());
        states.put("peak", bean.getPeakThreadCount());
        return states;
    }

    private static final class LatencyHistogram {
        private static final int MAX_BUCKET_MS = 5_000;
        private final AtomicLongArray buckets = new AtomicLongArray(MAX_BUCKET_MS + 1);
        private final LongAdder count = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final AtomicLong maxNanos = new AtomicLong();

        void recordSince(long startedNanos) {
            recordNanos(System.nanoTime() - startedNanos);
        }

        void recordNanos(long elapsedNanos) {
            long safeNanos = Math.max(0L, elapsedNanos);
            int millis = (int) Math.min(MAX_BUCKET_MS, TimeUnit.NANOSECONDS.toMillis(safeNanos));
            buckets.incrementAndGet(millis);
            count.increment();
            totalNanos.add(safeNanos);
            maxNanos.accumulateAndGet(safeNanos, Math::max);
        }

        synchronized void reset() {
            for (int i = 0; i <= MAX_BUCKET_MS; i++) {
                buckets.set(i, 0L);
            }
            count.reset();
            totalNanos.reset();
            maxNanos.set(0L);
        }

        Map<String, Object> snapshot() {
            long sampleCount = count.sum();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("count", sampleCount);
            if (sampleCount == 0) {
                result.put("meanMs", 0);
                result.put("p50Ms", 0);
                result.put("p95Ms", 0);
                result.put("p99Ms", 0);
                result.put("maxMs", 0);
                return result;
            }
            result.put("meanMs", Math.round(totalNanos.sum() / (sampleCount * 1_000_000.0)));
            result.put("p50Ms", percentile(sampleCount, 0.50));
            result.put("p95Ms", percentile(sampleCount, 0.95));
            result.put("p99Ms", percentile(sampleCount, 0.99));
            result.put("maxMs", TimeUnit.NANOSECONDS.toMillis(maxNanos.get()));
            return result;
        }

        private int percentile(long sampleCount, double quantile) {
            long required = (long) Math.ceil(sampleCount * quantile);
            long seen = 0;
            for (int i = 0; i <= MAX_BUCKET_MS; i++) {
                seen += buckets.get(i);
                if (seen >= required) {
                    return i;
                }
            }
            return MAX_BUCKET_MS;
        }
    }
}
