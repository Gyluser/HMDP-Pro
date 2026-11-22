package com.hmdp.utils;

import org.slf4j.MDC;

import java.util.UUID;

/** HTTP 请求与 RocketMQ 消息之间传递的轻量 TraceId 上下文。用 ThreadLocal 区分不同线程。 */
public final class TraceContext {
    public static final String TRACE_ID_HEADER = "X-Trace-Id";
    private static final String MDC_KEY = "traceId";
    private static final ThreadLocal<String> TRACE_ID = new ThreadLocal<>();

    private TraceContext() {
    }

    public static String currentTraceId() {
        String traceId = TRACE_ID.get();
        if (traceId == null) {
            traceId = UUID.randomUUID().toString();
            set(traceId);
        }
        return traceId;
    }

    public static void set(String traceId) {
        TRACE_ID.set(traceId);
        MDC.put(MDC_KEY, traceId);
    }

    public static void clear() {
        TRACE_ID.remove();
        MDC.remove(MDC_KEY);
    }
}
