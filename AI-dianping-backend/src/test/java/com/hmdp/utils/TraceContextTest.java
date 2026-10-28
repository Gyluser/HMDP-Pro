package com.hmdp.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

class TraceContextTest {

    @AfterEach
    void cleanUp() {
        TraceContext.clear();
    }

    @Test
    void shouldGenerateAndKeepTraceIdWhenContextIsEmpty() {
        String first = TraceContext.currentTraceId();
        String second = TraceContext.currentTraceId();

        assertThat(first).isEqualTo(second);
        assertThat(MDC.get("traceId")).isEqualTo(first);
    }

    @Test
    void shouldRemoveThreadLocalAndMdcWhenCleared() {
        TraceContext.set("trace-to-clear");

        TraceContext.clear();

        assertThat(MDC.get("traceId")).isNull();
        assertThat(TraceContext.currentTraceId()).isNotEqualTo("trace-to-clear");
    }
}
