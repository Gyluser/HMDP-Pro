package com.hmdp.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.LongAdder;

/** 秒杀 MQ 的轻量运行指标，不依赖额外监控平台。 */
@Slf4j
@Component
public class SeckillMqMetrics {
    private final LongAdder requests = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder published = new LongAdder();
    private final LongAdder publishFailures = new LongAdder();
    private final LongAdder consumed = new LongAdder();
    private final LongAdder consumeFailures = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final LongAdder deadLetters = new LongAdder();
    private final LongAdder compensated = new LongAdder();

    public void request() { requests.increment(); }
    public void rejected() { rejected.increment(); }
    public void published() { published.increment(); }
    public void publishFailure() { publishFailures.increment(); }
    public void consumed() { consumed.increment(); }
    public void consumeFailure() { consumeFailures.increment(); }
    public void retried() { retries.increment(); }
    public void deadLettered() { deadLetters.increment(); }
    public void compensated() { compensated.increment(); }

    @Scheduled(fixedDelay = 60_000)
    public void logSnapshot() {
        if (requests.sum() == 0 && consumed.sum() == 0 && deadLetters.sum() == 0) {
            return;
        }
        log.info("秒杀 MQ 指标 request={}, rejected={}, published={}, publishFailure={}, consumed={}, "
                        + "consumeFailure={}, retry={}, deadLetter={}, compensated={}",
                requests.sum(), rejected.sum(), published.sum(), publishFailures.sum(), consumed.sum(),
                consumeFailures.sum(), retries.sum(), deadLetters.sum(), compensated.sum());
    }
}
