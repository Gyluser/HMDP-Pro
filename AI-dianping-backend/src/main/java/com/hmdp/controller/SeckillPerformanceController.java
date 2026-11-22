package com.hmdp.controller;

import com.hmdp.service.SeckillPerformanceMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Local-only observability endpoint. It is absent unless explicitly enabled. */
@RestController
@RequestMapping("/internal/perf/seckill")
@ConditionalOnProperty(name = "hmdp.perf-metrics.enabled", havingValue = "true")
public class SeckillPerformanceController {
    private final SeckillPerformanceMetrics metrics;

    public SeckillPerformanceController(SeckillPerformanceMetrics metrics) {
        this.metrics = metrics;
    }

    @GetMapping
    public Map<String, Object> snapshot() {
        return metrics.snapshot();
    }

    @PostMapping("/reset")
    public ResponseEntity<Map<String, Object>> reset() {
        if (!metrics.resetIfIdle()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("reset", false, "reason", "seckill requests are still in flight"));
        }
        return ResponseEntity.ok(Map.of("reset", true));
    }
}
