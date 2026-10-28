package com.hmdp;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.ScanOptions;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** 本机 Redis/RabbitMQ 集成测试：仅使用临时秒杀 Key，结束后清理。 */
@SpringBootTest(properties = "spring.task.scheduling.enabled=false")
class RabbitMqSeckillIntegrationTests {

    @Autowired
    private AmqpAdmin amqpAdmin;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private String voucherId;
    private String userId;
    private String orderId;
    private final Set<String> testVoucherIds = ConcurrentHashMap.newKeySet();
    private final Set<String> testOrderIds = ConcurrentHashMap.newKeySet();

    @Test
    void shouldDeclareRabbitMqTopology() {
        assertThat(amqpAdmin.getQueueProperties(RabbitMQConfig.SECKILL_ORDER_QUEUE)).isNotNull();
        assertThat(amqpAdmin.getQueueProperties(RabbitMQConfig.SECKILL_ORDER_RETRY_5S_QUEUE)).isNotNull();
        assertThat(amqpAdmin.getQueueProperties(RabbitMQConfig.SECKILL_ORDER_RETRY_30S_QUEUE)).isNotNull();
        assertThat(amqpAdmin.getQueueProperties(RabbitMQConfig.SECKILL_ORDER_DLX_QUEUE)).isNotNull();
    }

    @Test
    void shouldReserveConfirmAndReleaseSeckillStateAtomically() {
        voucherId = "mq-test-" + System.nanoTime();
        userId = "user-" + System.nanoTime();
        orderId = "order-" + System.nanoTime();
        testVoucherIds.add(voucherId);
        testOrderIds.add(orderId);
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
        String orderKey = RedisConstants.SECKILL_ORDER_KEY + voucherId;
        String reservationKey = RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        stringRedisTemplate.opsForValue().set(stockKey, "1");
        assertThat(stringRedisTemplate.execute(script("seckill.lua"), Collections.emptyList(),
                voucherId, userId, orderId)).isZero();
        assertThat(stringRedisTemplate.opsForValue().get(stockKey)).isEqualTo("0");
        assertThat(stringRedisTemplate.opsForSet().isMember(orderKey, userId)).isTrue();
        assertThat(stringRedisTemplate.opsForHash().get(reservationKey, "userId")).isEqualTo(userId);

        assertThat(stringRedisTemplate.execute(script("confirm_reservation.lua"),
                Arrays.asList(reservationKey, RedisConstants.SECKILL_RESERVATION_INDEX_KEY), orderId)).isEqualTo(1L);
        assertThat(stringRedisTemplate.hasKey(reservationKey)).isFalse();
        assertThat(stringRedisTemplate.opsForValue().get(stockKey)).isEqualTo("0");

        // 再次预扣，验证失败回补时库存和一人一单标记会一起恢复。
        orderId = "order-" + System.nanoTime();
        testOrderIds.add(orderId);
        reservationKey = RedisConstants.SECKILL_RESERVATION_KEY + orderId;
        stringRedisTemplate.opsForSet().remove(orderKey, userId);
        stringRedisTemplate.opsForValue().set(stockKey, "1");
        assertThat(stringRedisTemplate.execute(script("seckill.lua"), Collections.emptyList(),
                voucherId, userId, orderId)).isZero();
        assertThat(stringRedisTemplate.execute(script("release_reservation.lua"),
                Arrays.asList(reservationKey, RedisConstants.SECKILL_RESERVATION_INDEX_KEY, stockKey, orderKey), orderId))
                .isEqualTo(1L);
        assertThat(stringRedisTemplate.opsForValue().get(stockKey)).isEqualTo("1");
        assertThat(stringRedisTemplate.opsForSet().isMember(orderKey, userId)).isFalse();
        assertThat(stringRedisTemplate.hasKey(reservationKey)).isFalse();

        // 回补脚本可重复执行，避免消费者重投或补偿任务重复触发时多加库存。
        assertThat(stringRedisTemplate.execute(script("release_reservation.lua"),
                Arrays.asList(reservationKey, RedisConstants.SECKILL_RESERVATION_INDEX_KEY, stockKey, orderKey), orderId))
                .isZero();
        assertThat(stringRedisTemplate.opsForValue().get(stockKey)).isEqualTo("1");
    }

    @Test
    void shouldAllowOnlyOneReservationWhenManyUsersCompeteForLastStock() throws Exception {
        String concurrentVoucherId = registerVoucher("mq-stock-race-" + System.nanoTime());
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + concurrentVoucherId;
        stringRedisTemplate.opsForValue().set(stockKey, "1");

        int contenders = 24;
        ExecutorService executor = Executors.newFixedThreadPool(contenders);
        CountDownLatch ready = new CountDownLatch(contenders);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Long>> results = IntStream.range(0, contenders)
                    .mapToObj(index -> executor.submit(() -> {
                        String contenderOrderId = registerOrder("order-race-" + index + "-" + System.nanoTime());
                        ready.countDown();
                        assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                        return stringRedisTemplate.execute(script("seckill.lua"), Collections.emptyList(),
                                concurrentVoucherId, "user-race-" + index, contenderOrderId);
                    }))
                    .toList();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            long successCount = 0;
            for (Future<Long> result : results) {
                if (result.get(10, TimeUnit.SECONDS) == 0L) {
                    successCount++;
                }
            }
            assertThat(successCount).isEqualTo(1);
            assertThat(stringRedisTemplate.opsForValue().get(stockKey)).isEqualTo("0");
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRejectConcurrentDuplicateUserWithoutExtraStockDeduction() throws Exception {
        String concurrentVoucherId = registerVoucher("mq-user-race-" + System.nanoTime());
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + concurrentVoucherId;
        String sameUserId = "same-user-" + System.nanoTime();
        stringRedisTemplate.opsForValue().set(stockKey, "2");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Long>> results = IntStream.range(0, 2)
                    .mapToObj(index -> executor.submit(() -> {
                        String concurrentOrderId = registerOrder("order-user-race-" + index + "-" + System.nanoTime());
                        ready.countDown();
                        assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                        return stringRedisTemplate.execute(script("seckill.lua"), Collections.emptyList(),
                                concurrentVoucherId, sameUserId, concurrentOrderId);
                    }))
                    .toList();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            long first = results.get(0).get(10, TimeUnit.SECONDS);
            long second = results.get(1).get(10, TimeUnit.SECONDS);
            assertThat(List.of(first, second)).containsExactlyInAnyOrder(0L, 2L);
            assertThat(stringRedisTemplate.opsForValue().get(stockKey)).isEqualTo("1");
            assertThat(stringRedisTemplate.opsForSet()
                    .isMember(RedisConstants.SECKILL_ORDER_KEY + concurrentVoucherId, sameUserId)).isTrue();
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldNotRetainMalformedReservationIndexAfterCompensation() {
        List<String> malformedOrderIds = new ArrayList<>();
        ScanOptions options = ScanOptions.scanOptions().match("order-race-*").count(100).build();
        try (Cursor<ZSetOperations.TypedTuple<String>> cursor = stringRedisTemplate.opsForZSet()
                .scan(RedisConstants.SECKILL_RESERVATION_INDEX_KEY, options)) {
            cursor.forEachRemaining(item -> malformedOrderIds.add(item.getValue()));
        }
        assertThat(malformedOrderIds).isEmpty();
    }

    @AfterEach
    void cleanUp() {
        if (voucherId != null) {
            testVoucherIds.add(voucherId);
        }
        if (orderId != null) {
            testOrderIds.add(orderId);
        }
        testVoucherIds.stream().filter(java.util.Objects::nonNull).forEach(id -> {
            stringRedisTemplate.delete(RedisConstants.SECKILL_STOCK_KEY + id);
            stringRedisTemplate.delete(RedisConstants.SECKILL_ORDER_KEY + id);
        });
        testOrderIds.stream().filter(java.util.Objects::nonNull).forEach(id -> {
            stringRedisTemplate.delete(RedisConstants.SECKILL_RESERVATION_KEY + id);
            stringRedisTemplate.opsForZSet().remove(RedisConstants.SECKILL_RESERVATION_INDEX_KEY, id);
        });
    }

    private String registerVoucher(String id) {
        testVoucherIds.add(id);
        return id;
    }

    private String registerOrder(String id) {
        testOrderIds.add(id);
        return id;
    }

    private DefaultRedisScript<Long> script(String location) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(Long.class);
        return script;
    }
}
