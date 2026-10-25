package com.hmdp.service.impl;

import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Set;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VoucherOrderServiceImplTest {

    @Test
    void shouldRemoveMalformedReservationIndexAndContinue() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ZSetOperations<String, String> zSetOperations = mock(ZSetOperations.class);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(RedisConstants.SECKILL_RESERVATION_INDEX_KEY),
                anyDouble(), anyDouble(), anyLong(), anyLong()))
                .thenReturn(Set.of("order-race-invalid"));
        VoucherOrderServiceImpl voucherOrderService = new VoucherOrderServiceImpl();
        ReflectionTestUtils.setField(voucherOrderService, "stringRedisTemplate", redisTemplate);

        voucherOrderService.compensateExpiredReservations();

        verify(zSetOperations).remove(RedisConstants.SECKILL_RESERVATION_INDEX_KEY, "order-race-invalid");
    }
}
