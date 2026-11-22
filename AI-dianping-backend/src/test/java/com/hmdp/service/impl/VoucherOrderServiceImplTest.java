package com.hmdp.service.impl;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.messaging.MessagingException;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VoucherOrderServiceImplTest {

    @Test
    void shouldTreatRocketMqSendRequestTimeoutAsUncertainDelivery() {
        VoucherOrderServiceImpl voucherOrderService = new VoucherOrderServiceImpl();
        MessagingException exception = new MessagingException("send timeout",
                new org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException("send timeout"));

        Boolean timedOut = ReflectionTestUtils.invokeMethod(voucherOrderService, "hasTimeoutCause", exception);

        assertThat(timedOut).isTrue();
    }

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

    @Test
    void shouldUseWatchDogLockInsteadOfFixedLease() throws Exception {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        RedissonClient redissonClient = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        VoucherOrderMapper voucherOrderMapper = mock(VoucherOrderMapper.class);
        when(redissonClient.getLock(anyString())).thenReturn(lock);
        when(lock.tryLock(2, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        when(redisTemplate.hasKey(anyString())).thenReturn(false);
        when(voucherOrderMapper.selectById(1L)).thenReturn(null);

        VoucherOrderServiceImpl voucherOrderService = new VoucherOrderServiceImpl();
        ReflectionTestUtils.setField(voucherOrderService, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(voucherOrderService, "redissonClient", redissonClient);
        ReflectionTestUtils.setField(voucherOrderService, "baseMapper", voucherOrderMapper);
        VoucherOrder order = new VoucherOrder().setId(1L).setUserId(2L).setVoucherId(3L);

        voucherOrderService.handleVoucherOrderByMq(order);

        verify(lock).tryLock(2, TimeUnit.SECONDS);
        verify(lock).unlock();
    }
}
