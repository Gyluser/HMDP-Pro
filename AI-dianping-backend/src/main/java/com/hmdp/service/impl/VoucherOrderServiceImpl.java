package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.RocketMQConfig;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.SeckillMqMetrics;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.TraceContext;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import jakarta.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final long RESERVATION_TIMEOUT_SECONDS = 300L;
    private static final long PUBLISH_CONFIRM_TIMEOUT_SECONDS = 3L;
    private static final String MAIN_MESSAGE_KIND = "seckill-main";

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RocketMQTemplate rocketMQTemplate;
    @Resource
    private SeckillMqMetrics seckillMqMetrics;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT = script("seckill.lua");
    private static final DefaultRedisScript<Long> CONFIRM_RESERVATION_SCRIPT = script("confirm_reservation.lua");
    private static final DefaultRedisScript<Long> RELEASE_RESERVATION_SCRIPT = script("release_reservation.lua");
    private static DefaultRedisScript<Long> script(String location) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(Long.class);
        return script;
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        seckillMqMetrics.request();
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行seckill.lua，检查库存，检查用户是否重复购买，扣减 Redis 库存，记录用户抢购标记，记录 reservation
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),  //execute(脚本对象, KEYS列表, ARGV参数...)
                voucherId.toString(), userId.toString(), String.valueOf(orderId));

        // 2.执行失败
        if (result == null || result != 0L) {
            seckillMqMetrics.rejected();
            return Result.fail(result != null && result == 1L ? "库存不足" : "不能重复下单");
        }

        // 3.执行成功则构造订单对象
        VoucherOrder order = new VoucherOrder().setId(orderId).setUserId(userId).setVoucherId(voucherId);
        // 4.发送到 RocketMQ 主 Topic；SYNC_SEND 返回 SEND_OK 才认为 Broker 已接收。
        try {
            Message<VoucherOrder> message = MessageBuilder.withPayload(order)
                    .setHeader("message-kind", MAIN_MESSAGE_KIND)
                    .setHeader("orderId", String.valueOf(orderId))
                    .setHeader("x-retry-count", 0)
                    .setHeader(TraceContext.TRACE_ID_HEADER, TraceContext.currentTraceId())
                    .build();

            SendResult sendResult = rocketMQTemplate.syncSend(RocketMQConfig.SECKILL_ORDER_TOPIC, message,
                    TimeUnit.SECONDS.toMillis(PUBLISH_CONFIRM_TIMEOUT_SECONDS));

            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                releaseReservation(orderId);    // 释放redis预扣
                seckillMqMetrics.publishFailure();
                return Result.fail("订单消息发送失败，请稍后重试");
            }
            // 正常完成：Redis 预扣成功，RocketMQ 已接收消息。
            seckillMqMetrics.published();
            return Result.ok(orderId);
        } catch (org.springframework.messaging.MessagingException e) {
            if (hasTimeoutCause(e)) {
                // 网络超时的最终写入状态未知，保留 Redis 预留状态交由补偿任务判定。
                log.warn("秒杀订单 RocketMQ 发送超时，orderId={}", orderId);
                seckillMqMetrics.publishFailure();
                return Result.fail("订单正在处理中，请稍后查询");
            }
            log.error("秒杀订单消息发送异常，orderId={}", orderId, e);
            releaseReservation(orderId);
            seckillMqMetrics.publishFailure();
            return Result.fail("订单消息发送失败，请稍后重试");
        } catch (Exception e) {
            log.error("秒杀订单消息发送异常，orderId={}", orderId, e);
            releaseReservation(orderId);
            seckillMqMetrics.publishFailure();
            return Result.fail("订单消息发送失败，请稍后重试");
        }
    }

    private boolean hasTimeoutCause(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof org.apache.rocketmq.remoting.exception.RemotingTimeoutException
                    || current instanceof org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    @Override
    @Transactional
    public void handleVoucherOrderByMq(VoucherOrder voucherOrder) {
        Long orderId = voucherOrder.getId();
        // 创建订单级别的锁，避免多个消费者操作一个订单
        RLock lock = redissonClient.getLock(RedisConstants.SECKILL_RESERVATION_LOCK_KEY + orderId);
        boolean locked = false;
        try {
            // 不传 leaseTime，使用 Redisson Watch Dog 在业务线程存活期间自动续期，
            // 避免数据库瞬时变慢导致固定 30 秒租约过期。
            locked = lock.tryLock(2, TimeUnit.SECONDS);
            if (!locked) {
                throw new IllegalStateException("无法获取秒杀订单处理锁");
            }
            if (!reservationExists(orderId)) {
                //Redis 预留没有了，数据库订单也没有，说明这笔消息已经超时，补偿任务已经把 Redis 库存恢复，此时不能再次创建订单
                if (getById(orderId) == null) {
                    log.warn("忽略已补偿的过期秒杀消息，orderId={}", orderId);
                }
                return;
            }
            // 按 orderId 做第一层幂等，订单已经存在则不再操作
            if (getById(orderId) != null) {
                confirmReservationAfterCommit(orderId);
                return;
            }
            // 按用户和优惠券做第二层幂等，查询这个用户是否已经购买过这张优惠券，如果已有，则不再操作
            Long duplicateCount = query().eq("user_id", voucherOrder.getUserId())
                    .eq("voucher_id", voucherOrder.getVoucherId()).count();
            if (duplicateCount > 0) {
                releaseReservationAfterCommit(voucherOrder);
                return;
            }
            // 最后才扣数据库的库存
            boolean stockUpdated = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0)
                    .update();
            if (!stockUpdated) {
                throw new IllegalStateException("数据库库存不足");
            }
            if (!save(voucherOrder)) {
                throw new IllegalStateException("创建秒杀订单失败");
            }
            // 数据库操作成功后才确认 Redis 预留
            confirmReservationAfterCommit(orderId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("获取秒杀订单处理锁时被中断", e);
        } finally {
            // 确实拿到了锁，并且锁还归当前线程所有
            if (locked && lock.isHeldByCurrentThread()) {
                // @Transactional 的提交发生在方法返回后，锁也必须延迟到事务完成后释放，
                // 否则补偿任务可能在提交窗口内把尚未可见的订单当成失败订单回补。
                if (TransactionSynchronizationManager.isSynchronizationActive()) {
                    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                        @Override
                        public void afterCompletion(int status) {
                            if (lock.isHeldByCurrentThread()) {
                                lock.unlock();
                            }
                        }
                    });
                } else {    // 当前分支根本没有事务了
                    lock.unlock();
                }
            }
        }
    }

    private boolean reservationExists(Long orderId) {
        return Boolean.TRUE.equals(stringRedisTemplate.hasKey(RedisConstants.SECKILL_RESERVATION_KEY + orderId));
    }

    private void confirmReservationAfterCommit(Long orderId) {
        // 判断当前是否在事务中？
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            // 已经不在事务里：没有“提交后”这个概念，直接确认 Redis 预留即可。
            confirmReservation(orderId);
            return;
        }

        //在事务里：不能立刻确认，继续往下注册回调。
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() { // 重写aftercommit函数，提交之后再确认缓存
                confirmReservation(orderId);
            }
        });
    }

    private void releaseReservationAfterCommit(VoucherOrder order) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            releaseReservation(order.getId(), order.getUserId(), order.getVoucherId());
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                releaseReservation(order.getId(), order.getUserId(), order.getVoucherId());
            }
        });
    }

    private void confirmReservation(Long orderId) {
        stringRedisTemplate.execute(CONFIRM_RESERVATION_SCRIPT,
                Arrays.asList(RedisConstants.SECKILL_RESERVATION_KEY + orderId,
                        RedisConstants.SECKILL_RESERVATION_INDEX_KEY), String.valueOf(orderId));
    }

    public void releaseReservation(Long orderId) {
        Map<Object, Object> reservation = stringRedisTemplate.opsForHash()
                .entries(RedisConstants.SECKILL_RESERVATION_KEY + orderId);
        if (reservation.isEmpty()) {
            return;
        }
        releaseReservation(orderId,
                Long.valueOf(String.valueOf(reservation.get("userId"))),
                Long.valueOf(String.valueOf(reservation.get("voucherId"))));
    }

    private void releaseReservation(Long orderId, Long userId, Long voucherId) {
        stringRedisTemplate.execute(RELEASE_RESERVATION_SCRIPT,
                Arrays.asList(RedisConstants.SECKILL_RESERVATION_KEY + orderId,
                        RedisConstants.SECKILL_RESERVATION_INDEX_KEY,
                        RedisConstants.SECKILL_STOCK_KEY + voucherId,
                        RedisConstants.SECKILL_ORDER_KEY + voucherId),
                String.valueOf(orderId));
    }

    @Scheduled(fixedDelay = 60_000)
    public void compensateExpiredReservations() {
        long cutoff = System.currentTimeMillis() / 1000 - RESERVATION_TIMEOUT_SECONDS;
//        查询当前预留时间长超过五分钟的订单
        Set<String> orderIds = stringRedisTemplate.opsForZSet().rangeByScore(
                RedisConstants.SECKILL_RESERVATION_INDEX_KEY, 0, cutoff, 0, 100);
        if (orderIds == null || orderIds.isEmpty()) {
            return;
        }
        for (String orderIdText : orderIds) {
            Long orderId;
            try {
                orderId = Long.valueOf(orderIdText);
            } catch (NumberFormatException e) {
                // 索引中的成员只能是 RedisIdWorker 生成的数字订单 ID。遇到历史测试或
                // 手工残留的非法成员时，清理该成员并继续补偿其他正常订单。
                stringRedisTemplate.opsForZSet().remove(RedisConstants.SECKILL_RESERVATION_INDEX_KEY, orderIdText);
                log.warn("清理非法秒杀预留索引，orderId={}", orderIdText);
                continue;
            }
            RLock lock = redissonClient.getLock(RedisConstants.SECKILL_RESERVATION_LOCK_KEY + orderId);
            boolean locked = false;
            try {
                locked = lock.tryLock(0, 10, TimeUnit.SECONDS);
                if (!locked) {
                    continue;
                }
                if (getById(orderId) != null) {
                    confirmReservation(orderId);
                } else {
                    releaseReservation(orderId);
                    seckillMqMetrics.compensated();
                    log.warn("已回补超时秒杀预扣，orderId={}", orderId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("补偿秒杀预扣失败，orderId={}", orderId, e);
            } finally {
                if (locked && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
    }
}
