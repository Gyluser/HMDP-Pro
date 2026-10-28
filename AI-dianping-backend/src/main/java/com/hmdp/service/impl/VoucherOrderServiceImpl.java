package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.RabbitMQConfig;
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
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
    private RabbitTemplate rabbitTemplate;
    @Resource
    private SeckillMqMetrics seckillMqMetrics;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT = script("seckill.lua");
    private static final DefaultRedisScript<Long> CONFIRM_RESERVATION_SCRIPT = script("confirm_reservation.lua");
    private static final DefaultRedisScript<Long> RELEASE_RESERVATION_SCRIPT = script("release_reservation.lua");
    private final Set<Long> returnedMainOrderIds = ConcurrentHashMap.newKeySet();
    //创建线程安全 Set。RabbitMQ 的 Confirm 回调和 Return 回调可能在不同线程执行，因此不能使用普通的 HashSet。
    //这个集合保存：已经被 RabbitMQ Return 回调判定为无法路由的主订单 ID

    private static DefaultRedisScript<Long> script(String location) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(Long.class);
        return script;
    }

    @PostConstruct
    private void configureRabbitCallbacks() {
        // 设置 RabbitMQ 的 mandatory 标记，如果消息无法从交换机路由到任何队列，RabbitMQ 要把消息返回给生产者。
        rabbitTemplate.setMandatory(true);
        // RabbitMQ 对生产者消息进行确认时执行。
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
    //        只有以下条件同时满足，才执行主订单回滚：
    //        1. Broker 没有确认成功
    //        2. correlationData 不为空
    //        3. correlationData ID 不为空
    //        4. ID 以 main: 开头，说明是主订单消息
            if (!ack && correlationData != null && correlationData.getId() != null
                    && correlationData.getId().startsWith("main:")) {
                Long orderId = Long.valueOf(correlationData.getId().substring("main:".length()));
                log.error("秒杀订单消息未被 RabbitMQ 确认，orderId={}, cause={}", orderId, cause);
                releaseReservation(orderId);
            }
        });
//        当消息已经到交换机，但没有找到匹配队列时执行。
        rabbitTemplate.setReturnsCallback(returned -> {
            Object kind = returned.getMessage().getMessageProperties().getHeaders().get("message-kind");
            Object orderId = returned.getMessage().getMessageProperties().getHeaders().get("orderId");
            log.error("RabbitMQ 消息无法路由，exchange={}, routingKey={}, replyCode={}, replyText={}",
                    returned.getExchange(), returned.getRoutingKey(), returned.getReplyCode(), returned.getReplyText());
//            只处理主订单消息。重试消息、死信消息不在这里进行主订单回滚。
            if (MAIN_MESSAGE_KIND.equals(kind) && orderId != null) {
                Long id = Long.valueOf(orderId.toString());
                returnedMainOrderIds.add(id);
                releaseReservation(id);
            }
        });
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        seckillMqMetrics.request();
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行seckill.lua，检查库存，检查用户是否重复购买，扣减 Redis 库存，记录用户抢购标记，记录 reservation
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));

        // 2.执行失败
        if (result == null || result != 0L) {
            seckillMqMetrics.rejected();
            return Result.fail(result != null && result == 1L ? "库存不足" : "不能重复下单");
        }

        // 3.执行成功则构造订单对象
        VoucherOrder order = new VoucherOrder().setId(orderId).setUserId(userId).setVoucherId(voucherId);
        // 4.创建 Publisher Confirm 关联数据。
        CorrelationData correlationData = new CorrelationData("main:" + orderId);
        // 5.Java 后端作为生产者，将订单消息发送到 RabbitMQ 主交换机，
        // 等待 Broker 发布确认，消费者之后异步创建 MySQL 订单
        try {
            rabbitTemplate.convertAndSend(RabbitMQConfig.SECKILL_ORDER_EXCHANGE,
                    RabbitMQConfig.SECKILL_ORDER_ROUTING_KEY, order, message -> {
                        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        message.getMessageProperties().setHeader("message-kind", MAIN_MESSAGE_KIND);
                        message.getMessageProperties().setHeader("orderId", String.valueOf(orderId));
                        message.getMessageProperties().setHeader("x-retry-count", 0);
                        message.getMessageProperties().setHeader(TraceContext.TRACE_ID_HEADER,
                                TraceContext.currentTraceId());
                        return message;
                    }, correlationData);
            // 等待 Broker 的 Publisher Confirm，确认消息可靠进入 RabbitMQ
            CorrelationData.Confirm confirm = correlationData.getFuture()
                    .get(PUBLISH_CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            // 投递失败：1. Broker 返回 NACK 2. 之前发生过 Return
            if (!confirm.isAck() || returnedMainOrderIds.remove(orderId)) {
                releaseReservation(orderId);    // 释放redis预扣
                seckillMqMetrics.publishFailure();
                return Result.fail("订单消息发送失败，请稍后重试");
            }
            // 正常完成：Redis 预扣成功，RabbitMQ 发布成功
            seckillMqMetrics.published();
            return Result.ok(orderId);
        } catch (java.util.concurrent.TimeoutException e) {
            // 可能已进入 Broker 但确认延迟，保留预留状态等待定时补偿判断。
            log.warn("秒杀订单 Publisher Confirm 超时，orderId={}", orderId);
            seckillMqMetrics.publishFailure();
            return Result.fail("订单正在处理中，请稍后查询");
        } catch (Exception e) {
            log.error("秒杀订单消息发送异常，orderId={}", orderId, e);
            releaseReservation(orderId);
            seckillMqMetrics.publishFailure();
            return Result.fail("订单消息发送失败，请稍后重试");
        }
    }

    @Override
    @Transactional
    public void handleVoucherOrderByMq(VoucherOrder voucherOrder) {
        Long orderId = voucherOrder.getId();
//        创建订单级别的锁，避免多个消费者操作一个订单
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
                } else {
                    lock.unlock();
                }
            }
        }
    }

    private boolean reservationExists(Long orderId) {
        return Boolean.TRUE.equals(stringRedisTemplate.hasKey(RedisConstants.SECKILL_RESERVATION_KEY + orderId));
    }

    private void confirmReservationAfterCommit(Long orderId) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            confirmReservation(orderId);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
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
