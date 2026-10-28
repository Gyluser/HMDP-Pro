package com.hmdp.listener;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.entity.SeckillOrderException;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillOrderExceptionMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.SeckillMqMetrics;
import com.hmdp.utils.TraceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 参考项目的 AUTO 确认模式：正常返回即确认；失败消息转入延迟重试或死信队列。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SeckillOrderConsumer {
    private static final int MAX_RETRY_COUNT = 2;

    private final IVoucherOrderService voucherOrderService;
    private final RabbitTemplate rabbitTemplate;
    private final SeckillMqMetrics seckillMqMetrics;
    private final SeckillOrderExceptionMapper seckillOrderExceptionMapper;

    @RabbitListener(queues = RabbitMQConfig.SECKILL_ORDER_QUEUE)
    public void processOrder(VoucherOrder order, Message message) {
        String traceId = header(message, TraceContext.TRACE_ID_HEADER);
        if (traceId != null && !traceId.isBlank()) {
            TraceContext.set(traceId);
        }
        try {
            seckillMqMetrics.consumed();
            voucherOrderService.handleVoucherOrderByMq(order);
            log.info("秒杀订单消费成功，orderId={}, userId={}, voucherId={}",
                    order.getId(), order.getUserId(), order.getVoucherId());
        } catch (Exception e) {
            //1. 如果秒杀失败
            seckillMqMetrics.consumeFailure();
            int retryCount = retryCount(message);
            log.error("秒杀订单消费失败，orderId={}, retryCount={}", order.getId(), retryCount, e);
            //2. 开启重试，判断重试次数
            //2.1 重试次数小于最大限度，放入重试队列
            if (retryCount < MAX_RETRY_COUNT) {
                try {
                    publishRetry(order, message, retryCount + 1, e);
                    seckillMqMetrics.retried();
                } catch (IllegalStateException publishFailure) {
                    // 原消息进入重试/死信队列失败，必须显式要求重新入队，AUTO ACK + default-requeue-rejected=false 下
                    // 否则重试消息投递失败时原消息会被容器丢弃。
                    throw new ImmediateRequeueAmqpException("重试消息投递失败，原消息重新入队", publishFailure);
                }
            // 2.2 重试次数大于最大限度，直接放入死信队列
            } else {
                try {
                    publishDeadLetter(order, message, e);
                    seckillMqMetrics.deadLettered();
                } catch (IllegalStateException publishFailure) {
                    throw new ImmediateRequeueAmqpException("死信消息投递失败，原消息重新入队", publishFailure);
                }
            }
            // 原消息成功进入重试/死信队列，必须显示拒绝原消息再次进入主消息队列
            // 否则原消息会重复处理。（后续从重试队列入队的“原消息”已经变成新消息了）
            throw new AmqpRejectAndDontRequeueException("秒杀订单已转移至重试或死信队列", e);
        } finally {
            TraceContext.clear();
        }
    }

    @RabbitListener(queues = RabbitMQConfig.SECKILL_ORDER_DLX_QUEUE)
    public void processDeadLetter(VoucherOrder order, Message message) {
        String traceId = header(message, TraceContext.TRACE_ID_HEADER);
        if (traceId != null && !traceId.isBlank()) {
            TraceContext.set(traceId);
        }
        try {
            String failureReason = header(message, "x-failure-reason");
            SeckillOrderException exceptionOrder = new SeckillOrderException()
                    .setOrderId(order.getId())
                    .setUserId(order.getUserId())
                    .setVoucherId(order.getVoucherId())
                    .setRetryCount(retryCount(message))
                    .setFailureReason(abbreviate(failureReason, 500))
                    .setTraceId(traceId);
            seckillOrderExceptionMapper.upsert(exceptionOrder);
            log.error("秒杀订单进入死信队列，orderId={}, userId={}, voucherId={}, retryCount={}, reason={}",
                    order.getId(), order.getUserId(), order.getVoucherId(), retryCount(message),
                    failureReason);
        } catch (Exception persistFailure) {
            // 只有异常订单已持久化，监听方法才能正常结束并 AUTO ACK。
            throw new ImmediateRequeueAmqpException("死信订单持久化失败，消息重新入队", persistFailure);
        } finally {
            TraceContext.clear();
        }
    }

    private void publishRetry(VoucherOrder order, Message source, int nextRetryCount, Exception error) {
        String routingKey = nextRetryCount == 1
                ? RabbitMQConfig.SECKILL_ORDER_RETRY_ROUTING_KEY + ".5s"
                : RabbitMQConfig.SECKILL_ORDER_RETRY_ROUTING_KEY + ".30s";
        publishAndAwaitConfirm(RabbitMQConfig.SECKILL_ORDER_RETRY_EXCHANGE, routingKey, order,
                source, nextRetryCount, error, "retry");
    }

    private void publishDeadLetter(VoucherOrder order, Message source, Exception error) {
        publishAndAwaitConfirm(RabbitMQConfig.SECKILL_ORDER_DLX_EXCHANGE,
                RabbitMQConfig.SECKILL_ORDER_DLX_ROUTING_KEY, order, source,
                retryCount(source), error, "dead");
    }

    private void publishAndAwaitConfirm(String exchange, String routingKey, VoucherOrder order,
                                        Message source, int retryCount, Exception error, String kind) {
        // 获取关联信息
        CorrelationData correlationData = new CorrelationData(kind + ":" + order.getId() + ":" + retryCount);
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, order, outgoing -> {
                outgoing.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                outgoing.getMessageProperties().setHeader("message-kind", kind);
                outgoing.getMessageProperties().setHeader("orderId", String.valueOf(order.getId()));
                outgoing.getMessageProperties().setHeader("x-retry-count", retryCount);
                outgoing.getMessageProperties().setHeader("x-failure-reason",
                        error.getClass().getSimpleName() + ": " + error.getMessage());
                copyTraceId(source, outgoing);
                return outgoing;
            }, correlationData);
            CorrelationData.Confirm confirm = correlationData.getFuture().get(3, TimeUnit.SECONDS);
            if (!confirm.isAck()) {
                throw new IllegalStateException("RabbitMQ 未确认" + kind + "消息: " + confirm.getReason());
            }
        } catch (Exception publishError) {
            throw new IllegalStateException("发送" + kind + "消息失败，保留原消息等待再次处理", publishError);
        }
    }

    private int retryCount(Message message) {
        Object value = message.getMessageProperties().getHeaders().get("x-retry-count");
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof byte[] bytes) {
            return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
        }
        return value == null ? 0 : Integer.parseInt(value.toString());
    }

    private void copyTraceId(Message source, Message target) {
        String traceId = header(source, TraceContext.TRACE_ID_HEADER);
        if (traceId != null && !traceId.isBlank()) {
            target.getMessageProperties().setHeader(TraceContext.TRACE_ID_HEADER, traceId);
        }
    }

    private String header(Message message, String name) {
        Object value = message.getMessageProperties().getHeaders().get(name);
        if (value == null) {
            return null;
        }
        return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : value.toString();
    }

    private String abbreviate(String value, int maxLength) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
