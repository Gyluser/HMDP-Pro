package com.hmdp.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.config.RocketMQConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.SeckillMqMetrics;
import com.hmdp.utils.TraceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/** 主 Topic 消费端：业务失败显式延迟重投，不使用 RocketMQ 自动业务重试。 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = RocketMQConfig.SECKILL_ORDER_TOPIC,
        consumerGroup = RocketMQConfig.SECKILL_ORDER_CONSUMER_GROUP,
        consumeMode = ConsumeMode.CONCURRENTLY,
        maxReconsumeTimes = 0)
public class SeckillOrderConsumer implements RocketMQListener<MessageExt> {
    private static final int MAX_RETRY_COUNT = 2;
    private static final String RETRY_COUNT_PROPERTY = "x-retry-count";
    private static final String FAILURE_REASON_PROPERTY = "x-failure-reason";

    private final IVoucherOrderService voucherOrderService;
    private final RocketMQTemplate rocketMQTemplate;
    private final SeckillMqMetrics seckillMqMetrics;
    private final ObjectMapper objectMapper;

    @Override
    public void onMessage(MessageExt message) {
        VoucherOrder order;
        try {
            order = objectMapper.readValue(message.getBody(), VoucherOrder.class);
        } catch (Exception deserializeFailure) {
            throw new IllegalStateException("秒杀订单消息反序列化失败", deserializeFailure);
        }

        String traceId = property(message, TraceContext.TRACE_ID_HEADER);
        if (traceId != null && !traceId.isBlank()) {
            TraceContext.set(traceId);
        }
        try {
            seckillMqMetrics.consumed();
            voucherOrderService.handleVoucherOrderByMq(order);
            log.info("秒杀订单消费成功，orderId={}, userId={}, voucherId={}",
                    order.getId(), order.getUserId(), order.getVoucherId());
        } catch (Exception businessFailure) {
            seckillMqMetrics.consumeFailure();
            int retryCount = retryCount(message);
            log.error("秒杀订单消费失败，orderId={}, retryCount={}", order.getId(), retryCount, businessFailure);
            if (retryCount < MAX_RETRY_COUNT) {
                publishRetry(order, message, retryCount + 1, businessFailure);
                seckillMqMetrics.retried();
            } else {
                publishDeadLetter(order, message, retryCount, businessFailure);
                seckillMqMetrics.deadLettered();
            }
            // 显式延迟消息或 DLT 已 SEND_OK，正常返回确认当前消息。
        } finally {
            TraceContext.clear();
        }
    }

    private void publishRetry(VoucherOrder order, MessageExt source, int nextRetryCount, Exception failure) {
        int delayLevel = nextRetryCount == 1
                ? RocketMQConfig.RETRY_5_SECONDS_DELAY_LEVEL
                : RocketMQConfig.RETRY_30_SECONDS_DELAY_LEVEL;
        sendAndRequireOk(RocketMQConfig.SECKILL_ORDER_TOPIC,
                message(order, source, nextRetryCount, failure, "retry"), delayLevel, "retry");
    }

    private void publishDeadLetter(VoucherOrder order, MessageExt source, int retryCount, Exception failure) {
        sendAndRequireOk(RocketMQConfig.SECKILL_ORDER_DLT_TOPIC,
                message(order, source, retryCount, failure, "dead"), 0, "dead");
    }

    private Message<VoucherOrder> message(VoucherOrder order, MessageExt source, int retryCount,
                                          Exception failure, String kind) {
        return MessageBuilder.withPayload(order)
                .setHeader("message-kind", kind)
                .setHeader("orderId", String.valueOf(order.getId()))
                .setHeader(RETRY_COUNT_PROPERTY, retryCount)
                .setHeader(FAILURE_REASON_PROPERTY, failureDescription(failure))
                .copyHeadersIfAbsent(traceHeaders(source))
                .build();
    }

    private void sendAndRequireOk(String topic, Message<?> message, int delayLevel, String kind) {
        try {
            SendResult result = rocketMQTemplate.syncSend(topic, message, 3_000, delayLevel);
            if (result == null || result.getSendStatus() != SendStatus.SEND_OK) {
                throw new IllegalStateException("RocketMQ 未确认" + kind + "消息: "
                        + (result == null ? "null" : result.getSendStatus()));
            }
        } catch (Exception publishFailure) {
            // 只有投递链路故障才抛出，让 RocketMQ 原消息重投，避免在未确认时丢失。
            throw new IllegalStateException("发送" + kind + "消息失败，等待 RocketMQ 重新投递", publishFailure);
        }
    }

    private int retryCount(MessageExt message) {
        String value = property(message, RETRY_COUNT_PROPERTY);
        try {
            return value == null ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private Map<String, Object> traceHeaders(MessageExt message) {
        Map<String, Object> headers = new HashMap<>();
        String traceId = property(message, TraceContext.TRACE_ID_HEADER);
        if (traceId != null && !traceId.isBlank()) {
            headers.put(TraceContext.TRACE_ID_HEADER, traceId);
        }
        return headers;
    }

    private String property(MessageExt message, String name) {
        return message.getUserProperty(name);
    }

    private String failureDescription(Exception failure) {
        return failure.getClass().getSimpleName() + ": "
                + (failure.getMessage() == null ? "" : failure.getMessage());
    }
}
