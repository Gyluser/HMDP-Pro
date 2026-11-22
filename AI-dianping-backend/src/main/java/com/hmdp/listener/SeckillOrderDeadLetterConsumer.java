package com.hmdp.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.config.RocketMQConfig;
import com.hmdp.entity.SeckillOrderException;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillOrderExceptionMapper;
import com.hmdp.utils.TraceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/** 死信 Topic 消费端：仅在异常订单 UPSERT 成功后正常返回并确认消息。 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = RocketMQConfig.SECKILL_ORDER_DLT_TOPIC,
        consumerGroup = RocketMQConfig.SECKILL_ORDER_DLT_CONSUMER_GROUP,
        consumeMode = ConsumeMode.CONCURRENTLY)
public class SeckillOrderDeadLetterConsumer implements RocketMQListener<MessageExt> {
    private final SeckillOrderExceptionMapper seckillOrderExceptionMapper;
    private final ObjectMapper objectMapper;

    @Override
    public void onMessage(MessageExt message) {
        String traceId = message.getUserProperty(TraceContext.TRACE_ID_HEADER);
        if (traceId != null && !traceId.isBlank()) {
            TraceContext.set(traceId);
        }
        try {
            VoucherOrder order = objectMapper.readValue(message.getBody(), VoucherOrder.class);
            SeckillOrderException exceptionOrder = new SeckillOrderException()
                    .setOrderId(order.getId())
                    .setUserId(order.getUserId())
                    .setVoucherId(order.getVoucherId())
                    .setRetryCount(retryCount(message.getUserProperty("x-retry-count")))
                    .setFailureReason(abbreviate(message.getUserProperty("x-failure-reason"), 500))
                    .setTraceId(traceId);
            seckillOrderExceptionMapper.upsert(exceptionOrder);
            log.error("秒杀订单进入死信 Topic，orderId={}, userId={}, voucherId={}, retryCount={}, reason={}",
                    order.getId(), order.getUserId(), order.getVoucherId(), exceptionOrder.getRetryCount(),
                    exceptionOrder.getFailureReason());
        } catch (Exception persistFailure) {
            // 抛异常让 DLT 消息重新投递；绝不能在落库失败后确认。
            throw new IllegalStateException("死信订单持久化失败，等待 RocketMQ 重新投递", persistFailure);
        } finally {
            TraceContext.clear();
        }
    }

    private int retryCount(String value) {
        try {
            return value == null ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private String abbreviate(String value, int maxLength) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
