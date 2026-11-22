package com.hmdp.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.config.RocketMQConfig;
import com.hmdp.entity.SeckillOrderException;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillOrderExceptionMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.SeckillMqMetrics;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 不依赖 Broker 的 RocketMQ 路由单元测试；断言 Topic、延迟级别、重试计数和 DLT 落库语义。 */
@ExtendWith(MockitoExtension.class)
class RocketMqSeckillOrderConsumerTest {
    @Mock
    private IVoucherOrderService voucherOrderService;
    @Mock
    private RocketMQTemplate rocketMQTemplate;
    @Mock
    private SeckillMqMetrics seckillMqMetrics;
    @Mock
    private SeckillOrderExceptionMapper seckillOrderExceptionMapper;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SeckillOrderConsumer consumer;
    private SeckillOrderDeadLetterConsumer deadLetterConsumer;

    @BeforeEach
    void setUp() {
        consumer = new SeckillOrderConsumer(voucherOrderService, rocketMQTemplate, seckillMqMetrics, objectMapper);
        deadLetterConsumer = new SeckillOrderDeadLetterConsumer(seckillOrderExceptionMapper, objectMapper);
    }

    @Test
    void shouldConsumeMainOrderWithoutRepublishingWhenBusinessSucceeds() throws Exception {
        consumer.onMessage(message(0));

        verify(voucherOrderService).handleVoucherOrderByMq(order());
        verify(rocketMQTemplate, never()).syncSend(anyString(), any(Message.class), anyLong(), anyInt());
    }

    @Test
    void shouldPublishFirstFailureToMainTopicWithFiveSecondDelay() throws Exception {
        stubSendOk();
        doThrow(new IllegalStateException("db unavailable"))
                .when(voucherOrderService).handleVoucherOrderByMq(order());

        consumer.onMessage(message(0));

        ArgumentCaptor<Message<?>> outgoing = ArgumentCaptor.forClass(Message.class);
        verify(rocketMQTemplate).syncSend(eq(RocketMQConfig.SECKILL_ORDER_TOPIC), outgoing.capture(),
                eq(3_000L), eq(RocketMQConfig.RETRY_5_SECONDS_DELAY_LEVEL));
        assertThat(outgoing.getValue().getHeaders().get("x-retry-count")).isEqualTo(1);
    }

    @Test
    void shouldPublishSecondFailureToMainTopicWithThirtySecondDelay() throws Exception {
        stubSendOk();
        doThrow(new IllegalStateException("db unavailable"))
                .when(voucherOrderService).handleVoucherOrderByMq(order());

        consumer.onMessage(message(1));

        ArgumentCaptor<Message<?>> outgoing = ArgumentCaptor.forClass(Message.class);
        verify(rocketMQTemplate).syncSend(eq(RocketMQConfig.SECKILL_ORDER_TOPIC), outgoing.capture(),
                eq(3_000L), eq(RocketMQConfig.RETRY_30_SECONDS_DELAY_LEVEL));
        assertThat(outgoing.getValue().getHeaders().get("x-retry-count")).isEqualTo(2);
    }

    @Test
    void shouldPublishThirdFailureToDltAndPersistIt() throws Exception {
        stubSendOk();
        doThrow(new IllegalStateException("db unavailable"))
                .when(voucherOrderService).handleVoucherOrderByMq(order());

        consumer.onMessage(message(2));

        ArgumentCaptor<Message<?>> outgoing = ArgumentCaptor.forClass(Message.class);
        verify(rocketMQTemplate).syncSend(eq(RocketMQConfig.SECKILL_ORDER_DLT_TOPIC), outgoing.capture(),
                eq(3_000L), eq(0));
        assertThat(outgoing.getValue().getHeaders().get("x-retry-count")).isEqualTo(2);

        deadLetterConsumer.onMessage(message(2));
        ArgumentCaptor<SeckillOrderException> exceptionOrder = ArgumentCaptor.forClass(SeckillOrderException.class);
        verify(seckillOrderExceptionMapper).upsert(exceptionOrder.capture());
        assertThat(exceptionOrder.getValue().getOrderId()).isEqualTo(1L);
        assertThat(exceptionOrder.getValue().getRetryCount()).isEqualTo(2);
    }

    @Test
    void shouldRethrowWhenDeadLetterPersistenceFailsSoRocketMqCanRedeliver() {
        doThrow(new IllegalStateException("db unavailable"))
                .when(seckillOrderExceptionMapper).upsert(any(SeckillOrderException.class));

        assertThatThrownBy(() -> deadLetterConsumer.onMessage(message(2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("死信订单持久化失败");
    }

    private MessageExt message(int retryCount) throws Exception {
        MessageExt message = new MessageExt();
        message.setBody(objectMapper.writeValueAsBytes(order()));
        message.putUserProperty("x-retry-count", String.valueOf(retryCount));
        message.putUserProperty("x-failure-reason", "IllegalStateException: db unavailable");
        message.putUserProperty("X-Trace-Id", "test-trace");
        return message;
    }

    private VoucherOrder order() {
        return new VoucherOrder().setId(1L).setUserId(2L).setVoucherId(3L);
    }

    private SendResult sendOk() {
        SendResult result = new SendResult();
        result.setSendStatus(SendStatus.SEND_OK);
        return result;
    }

    private void stubSendOk() {
        when(rocketMQTemplate.syncSend(anyString(), any(Message.class), anyLong(), anyInt()))
                .thenReturn(sendOk());
    }
}
