package com.hmdp.listener;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.entity.SeckillOrderException;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillOrderExceptionMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.SeckillMqMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SeckillOrderConsumerTest {

    @Mock
    private IVoucherOrderService voucherOrderService;
    @Mock
    private RabbitTemplate rabbitTemplate;
    @Mock
    private SeckillMqMetrics seckillMqMetrics;
    @Mock
    private SeckillOrderExceptionMapper seckillOrderExceptionMapper;

    private SeckillOrderConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new SeckillOrderConsumer(voucherOrderService, rabbitTemplate, seckillMqMetrics,
                seckillOrderExceptionMapper);
    }

    @Test
    void shouldAcknowledgeOnlyAfterSuccessfulOrderHandling() {
        VoucherOrder order = order();

        consumer.processOrder(order, messageWithRetryCount(0));

        verify(voucherOrderService).handleVoucherOrderByMq(order);
        verify(rabbitTemplate, never()).convertAndSend(anyString(), anyString(), any(), any(MessagePostProcessor.class), any());
    }

    @Test
    void shouldSendFirstFailureToFiveSecondRetryAndRejectOriginal() {
        VoucherOrder order = order();
        doThrow(new IllegalStateException("database unavailable"))
                .when(voucherOrderService).handleVoucherOrderByMq(order);
        confirmPublisher(true);

        assertThatThrownBy(() -> consumer.processOrder(order, messageWithRetryCount(0)))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);

        ArgumentCaptor<MessagePostProcessor> processor = ArgumentCaptor.forClass(MessagePostProcessor.class);
        verify(rabbitTemplate).convertAndSend(
                org.mockito.ArgumentMatchers.eq(RabbitMQConfig.SECKILL_ORDER_RETRY_EXCHANGE),
                org.mockito.ArgumentMatchers.eq(RabbitMQConfig.SECKILL_ORDER_RETRY_ROUTING_KEY + ".5s"),
                org.mockito.ArgumentMatchers.eq(order), processor.capture(), any(CorrelationData.class));
        Message retried = processor.getValue().postProcessMessage(new Message(new byte[0], new MessageProperties()));
        assertThat(retried.getMessageProperties().getHeaders().get("x-retry-count")).isEqualTo(1);
    }

    @Test
    void shouldSendLastFailureToDeadLetterQueue() {
        VoucherOrder order = order();
        doThrow(new IllegalStateException("database unavailable"))
                .when(voucherOrderService).handleVoucherOrderByMq(order);
        confirmPublisher(true);

        assertThatThrownBy(() -> consumer.processOrder(order, messageWithRetryCount(2)))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);

        verify(rabbitTemplate).convertAndSend(
                org.mockito.ArgumentMatchers.eq(RabbitMQConfig.SECKILL_ORDER_DLX_EXCHANGE),
                org.mockito.ArgumentMatchers.eq(RabbitMQConfig.SECKILL_ORDER_DLX_ROUTING_KEY),
                org.mockito.ArgumentMatchers.eq(order), any(MessagePostProcessor.class), any(CorrelationData.class));
    }

    @Test
    void shouldRequeueOriginalMessageWhenRetryPublishIsNotConfirmed() {
        VoucherOrder order = order();
        doThrow(new IllegalStateException("database unavailable"))
                .when(voucherOrderService).handleVoucherOrderByMq(order);
        confirmPublisher(false);

        assertThatThrownBy(() -> consumer.processOrder(order, messageWithRetryCount(0)))
                .isInstanceOf(ImmediateRequeueAmqpException.class);
    }

    @Test
    void shouldPersistDeadLetterBeforeAutoAcknowledgement() {
        VoucherOrder order = order();
        MessageProperties properties = new MessageProperties();
        properties.setHeader("x-retry-count", 2);
        properties.setHeader("x-failure-reason", "IllegalStateException: database unavailable");
        properties.setHeader("X-Trace-Id", "trace-dead-letter");

        consumer.processDeadLetter(order, new Message(new byte[0], properties));

        ArgumentCaptor<SeckillOrderException> exceptionCaptor =
                ArgumentCaptor.forClass(SeckillOrderException.class);
        verify(seckillOrderExceptionMapper).upsert(exceptionCaptor.capture());
        SeckillOrderException exceptionOrder = exceptionCaptor.getValue();
        assertThat(exceptionOrder.getOrderId()).isEqualTo(1L);
        assertThat(exceptionOrder.getUserId()).isEqualTo(2L);
        assertThat(exceptionOrder.getVoucherId()).isEqualTo(3L);
        assertThat(exceptionOrder.getRetryCount()).isEqualTo(2);
        assertThat(exceptionOrder.getFailureReason()).isEqualTo("IllegalStateException: database unavailable");
        assertThat(exceptionOrder.getTraceId()).isEqualTo("trace-dead-letter");
    }

    @Test
    void shouldRequeueDeadLetterWhenPersistenceFails() {
        doThrow(new IllegalStateException("database unavailable"))
                .when(seckillOrderExceptionMapper).upsert(any(SeckillOrderException.class));

        assertThatThrownBy(() -> consumer.processDeadLetter(order(), messageWithRetryCount(2)))
                .isInstanceOf(ImmediateRequeueAmqpException.class);
    }

    private void confirmPublisher(boolean ack) {
        doAnswer(invocation -> {
            CorrelationData correlationData = invocation.getArgument(4);
            correlationData.getFuture().complete(new CorrelationData.Confirm(ack, ack ? null : "nack"));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(), any(MessagePostProcessor.class), any(CorrelationData.class));
    }

    private VoucherOrder order() {
        VoucherOrder order = new VoucherOrder();
        order.setId(1L);
        order.setUserId(2L);
        order.setVoucherId(3L);
        return order;
    }

    private Message messageWithRetryCount(int retryCount) {
        MessageProperties properties = new MessageProperties();
        properties.setHeader("x-retry-count", retryCount);
        return new Message(new byte[0], properties);
    }
}
