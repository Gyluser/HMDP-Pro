package com.hmdp.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * 秒杀订单 RabbitMQ 拓扑：主队列 -> 两级延迟重试队列 -> 死信队列。
 */
@Configuration
public class RabbitMQConfig {

    public static final String SECKILL_ORDER_EXCHANGE = "exchange.seckill.order";
    public static final String SECKILL_ORDER_ROUTING_KEY = "seckill.order";
    public static final String SECKILL_ORDER_QUEUE = "queue.seckill.order";

    public static final String SECKILL_ORDER_RETRY_EXCHANGE = "exchange.seckill.order.retry";
    public static final String SECKILL_ORDER_RETRY_ROUTING_KEY = "seckill.order.retry";
    public static final String SECKILL_ORDER_RETRY_5S_QUEUE = "queue.seckill.order.retry.5s";
    public static final String SECKILL_ORDER_RETRY_30S_QUEUE = "queue.seckill.order.retry.30s";

    public static final String SECKILL_ORDER_DLX_EXCHANGE = "exchange.seckill.order.dlx";
    public static final String SECKILL_ORDER_DLX_ROUTING_KEY = "seckill.order.dead";
    public static final String SECKILL_ORDER_DLX_QUEUE = "queue.seckill.order.dead";

    @Bean   //将消息转换为json格式
    public Jackson2JsonMessageConverter rabbitMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean   //创建主消息交换机
    public DirectExchange seckillOrderExchange() {
        return new DirectExchange(SECKILL_ORDER_EXCHANGE, true, false);
    }

    @Bean   //创建重试交换机
    public DirectExchange seckillOrderRetryExchange() {
        return new DirectExchange(SECKILL_ORDER_RETRY_EXCHANGE, true, false);
    }

    @Bean   //创建死信交换机
    public DirectExchange seckillOrderDlxExchange() {
        return new DirectExchange(SECKILL_ORDER_DLX_EXCHANGE, true, false);
    }

    @Bean   //创建主消息队列
    public Queue seckillOrderQueue() {
        return QueueBuilder.durable(SECKILL_ORDER_QUEUE).build();
    }

    @Bean   //绑定主消息交换机和队列，及其路由
    public Binding seckillOrderBinding() {
        return BindingBuilder.bind(seckillOrderQueue())
                .to(seckillOrderExchange())
                .with(SECKILL_ORDER_ROUTING_KEY);
    }

    @Bean   //创建重试队列
    public Queue seckillOrderRetry5sQueue() {
        return retryQueue(SECKILL_ORDER_RETRY_5S_QUEUE, 5_000);
    }

    @Bean
    public Queue seckillOrderRetry30sQueue() {
        return retryQueue(SECKILL_ORDER_RETRY_30S_QUEUE, 30_000);
    }

    private Queue retryQueue(String name, int ttlMillis) {
        Map<String, Object> arguments = new HashMap<>();

        arguments.put("x-message-ttl", ttlMillis);                            // 消息最多保存多久
        arguments.put("x-dead-letter-exchange", SECKILL_ORDER_EXCHANGE);      // 消息过期后转发到哪个交换机（主订单交换机）
        arguments.put("x-dead-letter-routing-key", SECKILL_ORDER_ROUTING_KEY);// 消息过期后转发到哪个路由（主订单路由）
        return QueueBuilder.durable(name).withArguments(arguments).build();
    }

    @Bean   //绑定对应的重试交换机和队列，及其路由
    public Binding seckillOrderRetry5sBinding() {
        return BindingBuilder.bind(seckillOrderRetry5sQueue())
                .to(seckillOrderRetryExchange())
                .with(SECKILL_ORDER_RETRY_ROUTING_KEY + ".5s");
    }

    @Bean
    public Binding seckillOrderRetry30sBinding() {
        return BindingBuilder.bind(seckillOrderRetry30sQueue())
                .to(seckillOrderRetryExchange())
                .with(SECKILL_ORDER_RETRY_ROUTING_KEY + ".30s");
    }

    @Bean   //创建死信队列
    public Queue seckillOrderDlxQueue() {
        return QueueBuilder.durable(SECKILL_ORDER_DLX_QUEUE).build();
    }

    @Bean   //绑定死信交换机和队列，及其路由
    public Binding seckillOrderDlxBinding() {
        return BindingBuilder.bind(seckillOrderDlxQueue())
                .to(seckillOrderDlxExchange())
                .with(SECKILL_ORDER_DLX_ROUTING_KEY);
    }
}
