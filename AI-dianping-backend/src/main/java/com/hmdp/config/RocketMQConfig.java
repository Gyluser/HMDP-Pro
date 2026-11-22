package com.hmdp.config;

/**
 * 秒杀订单 RocketMQ 拓扑：主 Topic -> 5 秒/30 秒显式延迟重投 -> 死信 Topic。
 *
 * Topic 由 Broker 按其 autoCreateTopicEnable 配置自动创建；生产环境应预创建并关闭该开关。
 */
public final class RocketMQConfig {

    private RocketMQConfig() {
    }

    public static final String SECKILL_ORDER_TOPIC = "HMDP_SECKILL_ORDER";
    public static final String SECKILL_ORDER_DLT_TOPIC = "HMDP_SECKILL_ORDER_DLT";
    public static final String SECKILL_ORDER_CONSUMER_GROUP = "hmdp-pro-seckill-order-consumer";
    public static final String SECKILL_ORDER_DLT_CONSUMER_GROUP = "hmdp-pro-seckill-order-dlt-consumer";

    /** RocketMQ 默认 delay level 2 = 5s。 */
    public static final int RETRY_5_SECONDS_DELAY_LEVEL = 2;
    /** RocketMQ 默认 delay level 4 = 30s。 */
    public static final int RETRY_30_SECONDS_DELAY_LEVEL = 4;
}
