package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.entity.SeckillOrderException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;

import static org.assertj.core.api.Assertions.assertThat;

/** 使用当前开发库验证异常订单表迁移及死信 UPSERT；测试记录会在结束前删除。 */
@SpringBootTest(properties = "spring.task.scheduling.enabled=false")
@Sql(scripts = "classpath:sql/V20261025__seckill_order_exception.sql")
class SeckillOrderExceptionMapperIntegrationTests {

    @Autowired
    private SeckillOrderExceptionMapper exceptionMapper;

    @Test
    void shouldPersistAndUpsertDeadLetterOrder() {
        long orderId = System.currentTimeMillis() * 1000 + 73;
        try {
            SeckillOrderException first = new SeckillOrderException()
                    .setOrderId(orderId)
                    .setUserId(900001L)
                    .setVoucherId(900002L)
                    .setRetryCount(2)
                    .setFailureReason("first failure")
                    .setTraceId("trace-first");
            assertThat(exceptionMapper.upsert(first)).isEqualTo(1);

            SeckillOrderException second = new SeckillOrderException()
                    .setOrderId(orderId)
                    .setUserId(900001L)
                    .setVoucherId(900002L)
                    .setRetryCount(3)
                    .setFailureReason("updated failure")
                    .setTraceId("trace-updated");
            assertThat(exceptionMapper.upsert(second)).isEqualTo(2);

            SeckillOrderException persisted = exceptionMapper.selectOne(
                    new LambdaQueryWrapper<SeckillOrderException>()
                            .eq(SeckillOrderException::getOrderId, orderId));
            assertThat(persisted).isNotNull();
            assertThat(persisted.getRetryCount()).isEqualTo(3);
            assertThat(persisted.getFailureReason()).isEqualTo("updated failure");
            assertThat(persisted.getTraceId()).isEqualTo("trace-updated");
            assertThat(persisted.getStatus()).isZero();
        } finally {
            exceptionMapper.delete(new LambdaQueryWrapper<SeckillOrderException>()
                    .eq(SeckillOrderException::getOrderId, orderId));
        }
    }
}
