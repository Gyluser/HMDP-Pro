package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.SeckillOrderException;
import org.apache.ibatis.annotations.Insert;

/** 秒杀死信订单的持久化入口；同一 orderId 重复到达时更新失败信息而不是重复插入。 */
public interface SeckillOrderExceptionMapper extends BaseMapper<SeckillOrderException> {

    @Insert("""
            INSERT INTO tb_seckill_order_exception
                (order_id, user_id, voucher_id, retry_count, failure_reason, trace_id, status)
            VALUES
                (#{orderId}, #{userId}, #{voucherId}, #{retryCount}, #{failureReason}, #{traceId}, 0)
            ON DUPLICATE KEY UPDATE
                retry_count = VALUES(retry_count),
                failure_reason = VALUES(failure_reason),
                trace_id = VALUES(trace_id),
                update_time = CURRENT_TIMESTAMP
            """)
    int upsert(SeckillOrderException exceptionOrder);
}
