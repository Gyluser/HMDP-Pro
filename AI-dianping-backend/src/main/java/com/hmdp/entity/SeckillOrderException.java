package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

/** 持久化记录进入死信队列、等待人工处理的秒杀订单。 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_seckill_order_exception")
public class SeckillOrderException {
    private Long id;
    private Long orderId;
    private Long userId;
    private Long voucherId;
    private Integer retryCount;
    private String failureReason;
    private String traceId;
    /** 0：待人工处理；1：已人工处理。 */
    private Integer status;
    private String handleNote;
    private LocalDateTime handledAt;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
