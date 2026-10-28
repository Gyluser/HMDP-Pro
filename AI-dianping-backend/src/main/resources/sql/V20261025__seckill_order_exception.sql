CREATE TABLE IF NOT EXISTS tb_seckill_order_exception (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    order_id BIGINT NOT NULL COMMENT '秒杀订单ID',
    user_id BIGINT UNSIGNED NOT NULL COMMENT '用户ID',
    voucher_id BIGINT UNSIGNED NOT NULL COMMENT '优惠券ID',
    retry_count INT NOT NULL COMMENT '进入死信前的重试次数',
    failure_reason VARCHAR(500) NOT NULL COMMENT '最后一次失败原因',
    trace_id VARCHAR(64) NULL COMMENT '链路追踪ID',
    status TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0待人工处理，1已人工处理',
    handle_note VARCHAR(500) NULL COMMENT '人工处理说明',
    handled_at DATETIME NULL COMMENT '人工处理时间',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_seckill_exception_order_id (order_id),
    KEY idx_seckill_exception_status_create_time (status, create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='秒杀死信异常订单表';
