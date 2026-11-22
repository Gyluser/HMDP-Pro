package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    /** RocketMQ 消费端创建秒杀订单。 */
    void handleVoucherOrderByMq(VoucherOrder voucherOrder);

}
