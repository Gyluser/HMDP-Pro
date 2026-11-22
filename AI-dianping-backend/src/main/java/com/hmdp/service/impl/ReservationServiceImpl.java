package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.entity.Reservation;
import com.hmdp.mapper.ReservationMapper;
import com.hmdp.service.IReservationService;
import org.springframework.stereotype.Service;

@Service
public class ReservationServiceImpl extends ServiceImpl<ReservationMapper, Reservation> implements IReservationService {
}
