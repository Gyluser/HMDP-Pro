package com.hmdp.utils;

import com.hmdp.entity.Reservation;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.entity.Voucher;
import com.hmdp.dto.Result;
import com.hmdp.service.IReservationService;
import com.hmdp.service.IShopService;
import com.hmdp.service.IUserService;
import com.hmdp.service.IVoucherService;
import dev.langchain4j.agent.tool.ToolMemoryId;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.springframework.stereotype.Component;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Collections;
import java.util.Objects;

import java.time.LocalDateTime;

@Component
public class ReservationTool {

    private final IReservationService reservationService;

    private final IUserService userService;

    private final IShopService shopService;

    private final IVoucherService voucherService;

    private final StringRedisTemplate stringRedisTemplate;

    public ReservationTool(IReservationService reservationService,
                           IUserService userService,
                           IShopService shopService,
                           IVoucherService voucherService,
                           StringRedisTemplate stringRedisTemplate) {
        this.reservationService = reservationService;
        this.userService = userService;
        this.shopService = shopService;
        this.voucherService = voucherService;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Tool("预约店铺服务，需要提供店铺ID和预约时间")
    public String addReservation(
            @ToolMemoryId Object memoryId,
            @P("商户id") Long shopId,
            @P("预约时间, 格式为 yyyy-MM-dd HH:mm:ss") String reservationTime
    ) {
        // 手动解析字符串为 LocalDateTime
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime;
        try {
            dateTime = LocalDateTime.parse(reservationTime, formatter);
        } catch (DateTimeParseException e) {
            return "预约时间格式不正确，请使用 yyyy-MM-dd HH:mm:ss 格式。";
        }
        if (!dateTime.isAfter(LocalDateTime.now())) {
            return "预约时间不能早于当前时间，请重新选择未来的时间。";
        }
        Long userId = currentUserId(memoryId);
        if (userId == null) {
            return "无法确认当前登录用户，请先登录后再预约。";
        }
        if (shopId == null || shopService.getById(shopId) == null) {
            return "没有找到这个店铺，请确认店铺ID。";
        }
        User user = userService.getById(userId);
        if (user == null || user.getPhone() == null) {
            return "无法获取当前用户信息，请先登录。";
        }

        // 2. 创建并保存预约
        Reservation reservation = new Reservation();
        reservation.setUserId(userId);
        reservation.setUserPhone(user.getPhone());
        reservation.setShopId(shopId);
        reservation.setReservationTime(dateTime); // 使用解析后的 dateTime
        reservation.setStatus(0);
        reservation.setCreateTime(LocalDateTime.now());
        if (!reservationService.save(reservation)) {
            return "预约失败，请稍后再试。";
        }

        return "预约成功！";
    }

    @Tool("查询我的预约信息")
    public List<Reservation> findMyReservation(@ToolMemoryId Object memoryId) {
        Long userId = currentUserId(memoryId);
        if (userId == null) {
            return Collections.emptyList();
        }
        return reservationService.query().eq("user_id", userId).list();
    }

    @Tool("取消我的预约，需要提供预约记录ID")
    public String cancelReservation(
            @ToolMemoryId Object memoryId,
            @P("预约记录ID，必须完整保留全部数字") String reservationIdText) {
        Long userId = currentUserId(memoryId);
        if (userId == null) {
            return "无法确认当前登录用户，请先登录。";
        }
        if (reservationIdText == null || reservationIdText.isBlank()) {
            return "请提供要取消的预约记录ID。";
        }

        // 预约ID使用字符串接收，避免大数ID在模型工具调用中被当成浮点数处理。
        Long reservationId;
        try {
            reservationId = Long.valueOf(reservationIdText.trim());
        } catch (NumberFormatException e) {
            return "预约记录ID格式不正确，请提供完整的数字ID。";
        }

        // 先按预约ID查记录，再单独校验当前用户ID；会话ID不参与预约归属判断。
        Reservation reservation = reservationService.getById(reservationId);
        if (reservation == null) {
            return "没有找到这条预约记录，请确认预约记录ID是否完整。";
        }
        if (!Objects.equals(reservation.getUserId(), userId)) {
            return "这条预约不属于当前登录用户，无法取消。";
        }
        if (!reservationService.removeById(reservationId)) {
            return "取消预约失败，请稍后再试。";
        }
        return "预约已取消。";
    }

    @Tool("查询店铺的营业时间")
    public String getShopBusinessHours(
            @ToolMemoryId Object memoryId,
            @P("商户id") Long shopId) {
        Shop shop = shopService.getById(shopId);
        if (shop == null) {
            return "没有找到这个店铺。";
        }
        String openHours = shop.getOpenHours();
        return shop.getName() + "的营业时间是："
                + (openHours == null || openHours.isBlank() ? "暂无记录" : openHours);
    }

    @Tool("查询店铺的位置和地址")
    public String getShopLocation(
            @ToolMemoryId Object memoryId,
            @P("商户id") Long shopId) {
        Shop shop = shopService.getById(shopId);
        if (shop == null) {
            return "没有找到这个店铺。";
        }
        return shop.getName() + "的位置是："
                + (shop.getAddress() == null || shop.getAddress().isBlank() ? "暂无地址记录" : shop.getAddress());
    }

    @Tool("查询店铺当前可用的优惠券及实时库存")
    public String getShopVouchers(
            @ToolMemoryId Object memoryId,
            @P("商户id") Long shopId) {
        Result result = voucherService.queryVoucherOfShop(shopId);
        if (!Boolean.TRUE.equals(result.getSuccess())) {
            return "查询优惠券失败：" + result.getErrorMsg();
        }
        Object data = result.getData();
        if (!(data instanceof List<?> list) || list.isEmpty()) {
            return "这个店铺当前没有可用优惠券。";
        }
        StringBuilder answer = new StringBuilder();
        for (Object item : list) {
            if (!(item instanceof Voucher voucher)) {
                continue;
            }
            String stock = "普通券/库存以页面为准";
            if (voucher.getId() != null) {
                String redisStock = stringRedisTemplate.opsForValue()
                        .get(RedisConstants.SECKILL_STOCK_KEY + voucher.getId());
                if (redisStock != null) {
                    stock = redisStock;
                } else if (voucher.getStock() != null) {
                    stock = voucher.getStock().toString();
                }
            }
            answer.append("优惠券ID=").append(voucher.getId())
                    .append("，标题=").append(voucher.getTitle())
                    .append("，支付金额=").append(voucher.getPayValue() / 100.0).append("元")
                    .append("，抵扣金额=").append(voucher.getActualValue() / 100.0).append("元")
                    .append("，库存=").append(stock).append("；");
        }
        return answer.length() == 0 ? "这个店铺当前没有可用优惠券。" : answer.toString();
    }

    private Long currentUserId(Object memoryId) {
        try {
            return AiConversationKeys.userIdFromMemoryId(memoryId);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
