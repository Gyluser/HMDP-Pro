package com.hmdp.utils;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.google.common.hash.BloomFilter;
import com.hmdp.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * <p>
 * Redis工具类
 * </p>
 *
 * @author 超大王
 * @since 2025-09-19
 */
@Slf4j
@Component
public class CacheClient {

    // 布隆过滤器
    @Resource
    private BloomFilter<Long> bloomFilter;

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    //根据指定的key查询缓存，并反序列化为指定类型，利用布隆过滤器和缓存空值解决缓存穿透问题
    public <R, ID> R queryWithPassThrough(
           final String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
            Long time, TimeUnit unit, Long nullValueTTL, TimeUnit nullValueUnit){
        // 1.布隆过滤器判断id是否存在
        if (id instanceof Long && !bloomFilter.mightContain((Long)id)) {
            //不存在，直接返回错误
            log.info("布隆过滤器拦截，id:{}不存在", id);
            return null;
        }
        // 2.从Redis查询缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 3.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 存在，直接返回
            R result = JSONUtil.toBean(json, type);
            return result;
        }

        // 4.判断是否为空值（缓存空值）
        if (json != null) {
            // 是空值，返回不存在
            return null;
        }
        // 5.缓存未命中，查询数据库
        R result = dbFallback.apply(id);
        // 6.数据库不存在，返回错误信息
        if (result == null) {
            //将空值写入Redis（设置较短的过期时间）
            stringRedisTemplate.opsForValue().set(key, "", nullValueTTL, nullValueUnit);
            return null;
        }
        // 7.存在，写入Redis，并加入随机TTL，降低大量缓存同时过期的概率
        Long random = (long) RandomUtil.randomInt(3, 10);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(result),
                time + random, unit);
        return result;
    }

}
