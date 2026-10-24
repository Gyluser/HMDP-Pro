package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.common.hash.BloomFilter;
import com.hmdp.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.Collections;
import java.util.UUID;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

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

    /**
     * 缓存逻辑过期后，专用于异步重建热点缓存的线程池。
     */
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>();

    static {
        UNLOCK_SCRIPT.setScriptText("if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end");
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

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

    /**
     * 写入不设置 Redis 物理 TTL、但包含逻辑过期时间的热点缓存。
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
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

    /**
     * 使用逻辑过期解决热点缓存击穿：过期时先返回旧值，仅一个请求异步重建缓存。
     * 预热可避免首次同步回源；缓存缺失时会自动构建该 key。
     */
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        if (id instanceof Long && !bloomFilter.mightContain((Long) id)) {
            log.info("布隆过滤器拦截，id:{}不存在", id);
            return null;
        }

        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (json != null && StrUtil.isBlank(json)) {
            return null;
        }
        if (StrUtil.isBlank(json)) {
            return rebuildLogicalExpireCache(keyPrefix, id, type, dbFallback, time, unit);
        }

        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        // 兼容切换前由 queryWithPassThrough() 写入的普通 Shop JSON。
        if (redisData.getData() == null || redisData.getExpireTime() == null) {
            R legacyResult = JSONUtil.toBean(json, type);
            setWithLogicalExpire(key, legacyResult, time, unit);
            return legacyResult;
        }
        R result = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            return result;
        }

        String lockKey = LOCK_SHOP_KEY + id;
        String lockToken = tryLock(lockKey);
        if (lockToken != null) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R newResult = dbFallback.apply(id);
                    if (newResult == null) {
                        stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                    } else {
                        setWithLogicalExpire(key, newResult, time, unit);
                    }
                } catch (Exception e) {
                    log.error("逻辑过期缓存重建失败，key={}", key, e);
                } finally {
                    unlock(lockKey, lockToken);
                }
            });
        }
        return result;
    }

    /**
     * 第一次访问或更新后删除缓存时，同步构建逻辑过期缓存；其余并发请求等待后重查。
     */
    private <R, ID> R rebuildLogicalExpireCache(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String lockKey = LOCK_SHOP_KEY + id;
        String lockToken = null;
        try {
            lockToken = tryLock(lockKey);
            if (lockToken == null) {
                Thread.sleep(50);
                return queryWithLogicalExpire(keyPrefix, id, type, dbFallback, time, unit);
            }

            String latestJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(latestJson)) {
                return queryWithLogicalExpire(keyPrefix, id, type, dbFallback, time, unit);
            }
            if (latestJson != null) {
                return null;
            }

            R result = dbFallback.apply(id);
            if (result == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            setWithLogicalExpire(key, result, time, unit);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待逻辑过期缓存重建锁时被中断", e);
        } finally {
            if (lockToken != null) {
                unlock(lockKey, lockToken);
            }
        }
    }

    /**
     * 使用互斥锁解决缓存击穿：只有获得锁的请求查询数据库并回填缓存。
     */
    public <R, ID> R queryWithMutex(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }

        String lockKey = LOCK_SHOP_KEY + id;
        String lockToken = null;
        try {
            lockToken = tryLock(lockKey);
            if (lockToken == null) {
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }

            // 获取锁后再次检查，避免等待锁的请求重复访问数据库。
            json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                return JSONUtil.toBean(json, type);
            }
            if (json != null) {
                return null;
            }

            R result = dbFallback.apply(id);
            if (result == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            set(key, result, time, unit);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待缓存重建锁时被中断", e);
        } finally {
            if (lockToken != null) {
                unlock(lockKey, lockToken);
            }
        }
    }

    private String tryLock(String key) {
        String token = UUID.randomUUID().toString();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, token, LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success) ? token : null;
    }

    private void unlock(String key, String token) {
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(key), token);
    }

}
