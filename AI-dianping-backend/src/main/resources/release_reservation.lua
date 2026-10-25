-- 投递失败或预留超时后，原子回补 Redis 库存并撤销一人一单标记。
-- KEYS[1]：seckill:reservation:{orderId}
-- KEYS[2]：seckill:reservation:index
-- KEYS[3]：seckill:stock:{voucherId}
-- KEYS[4]：seckill:order:{voucherId}
-- ARGV[1]：orderId
local userId = redis.call('hget', KEYS[1], 'userId')
if (not userId) then
    redis.call('zrem', KEYS[2], ARGV[1])
    return 0
end
redis.call('incrby', KEYS[3], 1)
redis.call('srem', KEYS[4], userId)
redis.call('del', KEYS[1])
redis.call('zrem', KEYS[2], ARGV[1])
return 1
