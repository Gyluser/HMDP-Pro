-- 数据库事务提交后确认订单预留状态。
-- KEYS[1]：seckill:reservation:{orderId}
-- KEYS[2]：seckill:reservation:index
-- ARGV[1]：orderId
redis.call('del', KEYS[1])
redis.call('zrem', KEYS[2], ARGV[1])
return 1
