-- 对已存在的 hmdpA 数据库执行一次；新建数据库则 db.sql 已包含该索引。
ALTER TABLE tb_voucher_order
    ADD UNIQUE KEY uk_voucher_order_user_voucher (user_id, voucher_id);
