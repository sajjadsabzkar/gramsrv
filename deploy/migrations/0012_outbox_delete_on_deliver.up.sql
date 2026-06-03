-- 0012_outbox_delete_on_deliver: outbox 投递成功改为直接 DELETE（方案 A），杜绝 delivered 行无限堆积。
--
-- 配合 query 改动（MarkDispatchDelivered / MarkDispatchDeliveredBatch 由 UPDATE status='delivered' 改为 DELETE）：
--   1) 清理改造前堆积的存量 delivered 行；
--   2) 收紧 status CHECK，移除不再使用的 'delivered'（状态机只剩 pending / dispatching / failed）。
-- dispatch_outbox 为 HASH 分区表，父表 DELETE / ALTER CONSTRAINT 自动作用于全部分区。

DELETE FROM dispatch_outbox WHERE status = 'delivered';

ALTER TABLE dispatch_outbox DROP CONSTRAINT IF EXISTS dispatch_outbox_status_check;
ALTER TABLE dispatch_outbox ADD CONSTRAINT dispatch_outbox_status_check
    CHECK (status IN ('pending', 'dispatching', 'failed'));
