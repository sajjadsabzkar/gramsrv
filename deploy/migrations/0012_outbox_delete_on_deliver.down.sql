-- 0012 down: 恢复宽 status CHECK（含 'delivered'）以保持迁移链完整。
-- 注意：方案 A 已删除的 delivered 行不可恢复；query 层（DELETE）回退需手动改回 UPDATE，本 down 不涉及代码。
ALTER TABLE dispatch_outbox DROP CONSTRAINT IF EXISTS dispatch_outbox_status_check;
ALTER TABLE dispatch_outbox ADD CONSTRAINT dispatch_outbox_status_check
    CHECK (status IN ('pending', 'dispatching', 'delivered', 'failed'));
