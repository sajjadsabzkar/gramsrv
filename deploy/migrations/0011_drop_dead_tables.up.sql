-- 0011_drop_dead_tables: 清除已被取代、运行时零引用的遗留表（全新项目，不保留回退）。
--
-- - update_events  : 一阶段 auth_key 维度 update 队列（0006 建 / 0007 扩展），二阶段被
--                    user_update_events 取代；无任何 query / Go 引用，仅在 sqlc 留下孤儿 model。
-- - messages_legacy: 0009 由旧 messages（0005）重命名保留的迁移残骸，数据已迁入 private_messages + message_boxes。
-- - dialogs_legacy : 0009 由旧 dialogs（0002）重命名保留的迁移残骸，数据已迁入新 dialogs。
--
-- 顺序要求：update_events.message_id 外键指向 messages_legacy（原 messages），故先删 update_events。
-- 删除后需重跑 `sqlc generate`，models.go 中 UpdateEvent / MessagesLegacy / DialogsLegacy 孤儿 model 会自动消失。

DROP TABLE IF EXISTS update_events;
DROP TABLE IF EXISTS messages_legacy;
DROP TABLE IF EXISTS dialogs_legacy;
