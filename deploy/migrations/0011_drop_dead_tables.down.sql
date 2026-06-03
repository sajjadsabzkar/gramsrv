-- 0011 down (no-op): 本迁移清除的是已被取代、运行时零引用的死表
-- （update_events / messages_legacy / dialogs_legacy）。
--
-- 全新项目明确不保留回退路径，故不在此重建这些表；历史结构可查 0002 / 0005 / 0006 / 0007 迁移脚本（保留未删）。
-- golang-migrate 执行本文件即把版本回退到 0010，不恢复任何死表或其数据。
SELECT 1;
