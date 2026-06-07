# Message Module Design

Date: 2026-05-31

## Scope

二阶段先实现私聊文本闭环：

- `messages.sendMessage`
- `messages.forwardMessages`
- `messages.getDialogs`
- `messages.getHistory`
- `messages.readHistory`
- `messages.editMessage`
- `messages.deleteMessages`
- `messages.deleteHistory`
- `updates.getState`
- `updates.getDifference`
- 在线 session update 推送

不在本阶段实现：媒体、定时消息、群组/频道、Bot API、文件 DC。

## Storage

大表从第一版开始分区：

| table | partition key | purpose |
|---|---|---|
| `private_messages` | `sender_user_id` HASH | 共享私聊消息主体；`sender_user_id + random_id` 保证发送幂等 |
| `message_boxes` | `owner_user_id` HASH | owner 视角消息盒；TDesktop 看到的 message id 即 `box_id`，并保存当前 owner 的 `media_unread/reaction_unread` 内容已读状态 |
| `dialogs` | `user_id` HASH | 会话摘要；`folder_id=0/1` 表示主列表/归档，置顶、manual unread、action bar 隐藏均是 owner 视角 |
| `contact_blocks` | `owner_user_id` HASH | 当前 owner 的 blocklist；`owner_user_id + blocked_user_id` 唯一，作为本阶段私聊 privacy gate |
| `dialog_filters` / `dialog_filter_settings` | `user_id` HASH | TDesktop 自定义 dialog filter、filter 顺序与 folder tags 开关；不把自定义 filter 伪装成 dialogs.folder_id |
| `user_update_events` | `user_id` HASH | 账号级 pts durable log；承载新消息、已读 inbox/outbox、文本编辑、删除消息，也承载 contacts reset、dialog pinned/order/manual unread、peer settings、dialog filters 与 folder peers 等 owner 视角状态事件 |
| `dispatch_outbox` | `target_user_id` HASH | transactional outbox，事务后批量推送在线 session；投递成功即删除，仅保留未完成任务 |

Redis 只存可恢复计数：

- `counter:pts:{user_id}`：账号级 pts。
- `counter:box_id:{user_id}`：owner 视角 box_id。
- `ratelimit:messages:send:{user_id}`：发送窗口限流，当前为每用户每分钟 30 条。

Redis miss 时分别从 `MAX(user_update_events.pts)` 与 `MAX(message_boxes.box_id)` 恢复。
分配器热路径使用 Redis Lua 脚本递增；首次 miss 时先从 PG durable log 读取恢复值，再通过恢复脚本完成「初始化 + 首次自增」，避免并发 first-use 把 `pts` / `box_id` 分配出重复或回退。

## Send Flow

`messages.sendMessage` 在 RPC 层只做 TL 转换和当前 user/session 校验，业务写入由 `MessageStore.SendPrivateText` 单事务完成：

1. 写 `private_messages`，遇到同 `sender_user_id + random_id` 直接返回原消息盒。
2. Redis 分配 sender/recipient 的 `box_id` 与 `pts`。
3. 写 sender/recipient `message_boxes`，并保存 `silent/noforwards/reply_to/fwd_from` 元数据；reply 会把当前 owner 的 `reply_to_msg_id` 翻译成对端 owner 视角的 box_id。incoming media 在 recipient box 上置 `media_unread=true`，sender 自己始终为 false。
4. upsert 双方 `dialogs`。
5. 写双方 `user_update_events(new_message)`。
6. 写双方 `dispatch_outbox`，sender 侧带 `exclude_session_id`。
7. 提交后由 outbox worker 批量推送在线 session。

若事务失败但 Redis 已分配 pts，store 会尽力写 `noop` 事件占位，避免 pts 回退；PG 不可用时该补偿也可能失败，后续需要告警指标覆盖。

如果 recipient 已 block sender，`SendPrivateText` 仍写 sender outbox/dialog/update，保证当前用户能看到自己发出的消息；但不创建 recipient message box、不推进 recipient pts、不写 recipient dispatch_outbox，也不会进入 recipient 离线 `updates.getDifference`。该规则同样适用于 `messages.sendMedia/sendMultiMedia` 和私聊转发，因为它们共用 `sendOutgoing`/`SendPrivateText`。

## Forward / Reply Flow

`messages.forwardMessages` 当前覆盖私聊文本转发，参考实现 的业务语义但保持 telesrv 的 owner 视角模型：

1. RPC 层校验 from/to peer、id/random_id 等长、单次最多 100 条；scheduled/monoforum/quick reply/effect/paid/suggested 等当前阶段外能力用显式 TL 错误拒绝，目标为 channel 的 `send_as` 交由频道模块校验 self/current channel。
2. store 按当前 owner + from peer + box_id 保序读取源消息，源消息带 `noforwards` 时返回 `CHAT_FORWARDS_RESTRICTED`。
3. 未设置 `drop_author` 时，若源消息已有 forward header 则沿用；否则用源消息 `from/date` 生成 `messageFwdHeader`。`drop_author` 会去掉 forward header。
4. 每条转发复用 `SendPrivateText` 写入双端 message box / dialog / update event / dispatch outbox，`pts_count=1`，并使用对应 random_id 保证幂等。

`messages.sendMessage` 的 `reply_to` 支持 `inputReplyToMessage` 的私聊同 peer 回复和 quote 文本/entities/offset；`quote_text` 按 TDesktop `quote_length_max=1024` 限制，`quote_offset` 是原消息文本内 offset，不是 message id，按当前文本消息上限 4096 收口。cross-peer reply、story、monoforum、todo/poll reply 仍拒绝，但返回 `REPLY_MESSAGE_ID_INVALID` / `STORY_ID_INVALID` / `REPLY_TO_MONOFORUM_PEER_INVALID` / `POLL_OPTION_INVALID` 等显式错误，不再落 `NOT_IMPLEMENTED`。服务端保存 sender 视角和 recipient 视角各自的 `reply_to_msg_id`，避免一个 owner 的 box_id 泄漏到另一个 owner。

## Read Flow

`messages.readHistory` 采用 owner 视角的双端回执模型，参考实现 的 inbox 已读链路和 参考实现 的 inbox/outbox read history event：

1. reader 侧锁定当前 dialog，按 `max_id` 与当前可见 incoming message 计算新的 `read_inbox_max_id`，清零 unread_count 并清除 manual unread。
2. 如果 reader 确实推进了已读水位，给 reader 生成 `updateReadHistoryInbox(pts_count=1)`，写入 `user_update_events + dispatch_outbox`。
3. 同一事务中找出本次被读到的最新 incoming message，定位原 sender 的 dialog，推进 sender 侧 `read_outbox_max_id`。
4. sender 水位真正推进时，给 sender 生成 `updateReadHistoryOutbox(pts_count=1)`，离线设备可通过 `updates.getDifference` 补齐。

发送消息时不会预先推进 sender 的 `read_outbox_max_id`；只有对端实际 readHistory 后才产生 outbox 已读回执，避免“刚发出就被标成已读”的假状态。

`messages.getOutboxReadDate` 复用 sender 侧 durable `read_history_outbox` 事件：先校验当前 owner 的 `msg_id` 是该 peer 下可见 outgoing message，再取最早一条 `max_id >= msg_id` 的 outbox read event 日期返回 `outboxReadDate`；未被读到返回 `MESSAGE_NOT_READ_YET`。PG 上有 `(user_id, peer_type, peer_id, max_id, date) WHERE event_type='read_history_outbox'` partial index，避免 TDesktop 已读详情查询扫全量 update log。

## Content Read Flow

`messages.readMessageContents` 处理 TDesktop 打开媒体/反应等“内容已读”入口，不等同于 history read 水位：

1. 私聊 incoming media 创建 recipient message box 时置 `media_unread=true`；sender 自己为 false。
2. 私聊 reaction 写入时，若反应者不是原消息作者，会把原作者 owner 视角的 message box 标记 `reaction_unread=true`，并重算该 dialog 的 `unread_reactions_count`。
3. `readMessageContents` 在事务内锁定当前 owner 的 exact message boxes，只清理 `media_unread OR reaction_unread` 的行；不可见 id、已删除 id、已经 read 的 id 都不生成新 pts。
4. 实际清理时为当前 owner 分配连续 user pts，写 `user_update_events(read_message_contents)` 与 `dispatch_outbox`，TL 转换为 `updateReadMessagesContents{messages,pts,pts_count}`。重复调用返回当前 affectedMessages，`pts_count=0`。
5. 该事件排除当前 auth_key/session，其它在线 session 走 reliable outbox，离线设备通过 `updates.getDifference` 恢复。

## Edit Flow

`messages.editMessage` 当前只支持私聊文本编辑：

1. RPC 层校验当前账号、peer、message id、文本长度和 entities；`inputMediaWebPage/inputMediaEmpty` 降级为文本编辑，真实 media/reply_markup/quick replies/scheduled 返回显式 TL 错误。
2. store 锁定当前 owner 的 message_box，确认它是当前 user 发出的 outgoing 私聊消息。
3. 更新共享 `private_messages` 文本与 `edit_date`，并更新同一 private message 下所有未删除 owner message_box。
4. 每个受影响 owner 各自分配 `pts`，写 `updateEditMessage(pts_count=1)` 与 `dispatch_outbox`。
5. 当前请求设备直接拿到 `updates` 响应；其它在线设备走 reliable outbox，离线设备走 `updates.getDifference`。

如果文本和 entities 完全未变化，返回 `MESSAGE_NOT_MODIFIED`；非作者编辑返回 `MESSAGE_AUTHOR_REQUIRED`。

若 peer 已 block 当前用户，私聊 edit 会返回 `EDIT_MESSAGES_FORBIDDEN`，避免修改对方侧已存在的 message box。该 gate 只来自 `contact_blocks`；完整 Telegram privacy keys 暂不扩展。

## Delete Flow

`messages.deleteMessages` / `messages.deleteHistory` 以 owner 视角软删除 `message_boxes`，不会删除共享 `private_messages` 主体。默认 deleteHistory 清空后如果该 peer 已无可见消息，则删除当前 owner 的 dialog；后续任意新消息会通过正常 send/upsert 路径重建 dialog。`just_clear=true` 对齐 参考实现 语义：清空历史但保留一个空 dialog（当前阶段不生成 `messageActionHistoryClear` 服务消息）。

`revoke=true` 时按 `(message_sender_id, private_message_id)` 找到同一私聊消息在其它 owner 下的 message_box 并软删除。每个受影响 owner 都生成自己的 `updateDeleteMessages`，`message_ids` 是该 owner 视角 box_id，`pts_count=len(message_ids)`，并写入 `user_update_events + dispatch_outbox`。如果删除后仍有可见消息，dialog 的 top/unread 会按剩余消息重算；否则删除 dialog 或在 `just_clear` 下保留空 dialog。

若 `revoke=true` 会影响已 block 当前用户的一方，RPC 层返回 `DELETE_MESSAGES_FORBIDDEN`；`revoke=false` 或本地清理仍只影响当前 owner，可继续执行。

全清也必须让所有被删的 owner 视角 message_id 最终进入 update/difference，但不能合成一个超大 update。`messages.deleteHistory` 单次最多删除 `MaxDeleteHistoryBatch=1000` 条，按 box_id 倒序批量软删并返回 `affectedHistory.offset=1` 表示客户端应继续发起下一批；每一批各自产生一条有界 `updateDeleteMessages`。`messages.deleteMessages` 单次 id 数限制为 `MaxDeleteMessageIDs=1000`，服务端还会丢弃 `<=0` 或超过 TL/PG int4 可表达范围的 id。

## Query Path

- `messages.getHistory` 以 `owner_user_id + peer_user_id + box_id/date` 命中 `message_boxes` 分区索引；`offset_id`、`offset_date`、`max_id`、`min_id` 都下推为游标条件；`add_offset` 只允许 `[-100,100]` 的小窗口偏移，避免异常客户端把超大偏移变成 SQL 跳过扫描或内存 slice capacity。
- `messages.search` / `messages.searchGlobal` 当前只覆盖当前 owner 私聊文本搜索；查询仍限定在 `owner_user_id` HASH 分区内，文本条件由 `pg_trgm` GIN 索引兜底。参考实现 的经验，未接外部全文搜索前禁止无索引大表模糊扫；后续群组/频道/全局多 peer 搜索应接专用搜索索引或 FTS。
- `messages.getDialogs` 以 `user_id` 分区定位当前账号，再按 `top_message_date/top_message_id/peer_id` 做 seek pagination；folder_id=0/1 直接走 `dialogs.folder_id`，folder_id>=2 先取当前账号 `dialog_filters` 后按 include/exclude/contact 规则过滤；`hash` 与 `count` 基于当前筛选后的完整会话集计算。
- `messages.getDialogFilters` 返回 `dialogFilterDefault` + 当前账号持久化 filters；`messages.updateDialogFilter/updateDialogFiltersOrder/toggleDialogFilterTags` 与 `folders.editPeerFolders` 都是 owner 视角写入，归档只允许 folder_id 0/1，自定义 filters 从 ID 2 开始。
- `updates.getDifference` 只按 `user_id + pts` 顺序扫描 `user_update_events`，多设备各自的 `(auth_key_id,user_id)` state 只记录消费位置，不参与账号事件归属；离线设备通过同一条 durable log 恢复新消息、已读 inbox/outbox、内容已读、文本编辑、删除消息、联系人 reset、dialog pinned/order/manual unread、peer settings、dialog filters/order/reload 与 folder peers 变化，置顶顺序、peer settings flags、filter payload 与 folder peers 会随事件负载持久化。

## 参考实现 Comparison

参考实现 的 `SyncUpdatesNotMe` 设计有两个值得借鉴的点：所有“其它端通知”收敛到 sync 服务，以及带 pts 的消息/已读类 update 会写 `user_pts_updates` 后再推 session。它的不足也很明确：`updatePeerSettings` 只在线 push 不入 durable pts 队列，`markDialogUnread` / `hidePeerSettingsBar` 仍有 TODO，离线设备可能依赖后续主动刷新才能看到状态变化。

telesrv 当前做得更进一步：联系人 reset、dialog pinned/order/manual unread、peer settings、dialog filter/order/reload 与 folder peers 都写入账号级 `user_update_events`，并通过 Postgres `AppendWithDispatch` 在同一事务里写入 `dispatch_outbox`。`updatePinnedDialogs.order`、`updatePeerSettings.settings`、`updateDialogFilter.filter`、`updateDialogFilterOrder.order`、`updateFolderPeers.folder_peers` 都是 durable payload，不依赖后续主动刷新；在线投递由 outbox worker 负责，排除当前设备时同时携带 `exclude_auth_key_id + exclude_session_id`，避免同一 session_id 在不同 auth key 下误排除或串号。RPC handler 在检测到可靠 outbox 后不再额外手动 push，避免其它设备收到重复在线通知。

dialog 分组借鉴 参考实现 的 `dialog_filters`/`editPeerFolders` 业务语义与 参考实现 的 `UpdateDialogFilter` / `UpdateFolderPeers` 事件模型，但修正两点：filter 设置不仅在线 push，也有持久化真值表和 durable update；归档 folder 与自定义 filter 分离，避免一个 `folder_id` 字段同时承担归档状态和任意筛选规则。

删除链路借鉴 参考实现 的 `just_clear` / dialog 删除边界：普通 deleteHistory 会移除 dialog，`just_clear` 保留 dialog。借鉴 参考实现 的点是“先按 owner 计算受影响 message_id，再按 owner 重建 top message 并发 `updateDeleteMessages`”，而不是只对当前请求账号返回 affectedMessages；这样 revoke、多设备离线补偿和 dialog 后续重建都在同一条 owner 视角语义里。

已读和编辑链路也分别借鉴了两边的优点：参考实现 在 edit 时会校验 sender 并把编辑同步到 inbox/outbox，参考实现 把 readHistory 分成 reader 的 `UpdateReadHistoryInbox` 和 sender 的 `UpdateReadHistoryOutbox`。telesrv 将这两类事件都落到账号级 durable log，同事务写 reliable outbox，且 edit 更新共享消息主体和所有 owner 视角 message_box，避免只在线 push 或只改当前盒子导致离线设备/对端历史不一致。

reply/forward 的语义参考实现 的 handler：send/forward 都透传 `silent` 和 `noforwards`，reply 用 `inputReplyToMessage`，forward 在源消息没有 `fwd_from` 时构造原作者 header，遇到受保护内容拒绝转发。telesrv 额外修正 owner 视角 id 翻译与 durable update 负载：对端收到的 reply header 使用对端自己的 message id，离线设备通过 `updates.getDifference` 也能拿到同样的 `reply_to/fwd_from` 元数据。

## Observability

消息链路 RPC 层预留 `Metrics` 接口，当前覆盖：

- `MessageSend`：记录发送成功、payload 长度与双端 pts。
- `MessageRateLimited`：记录用户级窗口限流。
- `OutboxClaimed` / `OutboxDelivered` / `OutboxFailed`：记录 outbox claim、在线推送成功与重试失败。

默认实现是 no-op，生产接 Prometheus/OpenTelemetry 时只需在 `rpc.Deps` 注入实现，不污染业务服务和 store 边界。

## Next Execution Plan

连接层已具备 per-connection outbound actor、scoped session context 与 bounded inbound RPC scheduler。消息模块下一轮按以下顺序推进，不再先扩 RPC 面：

1. 压测基线：单机目标 200 msg/s 私聊文本、p99 sendMessage < 150ms、p99 getDifference < 100ms。已落地 `internal/loadtest` 真实 PG+Redis 压测 harness，首版基线见下「Load Baseline」节；1k online session 的网络 fanout 属连接层，本轮 harness 未覆盖（binder 用零连接 SessionManager，只测 outbox 的 PG 排空）。
2. Redis 计数强一致：`pts` / `box_id` 分配已收敛到 Lua 脚本路径，覆盖 Redis miss 从 PG durable log 恢复与并发 first-use 连续分配；事务失败 noop gap 与 random_id 幂等已有 store 测试覆盖，后续补真实 Redis+PG 混合压测。
3. PG 分区验收：已有 `message_boxes` history seek 与 `dispatch_outbox` stale retry 索引；已补 explain 集成测试，固定 `message_boxes` / `user_update_events` 单用户查询命中分区索引，`dispatch_outbox` 全局 claim 允许跨分区但必须走分区 partial index、不出现 Seq Scan。
4. Outbox 背压（已做到生产级，见下「Load Baseline」）：worker 有 batch claim、lease timeout、retry/backoff、failed 终态和 claim/deliver/fail 指标；batch size / interval / lease timeout / **worker 数** 均配置化（env `TELESRV_OUTBOX_BATCH` / `_INTERVAL` / `_LEASE_TIMEOUT` / `_WORKERS`）。**已并行化（N worker 竞争 `FOR UPDATE SKIP LOCKED` claim）并批量化（每批一次 `BatchListDispatchEvents` unnest join + 一次 `MarkDispatchDeliveredBatch`，取代逐条往返）**，排空上限从单 worker ~270 行/s 提到 ~12k 行/s（本机 PG，~45x）；批量化后瓶颈转移到 PG 本身。主动 push 仍只走 `auth_key_id + session_id + user_id` scoped API。
5. TDesktop 私聊闭环：用双账号/多设备验证 sendMessage、forwardMessages、reply、getDialogs、getHistory、readHistory、getOutboxReadDate、editMessage、deleteMessages/deleteHistory、updates.getDifference、当前设备过滤与退出换号不串号。2026-05-31 已用 Alice/Bob 两个 TDesktop workdir 验证在线 read/edit/delete 与 Bob 离线 send+edit/read/delete 差量同步；删除后 dialog 可清除，后续新消息仍按正常路径重建。2026-06-01 已补 reply/forward 服务端路径和单测，仍待重新用双 TDesktop 实机点 UI 行为（通知声音、reply 预览、forward header）。
6. 兼容矩阵回填：所有真实跑到的未知 RPC 自动入 trace，按 TDesktop 首屏和私聊流程补 `done/stub/todo` 状态。

## Load Baseline

压测 harness：[`internal/loadtest/send_load_test.go`](../internal/loadtest/send_load_test.go)，env-gated，未设 `TELESRV_TEST_POSTGRES_DSN`+`TELESRV_TEST_REDIS_ADDR` 时 Skip，对默认 `go test ./...` 无副作用。

**方法**：与 main.go 一致装配连接池 + Redis 分配器 + PG 消息存储 + transactional outbox + 真实 `OutboxDispatcher`（binder 用零连接 `SessionManager`，`PushToUserExceptSession` 返回 0，因此 outbox 走完整 `claim→批量取事件→push→批量标记` 的 PG 往返，测排空而非网络 fanout）。closed-loop 饱和：`concurrency` 个 worker 各自不停发满 `messages` 条。`TELESRV_LOAD_DEFER_DISPATCH=1` 时先把积压攒满、发送结束才启 dispatcher，用以隔离测量 outbox **纯排空上限**（否则 dispatcher 实时跟上发送、积压近 0 量不出天花板）。

**运行**（PowerShell）：

```powershell
$env:TELESRV_TEST_POSTGRES_DSN = "postgres://telesrv:telesrv@localhost:5432/telesrv?sslmode=disable"
$env:TELESRV_TEST_REDIS_ADDR   = "localhost:6399"
# 稳态（dispatcher 与发送并发）
go test ./internal/loadtest/ -run TestMessageSendBaseline -v -count=1 -timeout 240s
# 纯排空上限（先攒满积压再排空）
$env:TELESRV_LOAD_MESSAGES=30000; $env:TELESRV_OUTBOX_BATCH=200; $env:TELESRV_LOAD_DEFER_DISPATCH=1
go test ./internal/loadtest/ -run TestMessageSendBaseline -v -count=1 -timeout 300s
```

可调 env：`TELESRV_LOAD_USERS`(50) / `_CONCURRENCY`(32) / `_MESSAGES`(5000) / `_POOL_CONNS`(64) / `_DEFER_DISPATCH`；outbox 用 `TELESRV_OUTBOX_WORKERS`(loadtest 默认 8，运行时默认 2) / `_BATCH`(100) / `_INTERVAL` / `_LEASE_TIMEOUT`；`TELESRV_LOAD_ENFORCE_SLO=1` 时 SLO 超标硬失败（回归门禁）。正确性（无发送错误/无意外重复/全部发出/outbox 必须排空/无终态失败）始终硬断言。

**生产化做了什么**（2026-05-31）：

- **连接池配置化**（`TELESRV_POSTGRES_MAX_CONNS` 默认 50，`TELESRV_POSTGRES_MIN_CONNS` 默认 16，经 `postgres.WithMaxConns/WithMinConns`）。`postgres.Open` 会在启动时显式预热 min 连接，避免 TDesktop 双端冷启动时大量 RPC 一边建 PG 连接一边排队；pgx 默认 `max(4,NumCPU)` 在高并发下排队，是首版 send max 2s 突刺的根因。
- **dispatcher 并行**（`TELESRV_OUTBOX_WORKERS`，运行时默认 2，loadtest 默认 8，`rpc.WithOutboxWorkers`）。N worker 各跑 claim 循环，靠 `FOR UPDATE SKIP LOCKED` 互不重叠；本地 TDesktop 双端启动默认偏保守，避免多个 worker 同扫 64 分区父表触发 PG lock/shared-memory 压力。
- **dispatcher 批量**（`store` 具备批量能力时自动启用，否则逐条回退）。每批一次 `BatchListDispatchEvents`（双 `unnest WITH ORDINALITY USING(ord)` 配对 (user_id,pts) 的 join）+ 一次 `MarkDispatchDeliveredBatch`，把每条 ~2 次 PG 往返降到每批 ~3 次。

**outbox 排空上限演进**（本机 docker PG+Redis，攒满 6 万行积压后纯排空）：

| 方案 | drain 速率 | 相对 |
|---|---|---|
| 单 worker 逐条（首版） | ~270 行/s | 1x |
| 8 worker 逐条（仅并行） | ~1711 行/s | ~6x |
| **8 worker 批量（当前）** | **~12285 行/s** | **~45x** |

稳态下（concurrent，出厂默认 users=1000/concurrency=32/8 worker/batch100）：**峰值积压仅 ~330 行、排空 ~80ms**——dispatcher 实时跟上发送（~2500 行/s 入队）；send p99 ~114ms、getDifference p99 ~2ms、吞吐 ~1255 msg/s，SLO 全过。12k 行/s ≈ 6k msg/s 可持续 outbox 吞吐；16 worker/batch500 仍 ~11.7k 行/s（与 8 worker 持平 → 瓶颈已是 PG 而非 worker 数）。

> send p99 对**用户池集中度**敏感：harness 用 50 用户时 p99 飙到 ~220–325ms（32 并发挤少数用户的 dialog/message_box 行，行锁争用），2000 用户降到 ~58ms。这是小池假象，生产 20 万用户分散后争用极低；默认已取 users=1000。

**面向 20 万在线的剩余 levers（按 §8 记账，本轮未做）**：

1. **PG 已是共享瓶颈**：批量化后 send 与 drain 都打在同一 PG 上，本机 ~12k 行/s 触顶。生产需更强的 PG（多核/NVMe/调参），更高规模再上读副本 / 按 user 分库。worker/batch/池均可调以匹配硬件。
2. **send 路径 Redis 分配已移出事务（已做）**：原先 sender/recipient 的 `pts`/`box_id` 分配（4 次 Redis 往返）嵌在 PG 事务内，持连接空等 Redis。现已移到 `BeginTx` 之前——分配本就走 Redis 不属 PG 事务，前移后不再在持有 PG 连接（与行锁）期间空等，连接周转更快（池受压时收益最大；收益随 Redis RTT 增大，远端 Redis 更明显）。本机 docker（Redis 亚毫秒 RTT）下 pool=24/conc=96（4x 超订）实测 send p99 ~130ms（<150ms）、~1800 msg/s、0 err 0 dup、SLO 全过。
   - **配套的 pts 连续性兜底（mtproto 对齐，已做）**：分配前移加宽了「pts 已分配未提交」的瞬时空洞窗口（并发发送 commit 重排序，本就存在），故 `updates` 服务改为只暴露**连续 pts**：`getState` 报告最大连续已提交 pts（非 allocator 最大已分配值），`getDifference` 只返回从客户端 pts 起连续的事件、遇空洞即截断、`State.Pts` 取最后连续值、超 `limit`(100) 置 `updates.differenceSlice`。客户端永不越过在途空洞，空洞由 commit/补洞在毫秒内自愈，下次拉取补齐——绝不丢消息。连续值由 `UpdateEventStore.MaxContiguousPts`（PG 顶部 4096 窗口）计算。正确性已由单测（空洞截断/slice 翻页/getState 连续）+ 真实 PG 集成测试（200 并发发送 pts 严格 1..200 无空洞无重复无丢失、PG 空洞场景）覆盖。
   - 仍可继续：把 4 次 Redis 分配 pipeline/合并成 1 次（每用户 pts+box 一个 Lua、sender/recipient 并行），进一步缩短分配耗时。
3. **连接层 `SessionManager` 全局锁——已实测，非吞吐瓶颈（结论被数据修正）**：原假设是"pushToUser 抢全局锁 = 20 万在线最大瓶颈"。新增连接层 benchmark（`session_manager_bench_test.go`，20 万连接、内存 Conn 不走真实 socket）实测：
   - **push fanout**：~4.6M push/s（24 核），延迟 1→24 核仅 249ns→328ns，**几乎不随核数恶化**——临界区只是 snapshot 一个 per-user 小 map，锁极短。20 万在线 @ 200 msg/s × 双端 = ~40 万 push/s，**比上限低一个数量级，push 不是瓶颈**。
   - **Register/Unregister churn**：全局**写锁**，~2067ns/op @ 24 核 ≈ 48 万 churn/s。稳态 churn（20 万连接 × 平均存活分钟级）≈ 千级/s，余量巨大。
   - mutexprofile 确认两者都 100% 串行在同一把锁，但因临界区极短，**当前规模下不构成瓶颈**。
   - **真正的风险点**（分片的价值所在，但非紧急）：重连风暴（断网恢复 / 发版重启）时瞬时 churn 飙升，**写锁会阻塞同期所有 push 读锁**。按 userID 分片可把写锁从全局 1 把拆成 N 片，churn 风暴只影响 1/N 的 push——这是**稳健性/尾延迟**提升，不是吞吐解锁。建议留待真有重连风暴尾延迟问题、或单机连接数再上一个量级时再做。
   - 仍未覆盖：真实 socket 的 per-conn 加密/写出背压、20 万 fd 的内核态开销——那需要真实网络压测环境，非本 harness（内存 Conn）目标。

## Multi-Account / Multi-Device

- `authorizations` 是 auth_key 到 user 的权威绑定。
- active session 缓存 `auth_key_id + user_id`；router 额外按业务 auth_key 缓存 `user_id`，并用 singleflight 合并启动期并发 miss，避免同一永久 auth_key 派生的多个 temp session 重复查授权表。
- `update_states` 主键是 `(auth_key_id, user_id)`；同设备退出登录或换账号只清 auth_key 设备状态，不删除账号级 `user_update_events`。
- 一个账号多设备共享 `user_update_events`，各设备通过自己的 `auth_key_id + user_id` 状态和 `updates.getDifference` 补偿。
- 联系人备注、dialog pinned/order/manual unread、peer settings 都是 owner 视角数据；写业务表后同步写账号级 durable update event，并把投递任务写入 `dispatch_outbox`，避免“只在线 push、离线设备永远不知道”的状态漂移。

## ACK / Global Sequence Note

`参考实现` 的可借鉴点是：`msgs_ack` 不直接生成业务事件，它只确认某个已发送 server msg_id / RPC response 已被客户端收到；server 通过 ack cache 找回该响应对应的 `pts` / `globalSeqNo`，再推进 `(auth_key_id,user_id)` 维度的已确认水位。这个设计适合频道、多 peer 混合更新和 server 侧 delivered watermark。

当前阶段私聊文本只需要账号级 `pts` durable log，客户端 `updates.getDifference` 仍以请求里的 `pts` 为准；MTProto ACK 只释放出站重发缓存。后续引入频道或跨 peer 全局更新流时，再新增独立的 per-device `global_seq_no` / delivered watermark，不把它塞进 MTProto ACK 状态本身。

## 客户端 pts 重排依赖 pts_count 准确（TDesktop PtsWaiter，已核对源码）

> 结论来自实读 TDesktop pinned baseline（`9caf32dffc`）源码：
> `tdesktop/Telegram/SourceFiles/data/data_pts_waiter.cpp`、`data_pts_waiter.h`、`api/api_updates.cpp`。

**背景**：outbox worker 多 worker 并发 claim + 发送事务乱序提交，所以**主动推送可能乱序到达客户端**（pts=6 的 `UpdateNewMessage` 先于 pts=5 发出）。这不会让客户端乱序或丢消息——前提是每条 update 的 `pts` / `pts_count` 准确。

**客户端如何处理乱序**（`PtsWaiter::check`，data_pts_waiter.cpp:170-187）：客户端维护 `_good`（已应用的最大连续 pts）、`_last = max(见过的 pts)`、`_count += 每条 update 的 pts_count`：

- 收到 pts=6（count=1）：`_last=6, _count=5` → `_last > _count`（中间缺 pts）→ 这条 update **不应用，先缓存进 `_updatesQueue`**，`setWaitingForSkipped(1000ms)`。
- 随后收到 pts=5（count=1）：`_last=6, _count=6` → 相等 → `_good=6`，`applySkippedUpdates` 把缓存的 5、6 **按序应用**（data_pts_waiter.cpp:48-72）。
- 若 1 秒（`kWaitForSkippedTimeout`，data_pts_waiter.h:24）内空缺没补上 → 触发 `getDifference` 主动补齐。

即客户端**缓存乱序、等空缺、按序应用，1 秒补不齐才 getDifference**——比"丢弃重拉"更优雅。普通私聊 updates 与 channel 复用同一个 `_ptsWaiter` 单例（调用时 channel 传 `nullptr`，api_updates.cpp:603-604）。

**对 server 的硬约束**：客户端这套重排完全依赖 `_count += pts_count` 累加，因此 server 必须保证：

1. **每条 update 的 `pts_count` 准确等于它推进的 pts 步数**。私聊文本、已读 inbox/outbox、文本编辑恒为 1；批量删除为 owner 视角删除 message_id 数量：`UpdateDeleteMessages{Pts: event.Pts, PtsCount: len(message_ids)}`。
2. **每个分配出去的 pts 最终都能被 getDifference 拿到**（即使事务回滚，也要写 `noop` 占位，见 `recordPtsGaps`）——否则连续水位永远卡在空缺处，客户端会永久 `getDifference` 重试或永久 gap。

**当前安全**：私聊文本、转发、已读回执、文本编辑 `pts_count` 恒为 1，删除消息 `pts_count` 等于删除数量；每个 pts 都有真实事件或 noop 占位，且 getState/getDifference 只暴露连续 pts（见上文 Load Baseline 的「pts 连续性兜底」）。**未来风险**：引入其它“一次操作产生多条 update / `pts_count ≠ 1`”的场景（批量 service action、频道 editMessage 等）时，若 server 的 `pts_count` 算错，客户端 `_count` 会永久错位 → 永久 gap。新增此类 RPC 时必须同步核对 pts_count 语义。

## Current Limits

- `messages.sendMessage` 仅支持私聊文本；reply 仅支持同一私聊 peer 的 `inputReplyToMessage`。
- `messages.forwardMessages` 仅支持当前 owner 可见的私聊文本消息转发到 user peer，单次最多 100 条。
- `messages.editMessage` 仅支持文本消息编辑；网页预览 media 降级文本编辑，真实媒体、reply markup、quick replies、scheduled edit 留后续并返回显式 TL 错误。
- `messages.deleteMessages` / `messages.deleteHistory` 仅支持私聊消息；`min_date/max_date` 当前作为兼容 no-op，服务消息 `messageActionHistoryClear` 留到后续消息类型扩展。
- `messages.deleteHistory` 全清按 1000 条一批推进，`offset>0` 续删；`deleteMessages` 单次最多 1000 个 id，避免客户端构造超大数组或超大 `max_id` 导致服务端 OOM。
- `messages.getHistory/messages.search` 的 `add_offset` 统一 clamp 到 `[-100,100]`；TDesktop 正常滚动只会使用小偏移，恶意超大值不能触发无界内存分配或大 SQL OFFSET。
- `reply_markup`、quick replies、effects、paid send、suggested posts、story/monoforum/todo/poll reply 均返回客户端可理解的显式 TL 错误（如 `REPLY_MARKUP_INVALID`、`SHORTCUT_INVALID`、`EFFECT_ID_INVALID`、`PAYMENT_UNSUPPORTED`、`SUGGESTED_POST_PEER_INVALID`、`STORY_ID_INVALID`、`REPLY_TO_MONOFORUM_PEER_INVALID`、`POLL_OPTION_INVALID`），不再落 `NOT_IMPLEMENTED`；`send_as` 仅在目标为 channel 时按频道模块规则接受 self/current channel，私聊目标返回 `SEND_AS_PEER_INVALID`；目标为 channel 且请求未显式带 `send_as` 时，RPC 层读取 `messages.saveDefaultSendAs` 保存的默认身份并重新校验。
- 定时消息返回 `SCHEDULE_DATE_INVALID`。
- `messages.search` / `messages.searchGlobal` 当前是私聊文本 `ILIKE` + `pg_trgm`，后续大规模数据需要按语言/分词策略补全文索引或外部搜索服务。
- 单条文本当前限制 4096 个 Unicode code point；超限返回 `MESSAGE_TOO_LONG`，触发窗口限流返回 `FLOOD_WAIT_X`。

## 媒体消息（2026-06-02）

- `messages.sendMedia` / `uploadMedia` / `sendMultiMedia` 接入：RPC 层 `resolveInputMedia` 把 `inputMediaUploadedPhoto/Document`（组装 `upload.*` 分片→建 `Photo`/`Document`）与 `inputMediaPhoto/Document`（引用已存在资源，含贴纸）转成 `domain.MessageMedia`，经抽取的 `sendOutgoing` 走与文本完全相同的 pts/box/outbox/在线推送/离线 `getDifference` 路径；私聊与 channel 共用。
- `private_messages` / `message_boxes` 增 `media` JSONB 快照列：发送在事务内随 body/entities 一起写双端盒子，历史 `getHistory`/`getMessages`/dialog preview 读取时随消息一并解码，无需 join `documents`/`photos`。转发复制源消息 media（同一文档引用）；文本编辑保留 media。
- `tgMessage` 在 media 非空时 `SetMedia(MessageMediaPhoto/Document)`，客户端经 `upload.getFile` 从 blob backend 下载。Document id 在 domain/store 中保持 telesrv-owned 正数；外部 seed source id 在导入阶段归一，`InputDocument` / `inputDocumentFileLocation` 入站直接按服务端 id 解析，避免把第三方导出 id 当成本服资源身份。
- 仅媒体消息（无 caption）：放宽 `private_messages` body 非空 CHECK 为 `body<>'' OR media<>'{}'`。
- 范围外：grouped_id 相册聚合（sendMultiMedia 当前各条独立成消息）、geo/contact/poll/todo/dice/story media 仍 `MEDIA_INVALID`。
