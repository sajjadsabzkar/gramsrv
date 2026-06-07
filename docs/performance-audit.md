# telesrv 性能审计报告

- **日期**：2026-06-02
- **审计范围**：全部 140 个 Go 源文件（约 4.7 万行非测试代码）——连接层 `mtprotoedge`、`rpc`、业务 `app/*`、`store`（PostgreSQL / Redis / memory）、`queries/*.sql` 与 55 个迁移的索引。
- **方法**：5 个并行深度审计（PG 私聊查询 / PG channel 查询 / 连接层 fan-out / RPC 私聊热路径 / channel RPC + Redis），再对全部 P0 逐条读码核实（见附录 A）。
- **性质**：本文是当前阶段的**性能快照与债务维护清单**。RPC 实现进度仍以 [compatibility-matrix.md](compatibility-matrix.md) 为权威；性能项完成后需在本文记录状态、验证方式与剩余阻断。
- **当前阶段**：已不再是第一阶段/纯私聊阶段；当前目标是 TDesktop 主路径功能闭环 + 性能/可靠性硬化，大规模群组/频道放开前必须先清掉 P0 阻断项。
- **本轮落地状态（2026-06-02 / 2026-06-03）**：已修复 P0-1/P0-2/P0-4/P0-5、P1-b/P1-e/P1-i/P1-l、`users.getFullUser.common_chats_count` 反向规划 channel 分区、媒体 range read/元数据 LRU/sticker 小资源预热与 P2 update watermark/retention 的公共根因；P0-6 的 AES key schedule 属 MTProto per-message msg_key 成本，不做错误缓存优化，先保留 buffer/编码压测项；P1-a 受 `gotd/td` transport 抽象限制，禁止修改 `github.com/gotd/td`，需另做 telesrv-owned transport writer spike。
- **严重度口径**：`P0` 当前即瓶颈 / 开闸即炸；`P1` 上量级会炸或高频路径浪费；`P2` 浪费但不致命；`P3` 轻微。
- **规模假设**（用于判断后果）：单机目标 20 万在线 TCP 长连接；私聊 ~200 msg/s × 双端写；在线推送 fan-out ~40 万 push/s；PostgreSQL（默认连接池 50）+ Redis。

---

## 1. 总体结论

**基础工程素养很好**——大表按 user / channel 做 HASH 64 分区、私聊热路径 keyset 游标分页（无大 OFFSET）、服务端 limit 钳制几乎覆盖所有 RPC、`convert` 层预分配 + 去重、send 路径 Redis 已合并单 Lua、outbox 已批量化、SessionManager 锁临界区已被实测确认极短。**没有发现「按客户端巨值无界分配」这类可被单请求 DoS 打爆的洞。**

真正的性能债高度集中，归为**两条主线**：

| 主线 | 何时致命 | 性质 |
|---|---|---|
| **A. channel / 群组 fan-out 乘积放大** | 放开群组规模后 | 一条群消息触发「全员写 + 全量排序 + 数千次单查」的乘积爆炸 |
| **B. 连接层出站热路径 + 服务层缺批量** | 当前 TDesktop 主路径上量后放大 | 全局锁串行、每消息多次堆分配 / syscall、N+1 |

> 注：channel 代码虽已实现并通过功能联调，但**性能压测主要覆盖私聊**。主线 A 的几项在小规模群联调下不会暴露，需在放开大规模群组前先拆。

---

## 2. 核心洞察：一条群消息的乘积放大链

发一条 megagroup 消息，会同步触发**四段独立放大**（全部经代码核实，见附录 A）：

```
发送 1 条群消息（设 10 万成员，其中在线池 20 万里有该群成员若干）
│
├─① 存储层：upsertChannelDialogsForMessageTx   internal/store/postgres/channel.go:7958
│     对【全体活跃成员】INSERT…ON CONFLICT，WHERE 无 LIMIT
│     → 写 ~100,000 行 / 条消息，全在发送事务内，长持行锁 + 撑爆 WAL
│
├─② 选收件人：pushChannelUpdates → OnlineChannelMemberUserIDs
│     已从旧的全局在线 map 遍历改为 channel→online session 成员索引
│     → 持久 channel updates 按当前在线 active 成员 best-effort 推送；typing/reaction 等瞬时事件走 viewer 索引
│
├─③ 推送构造：pushChannelUpdates 对【每个 recipient】调 build(viewer)   internal/rpc/channels.go:3053
│     发送者 / 频道信息对所有 recipient 相同，却被重算 500 遍
│
└─④ 每次 build 内：tgUsersForIDs 逐个 Users.ByID   internal/rpc/channels.go:3143
      而 Service.ByID 每次先查 Self 再查目标（×2）   internal/app/users/service.go:50
      → 500 viewer × ~5 引用 × 2 ≈ 5,000 次单行 PG 查询 / 条消息
```

**旧链路合计：单条群消息 ≈ 10 万行写 + 一次 20 万项排序 + ~5,000 次单行查询。** 当前已拆掉全局在线排序和 users 公共 N+1，剩余风险集中在大群 dialog 写放大、收件人无关 update 模板化，以及 channel 读 RPC 的专用批量化。

---

## 3. P0 — 必修（当前即瓶颈 / 开闸即炸）

### P0-1　广播频道发消息对全员同步写 dialog（无上限写放大）
- **位置**：`internal/store/postgres/channel.go:7958` `upsertChannelDialogsForMessageTx`，由 `sendChannelMessageOnce` 在每条消息事务内调用。
- **问题**：`FROM channel_members WHERE channel_id=$1 AND status='active'` 无 LIMIT，每发一条消息写 / 更新 = 成员数 行；讨论组联动再来一遍。
- **影响**：唯一随频道规模**线性放大且在同步关键路径**的写。百万成员频道单条 post 跑数秒、长持行锁、阻塞该频道后续所有写。
- **建议**：广播频道（broadcast）不维护 per-member dialog，改 Telegram 式「单一 read state + 读时按 `top_message_id` vs `read_inbox_max_id` 懒算未读」（`ListChannelDialogs` 已有该回退逻辑，fan-out 行其实可删）；megagroup 设成员阈值，超阈值改异步 / 分批 job。
- **状态**：已加同步写闸门：broadcast 跳过全员 dialog 写；megagroup 仅 `ParticipantsCount <= 1000` 时同步写，未知/超阈值不做全员写。读取端已不再信任 broadcast/大超级群的 `channel_dialogs.unread_count` 缓存，改按 read watermark、`available_min_id`、channel top 与未删除消息动态派生普通未读。仍需大频道 PG `EXPLAIN` 与端到端压测。

### P0-2　`OnlineUserIDs` 每次 fan-out 全量遍历 + 排序 20 万在线
- **位置**：`internal/mtprotoedge/session_manager.go:469`。
- **问题**：截断发生在排序**之后**——为拿 500 个先把 20 万个全 append 全 `sort.Slice`，且全程持 `m.mu.RLock`。
- **影响**：每次 channel 更新都触发 O(n log n) 排序 + ~200KB 分配，持读锁阻塞连接注册 / 注销，是连接层热锁上的持续 CPU / GC 压力。
- **建议**：提供 O(1) 的 `IsOnline(userID)`，用频道成员（已 LIMIT 500）反向过滤，不导出全量在线集；若必须导出则「遍历到 limit 即停、不排序」。
- **状态**：已加 `IsUserOnline` / `OnlineUserIDsForCandidates` / viewer 索引 `OnlineChannelUserIDs` / membership 索引 `OnlineChannelMemberUserIDs`。`updates.getState/getDifference` 会分页加载当前 session 已加入的 active channel；join/leave/invite/ban/delete 路径会运行态增删索引。持久 channel updates 走在线成员索引并经 PG active membership 复核，取消 500 cap；typing/reaction 等瞬时事件走 viewer-only scope；`messages.getDialogs` 不再标记 viewer。旧 `OnlineUserIDs` 保留兼容但已改为到 limit 即停、不排序。Redis/PG 缓存在线关系暂不引入，待多实例或压测数据证明需要再做。

### P0-3　channel fan-out 每收件人重建相同 user/chat + N+1 单查
- **位置**：`internal/rpc/channels.go:3053` `pushChannelUpdates` → `internal/rpc/channels.go:3143` `tgUsersForIDs`。
- **问题**：收件人无关的发送者 / 频道信息被每个 viewer 重算；每次 `tgUsersForIDs` 逐个 `Users.ByID`。
- **建议**：把收件人无关的富集提到 fan-out 循环**外**做一次，循环内只算 viewer 差异（out/self，纯内存）；配合 P0-4 的批量接口。
- **状态**：已先把 `tgUsersForIDs` 改为批量 `Users.ByIDs`，消除该公共 N+1。收件人无关的 channel update 模板化仍待压测后继续拆。

### P0-4　`Users.Service.ByID` 每次额外查一次 `Self`（全局 ×2 放大）+ 服务层缺批量
- **位置**：`internal/app/users/service.go:50`，被 RPC 层 **24 处** N+1 循环调用。
- **问题**：每次解析一个 user = 2 次 PG 查询；且 `UsersService` / `MessagesService` / `ChannelsService` **根本没暴露批量 `ByIDs`**，存储层明明已有 `listUsersByIDs` / `listChannelsByIDs`（`= ANY`）却用不上。
- **建议**：① 给三个 service 加批量 `ByIDs`，底层接已有的 `= ANY` 查询；② `ByID` 热路径不必每次重查 `Self`（`currentUserID` 已由 session 鉴权）。**这是 P0-3 与下面一批 P1 N+1 的公共根因。**
- **状态**：已给 `UserStore` / `UsersService` 增加 `ByIDs`、`ByPhones`，`UsersService.ByID` 去掉重复 `Self` 查询；channel/user 富集公共函数已迁到批量 users。Messages/Channels 更细粒度批量接口仍在后续项。

### P0-5　`MessageIDGen` 进程级全局锁串行化所有连接的出站写
- **位置**：`internal/mtprotoedge/outbound.go:432` → 共享单例 `internal/mtprotoedge/server.go:184`（创建）/ `server.go:202`（注入每个 Conn）。gotd `proto.MessageIDGen.New` 全程持 `g.mux`。
- **问题**：全进程一个 `s.msgID`，20 万连接共享；每条出站消息（rpc_result / push / ack / pong）必过这把锁。40 万 push/s ⇒ ~50 万次/s 全局锁 acquire，多核下 cache-line 争用 + lock convoy，把本可并行的 N 个 outbound actor 强行串到一点。
- **建议**：改 **per-Conn `MessageIDGen`**（msg_id 只需单连接内单调，无需跨连接全局唯一）。msg_id 已只在该连接 outbound actor 单 goroutine 内调用 → per-Conn 后退化为无争用锁甚至无锁 int64。**改动最小、收益最大。**
- **状态**：已完成 per-Conn `MessageIDGen`，移除 server 级共享 generator。

### P0-6　出站写路径每条消息 4–6 次堆分配，gotd 零拷贝编码被旁路
- **位置**：`internal/mtprotoedge/outbound.go:419-481`（`body.Copy()` + `var out bin.Buffer` + 回退到 `Encode` 再复制 + 每条 `aes.NewCipher` 重建 key schedule）。
- **影响**：40 万 push/s × 5~7 次分配 ≈ **250 万 alloc/s**，是稳态主导 GC 压力；20 万连接广播同一 update 时同份 body 被各连接独立 encode + copy + encrypt。
- **建议**：outbound actor 内复用 per-Conn encode / encrypt buffer（单 goroutine 天然安全，免锁）；走 `Message`-based `EncodeWithoutCopy`；**广播 push 在 fan-out 前把 body 编码一次**，各连接只做 salt/msgID 包头 + 加密。
- **状态**：未完全落地。AES `NewCipher` 不作为缓存目标：MTProto 2.0 每条消息由 msg_key 派生 AES key/iv，不能按 auth key 长期复用 cipher。后续只做 TL body 预编码、buffer 复用和实测分配优化。

---

## 4. P1 — 上量级会炸 / 高频路径浪费

### 连接层（TDesktop 主路径上量后相关）
- **P1-a　每帧两次裸 `write` syscall，无 bufio** — `internal/mtprotoedge/outbound.go:473` → transport 直写裸 `net.Conn`，每条 ≈3 次 syscall（含 `SetWriteDeadline`）。40 万 push/s ⇒ ~120 万 write syscall/s。`gotd/td` transport 当前隐藏底层 writer，项目规则禁止改 `github.com/gotd/td`；已在 `Conn` 内加 telesrv-owned outbound writer 接口作为替换点，但真正 bufio/syscall 合并仍需 transport writer/codec spike 后再落地。
- **P1-b　fan-out 中 `c.Send` 同步阻塞，慢连接拖累整批** — `internal/mtprotoedge/session_manager.go:446` + `internal/mtprotoedge/outbound.go:117`：`Send` 同步等到写完 / 超时，队列满时阻塞 fan-out 协程最长到 ctx 超时（5s）。**状态：已新增 `Conn.SendBestEffort` 与 SessionManager best-effort fan-out，updates push 默认只等 `TELESRV_OUTBOUND_PUSH_TIMEOUT=200ms` 入队；RPC result/ack/pong 仍走可靠同步发送，outbox 入队失败不删 durable 任务。**
- **P1-c　入站每帧分配新 buffer** — `internal/mtprotoedge/server.go:344`：连接级 `bin.Buffer` 复用被 codec `ResetN` 的 `make` 抵消，每帧新分配明文缓冲。建议入站缓冲走 `sync.Pool`。

### N+1（根因 = P0-4 服务层缺批量）
- **P1-d　`forwardSources` 转发逐条查源 + 富集 N+1** — `internal/rpc/messages.go:4155`：100 条转发 = 100 次取源 + ~100 次富集查询。
- **P1-e　`getMessages` 逐 ID 单查** — `internal/rpc/messages.go:2888`：高频 RPC，100 个 ID = 100 次 `Search(Limit:1)`。**状态：已新增 `MessagesService.GetMessages` / `MessageStore.GetByIDs`，PG 用 `unnest(ids) WITH ORDINALITY` 一次取 owner-visible box ids；RPC 保持缺失项返回 `MessageEmpty`。**
- **P1-f　`getDifference` 富集逐 ID 单查 + 跨事件重复查** — `internal/rpc/update_peer_refs.go:9`：登录风暴期高频，每事件重建 map + 同 user 在多事件被重复查。
- **P1-g　channel 读 RPC 普遍 N+1** — `getParticipants` / `getAdminLog` / `getHistory` 等 24 处逐个 `Users.ByID`（`internal/rpc/channels.go:3350` 等）。
- **P1-h　`enrichChannelHistory` 与存储层重复解析** — `internal/rpc/update_peer_refs.go:45`：存储层已批量填好 `Users` / `Channels`，RPC 层又逐个重查一遍后 merge 丢弃（纯浪费）。
- **P1-i　`importContacts` 逐条 `ByPhone` + `Upsert`** — `internal/app/contacts/service.go:84`：登录潮高发，500 条 = ~1000 次串行往返，单请求长期占一条 PG 连接。建议 `WHERE phone = ANY($1)` + `unnest` 批量 upsert（~1000 → ~2）。
- **状态**：已改为批量 `ByPhones` + 批量 `UpsertMany`，同一目标用户去重写入，`Imported` 保留各 client_id。
- **P1-j　`ListForumTopics` 每话题 3 次 COUNT** — `internal/store/postgres/channel.go:5513` → `:6804`：单次最多 300 条额外 COUNT。建议改 `GROUP BY` 批量聚合（同文件 reactions / replies 已有该范式）。
- **P1-k　`ListChannelDialogs` 逐个回查 top message** — `internal/store/postgres/channel.go:3601`：主查询已 JOIN 到 top_msg 却只取了 date，又逐行 +100 单查；且过量取数 500 再内存排序切片。

### COUNT(*) 全量统计
- **P1-l　`ListMessagesByUser` 每页都 COUNT 全量** — `internal/store/postgres/queries/message.sql:380` + `internal/store/postgres/message.go:566`：每次翻页 / 搜索都把整个匹配集数一遍（trigram 只缩候选，count 仍大扫）。建议仅首页算 count、翻页省略（Telegram 客户端只首屏用总数）。
- **状态**：已新增 `MessageFilter.NeedTotalCount`，SQL 默认不 COUNT；私聊搜索首屏按显式 flag 计算，其余 history/翻页默认返回 0 count。
- **P1-m　`ListChannelRecommendations` 全库 COUNT** — `internal/store/postgres/channel.go:3824`：对全系统公开广播频道计数，带 `NOT EXISTS` 反连接。建议返回近似值或不返回精确总数。
- **P1-n　`users.getFullUser.common_chats_count` 反向规划 channel 分区** — ✅ **已修（2026-06-03）**：TDesktop 打开私聊会触发 `users.getFullUser`，旧实现为填 `common_chats_count` 调 `ListCommonChannels(limit=1)`，同时跑 COUNT + list，并从 `user_id` 入口反查按 `channel_id` hash 分区的 `channel_members/channels`，实测 Alice/Bob `EXPLAIN` planning 约 70ms、执行约 1.6ms，UI 表现为点开 Bob B 卡一下。现新增 `user_channel_member_index(user_id, channel_id)`，由 member upsert/leave/delete channel 事务同步；`users.getFullUser` 走 `CountOnly`，只在该 user 维度索引上 count。实测同一 count query planning 0.873ms、execution 0.079ms，Alice 打开 Bob B 日志中 `users.getFullUser` handler 3.26ms、端到端 9.49ms。

### PG 分区访问维度审计（2026-06-03）

本轮逐个核对 20 张 HASH 分区表，重点检查「查询入口维度」是否等于分区键。结论：`dialogs/dialog_drafts/dialog_filters/dialog_filter_settings/user_update_events/message_boxes` 的 owner/user 主路径基本都带 user 分区键；`channel_messages/channel_update_events/channel_forum_topics/channel_invites/channel_invite_importers/channel_admin_log_events/channel_message_viewers/channel_message_reactions` 的 channel 主路径基本都带 channel 分区键。剩余风险集中在需要反向访问的 read model / 队列：

| 级别 | 问题 | 实测计划 | 修复方向 |
|---|---|---:|---|
| P1 | `ListChannelDialogs` / `ListInactiveChannels` / `ListLeftChannels` / `ListDiscussionGroups` / `ListAdminedPublicChannels` / `ListActiveChannelIDsForUser` 从 `user_id` 入口读 `channel_members`，但表按 `channel_id` 分区 | `ListChannelDialogs` planning 118ms，`ListInactiveChannels` 114ms，`ListActiveChannelIDsForUser` 26ms，均展开 64 个 `channel_members` 分区；带 `channels/channel_messages` join 时最多展开 192 个分区 | 把 `user_channel_member_index` 升级为正式 user 维度 membership read model，或两步取 bounded channel_id 后用 `channel_id/id = ANY($1)` 访问 channel 分区；禁止 SQL 内直接 `index -> channel_members/channels` 动态 join，实测仍会展开 64 分区 |
| P1 | `message_boxes` 的编辑/reaction 可见 box 查询与 revoke 删除按 `(message_sender_id, private_message_id)` 反查，表按 `owner_user_id` 分区 | `ListVisibleMessageBoxesByPrivateMessage` planning 50ms、`DeleteMessageBoxesByPrivateMessages` 等价查询 48ms，展开 64 个 `message_boxes` 分区 | 增加 unpartitioned `private_message_box_index(message_sender_id, private_message_id, owner_user_id, box_id)`，或从 `private_messages(sender_user_id,id)` 得到 sender/recipient 后按 owner 分区两点查询 |
| P1 | `ListUserUpdateEventsAfter` / `BatchListDispatchEvents` 在账号级 update 查询里直接 left join `channels fwd_ch/reply_ch`，`channels` 按 id 分区但 join key 来自消息行 | planning 37ms，展开 128 个 `channels` 分区；`user_update_events/message_boxes` 本身已正确裁剪到 1 个 user 分区 | update 查询只返回 channel ref id，Go 层收集后批量 `listChannelsByIDs(id = ANY($1))` 富集，复用 channel difference 的做法 |
| P1 | `dispatch_outbox` worker 全局 claim/failed cleanup 按 `status/next_attempt_at/updated_at` 取任务，表按 `target_user_id` 分区 | claim planning 28ms，展开 128 个 `dispatch_outbox` 分区；这是队列热路径，不是用户 RPC 单次路径 | 引入 unpartitioned ready queue / claim index table，或改为固定 worker shard 维度分区；按 target_user_id 分区适合投递/删除，不适合全局 claim |
| P2 | `ResolvePublicChannelUsername` 从 `channel_usernames` join `channels`，动态 channel id 让 `channels` 64 分区全展开 | planning 37ms，展开 64 个 `channels` 分区 | 先查 `channel_usernames` 得到 channel_id，再 `getChannelByID` 单分区读取 |
| P2 | 删除频道消息时 `deleteChannelUnreadMentionsTx` 按 `channel_id + message_id` 删除 `channel_unread_mentions`，但该表按 `user_id` 分区；随后按 affected user 更新 `channel_dialogs` 也会动态展开 user 分区 | mentions delete planning 16ms，affected dialog update planning 24ms，分别展开 64 个 `channel_unread_mentions` / `channel_dialogs` 分区 | 删除前从 message mention read model 取 bounded affected user_id，或增加 `(channel_id,message_id,user_id)` 辅助索引表；更新 dialog 按 user_id 分批两点写 |

验证补充：

- `channels WHERE id = ANY($1)`、`channel_members WHERE channel_id = ANY($1)` 在参数/常量数组已知时能裁剪到对应分区；但 `WITH ids AS (...) SELECT ... WHERE id = ANY(ARRAY(SELECT ...))` 这种 SQL 内动态数组不会裁剪，planning 仍约 33ms/64 分区。
- `user_channel_member_index` 单表 user 维度查询 planning 0.7ms；但 `user_channel_member_index -> channel_members/channels` 一条 SQL 动态 join planning 71ms，仍不是可接受修法。
- `private_messages` 目前仅按 `sender_user_id` 幂等/编辑主体查询，未发现 recipient-only 热查询；`recipient_user_id` 索引存在但不会改变 sender 分区裁剪限制。

### 媒体管线（2026-06-03 媒体闭环审计新增，放开大文件 / 大群前评估）
- **P1-媒体-a　`upload.getFile` 整文件入内存 + 每 chunk 查 PG / sticker 小资源首开冷路径** — ✅ **已修（2026-06-03）**：原 `blobs.Get` 一次 `os.ReadFile` 读整个 blob 再切片，客户端按 ≤512KB/1MB 分块多次请求 ⇒ 同一大文件被重复整读 N 次（O(N²)）+ 每 chunk 一次 `GetFileBlob` PG 往返；sticker/reaction/thumb 虽小，但 TDesktop 重启或打开历史首次渲染时仍可能在 `messages.getStickerSet` + `upload.getFile` + 本地文件冷读上形成可感知卡顿。现 `BlobBackend.GetRange`/`LocalFS`（`internal/app/files/blobfs.go`）用 `ReadAt` 只读 offset+limit 段（`n` 受文件大小约束，超大 limit 不会按客户端值分配内存）；并加 `location_key→FileBlob` 进程内 LRU（容量 65536，元数据不可变故只读填充无需失效）消除每 chunk 的 PG 查；再加 `object_key→bytes` 小 blob LRU（单项 ≤256KB，总 64MB）、完整 sticker set cache 与启动 `WarmCaches`，从已 seed 元数据预热 sticker/reaction document 和可下载缩略图。参考实现 用 MinIO ranged read（`object.ReadAt`）+ SSDB 两级，但其下载命中冷存储不回填、无 LRU/阈值、photo/头像直连 MinIO；telesrv 当前先做进程内元数据/小字节 LRU + 段读，**多实例共享缓存（Redis）仍待二阶段换对象存储时评估**。实测启动预热 `49 sets / 2313 docs / 2298 blobs`，TDesktop `messages.getStickerSet` 从约 18-24ms 降至 0-1.7ms；seed document id 归一后，首次新服务端 id 会触发必要的 `upload.getFile` 主体/缩略图请求，但 server 端大多 0.5-6ms、样例最长 59ms，重复打开 Bob 会话 0.58s 截图已可见 sticker。

- **P1-媒体-a.1　system sticker set 污染 installed stickers 本地索引** — ✅ **已修（2026-06-03）**：TDesktop 贴纸正文缓存 key 为 `dc_id + document_id`，但 installed stickers 本地索引写入依赖 set flags；`installed_date` 会使 set 进入 Installed，普通 stickers 类型的 `Installed + NotLoaded` set 会让 `writeInstalledStickers()` 中止。此前 seed 把 animated emoji/dice/generic animations 这类 system set 也标成 installed，可能导致重启后 installed sticker 索引反复失效与 set 元数据重拉。现 migration `0066_system_sticker_sets_not_installed` 修复既有库，seed 后续也不再把 `set_kind=system` 声明为 installed。
- **P1-媒体-a.2　历史页 sticker 静态图慢 / 打开会话先空白** — ✅ **已修正（2026-06-07，撤销 2026-06-03 的过度修复）**：根因复盘 — TDesktop `history_view_sticker.cpp` 对 animated sticker 的占位渲染优先级为 `getStickerLarge()`（完整 `.tgs` 首帧，需下载）→ ~~`thumbnailInline()`（stripped 内联，源码显式注释禁用：sticker 需 alpha 通道）~~ → `goodThumbnail()` → 下载的 `thumbnail()` → **`paintPath()`（`PhotoPathSize` 矢量轮廓，内联即时，唯一不依赖下载的占位）** → 空白；且完整 `.tgs` 由 `DocumentMedia::checkStickerLarge()`→`automaticLoad()` 下载，**与 path/thumbnail 无关**。2026-06-03 的 a.2 误把"卡在 path 等完整 TGS"当作 path 短路（真根因是 a.3 的 document id 污染让完整文件下载/缓存失败），改为有 raster 时过滤 `PhotoPathSize`。a.3 修掉 id 后，这个过滤变成净负面：document 失去唯一即时占位 ⇒ 打开会话 sticker 先空白；且 `thumbnailPath()` 变空触发 `thumbnailWanted()` ⇒ 每个 sticker 多下载一次缩略图，与完整 `.tgs` 争用下载通道，几十个 sticker 排队 ⇒ "静态图特别慢、不像本地加载"。现 seed 恢复 `PhotoPathSize` 占位（与官方 sticker 一致，`internal/app/files/seed.go` 不再过滤 path，删 `seedPreferRasterDocumentThumbs`/`documentThumbsHave{Raster,Path}`），document.thumbs = path + downloadable `photoSize m`：打开即时画轮廓占位、有 path 后不再主动下载缩略图、下载通道全给完整 `.tgs`、第二次本地缓存秒显。已实测排除磁盘 IO/缓存容量（blob 4330 文件 71.3MB，几乎全 ≤64KB）与 `document.Size` 不匹配（28/28 一致）。**现有库需重新 seed 一次恢复 path**：`DELETE FROM sticker_sets; DELETE FROM available_reactions;` 后重启触发全量 reseed（`PutDocument`/`PutFileBlob` upsert，不删用户上传媒体）。`TestSeedMediaFromRealExport` 用真实导出验证 seed 后 document 保留 path。**待双 TDesktop 人工验证渲染体感。**
- **P1-媒体-a.3　复用导出 document id 命中 TDesktop 旧 `DocumentData` 缓存** — ✅ **已修（2026-06-03）**：TDesktop `Data::Session::document(id)` 按 `document_id` 单例化，`DocumentData::updateThumbnails()` 不会清空旧 inline/path thumbnail；因此 server 修掉坏 thumb 字段后，旧 Debug tdata 仍可能用同 id 的污染对象，打开历史先空白再等完整 TGS。根因是 seed 曾把外部导出 document id 直接作为本服资源主键。现 `internal/app/files` 在导入阶段把 source id 归一为 telesrv-owned storage id，RPC/download/custom emoji/channel appearance 全部直接使用该服务端 id；migration `0067_seed_document_id_namespace` 修复既有库的 documents/file_blobs/sticker_sets/available_reactions/message media/channel appearance 引用。这样不需要改官方客户端，也不在 RPC 层保留客户端特判。复测：migration 后 PG 相关引用均无 `>4e18` 外部 document id；双 TDesktop 重启后打开 Bob B/Alice A 首屏 250ms 已显示 sticker，`messages.getHistory` 约 6-12ms，`upload.getFile` 命中新 id 成功，server/client 日志无 location/hash/API 错误。
- **P1-媒体-b　`upload_parts` 无 GC / 无每用户配额** — `internal/app/files/service.go:30` + `deploy/migrations/0057_media.up.sql`：分片直接进 PG `bytea`，仅 `assembleUpload` 成功才删；**未 assemble 的分片永久滞留**，且不同 `file_id` 无上限 ⇒ 任意登录用户可用海量 fileID 各传几片撑爆 PG（容量 DoS）。建议每用户 in-flight 上传字节/分片配额 + 后台按 `created_at` 过期清理 worker。
- **P1-媒体-c　`media` JSONB 内联放大 fan-out 写** — `deploy/migrations/0057_media.up.sql:129`：`media` 快照内联在 `message_boxes`（owner 双份）/`channel_messages`，含 stripped thumb/attributes 使单行变大；叠加主线 A 的 channel 全员写扇出时，大群每条媒体消息按成员数复制整个 media JSONB。建议随主线 A 改懒算/单副本时一并评估 media 是否只存引用、下沉 `documents`/`photos`。
- **P2-媒体-d　`photos.getUserPhotos` N+1 + OFFSET** — `internal/app/files/photos.go:165` 逐个 `GetPhoto` + `internal/store/postgres/queries/media.sql:314` OFFSET 分页。头像数少、`limit<=100`，影响有限；建议批量 `GetPhotos(ids)` + keyset 分页。
- **P2-媒体-e　sticker set cover 元数据声明可下载但 seed 无 raster blob** — ✅ **已修（2026-06-03）**：TDesktop 会把 `StickerSet.thumbs` 中的 downloadable `PhotoSize` 变成 `inputStickerSetThumb` 下载；`TELESRV_STICKER_SEED_DIR` 当前 set_cover 只有 `PhotoPathSize` SVG（40 个 only-svg、4 个 empty），没有可服务的 jpg/png/webp。现 seed 与 `tgStickerSet` 转换均过滤 sticker set cover 的 downloadable thumb，只保留非下载占位；实际 sticker/reaction document 缩略图仍按 `inputDocumentFileLocation` 服务。

---

## 5. P2 / P3 — 浪费但不致命 / 轻微

| 级别 | 问题 | 位置 |
|---|---|---|
| P2 | `getDialogs` 全量拉内存排序 + 无条件拉 1000 条草稿（最高频 RPC 之一，应下推 SQL `ORDER BY LIMIT` + 按页 peer 取草稿） | `internal/app/dialogs/service.go:31` |
| P2 | `MaxContiguousPts` 每次 getState 读 4096 行（应 O(1) 持久化单列 / Redis） | ✅ 已新增 `user_update_watermarks`，`MaxContiguousPts` O(1) 读水位；缺行时一次性补算并 upsert。`Current()` 保留最大已提交 pts 语义，供 Redis allocator 恢复最大分配点，避免 gap 时回退 |
| P2 | **`user_update_events` / dispatch/outbox delivered rows 无保留期清理**，按长期在线量永久膨胀；`message_boxes` 是用户历史，不得在没有产品保留策略时通用 TTL 删除 | ✅ 已确认账号级 `user_update_events` 不能通用 TTL 裁剪，需永久保留以支持 TDesktop `differenceSlice` 续传；retention worker 仅清理 failed/过期 outbox，delivered outbox 已投递即删，不删除 `message_boxes` |
| P2 | `advanceChannelReadOutboxTx` 最多 128 次串行已读写（应单条集合写） | `internal/store/postgres/channel.go:5952` |
| P2 | `InviteToChannel` 200 人逐人循环 ~800 往返（应 `unnest` 批量） | `internal/store/postgres/channel.go:426` |
| P2 | `GetParticipants` OFFSET 分页 + `CASE role` 排序必触发 sort | `internal/store/postgres/channel.go:355` |
| P2 | `ReadHistory` / `EditMessage` / `DeleteMessages` 在 PG 事务内做 Redis pts 分配（send 已优化，这三条没跟上） | `internal/store/postgres/message.go:650` |
| P2 | `ReadHistory` / `RefreshDialog` 用 `COUNT(*)` 重算未读（应纯增量维护） | `internal/store/postgres/queries/message.sql:651` |
| P2 | `ListChannelDifference` 逐事件回查消息 | `internal/store/postgres/channel.go:6115` |
| P2 | `ListInactiveChannels` / `ListLeftChannels` 计算表达式排序 / OFFSET 分页（低频） | `channel.go:3756` / `:3729` |
| P2 | 私聊发送富集仍单查 self / 对端（最热路径，可缓存 self + 透传已查的对端） | `internal/rpc/messages.go:3911` |
| P2 | memory store 多处全 map O(n) 扫描（**仅测试 / 本地用，不影响生产**） | `internal/store/memory/memory.go:1287` 等 |
| P3 | SessionManager 注册表分片（仅重连风暴尾延迟，实测非稳态瓶颈，与既有结论一致） | `internal/mtprotoedge/session_manager.go` |
| P3 | `validateSeq` 每消息 O(400) map 扫描、热路径 hex 日志求值、`enrichUpdateEvents` 整片复制、`pushToUser` 每次切片快照 | 多处 |

---

## 6. 已确认健康（避免误报，证明审计严谨性）

- **send 路径 Redis 已是单 Lua**（`user_counter_allocator` 把 pts + box 合一，hash-tag 同 slot）——既定优化已落地，热路径仅 1 次 RTT。
- **私聊核心已达标**：双写 / dialog upsert / event / outbox 单事务、`ON CONFLICT(sender, random_id)` 幂等、keyset 分页无大 OFFSET、批量转发 / 删除已 `unnest`、outbox `FOR UPDATE SKIP LOCKED` + 多 worker + 投递即删、相关索引齐全。
- **channel 存储核心决策正确**：消息单副本存储（不给每成员写一行）、keyset 分页、服务端 limit 钳制、trigram 搜索索引、按 channel_id 分区；在线 fan-out 先用内存索引缩小到当前在线成员/active viewers，再用 `= ANY` 分批复核 active membership。
- **`convert.go` 预分配 + 去重到位，RPC 入参钳制完整**——无「按客户端值无界分配」DoS。
- **Redis 健康**：无大 key、无 `KEYS` / `SCAN` 全库扫、无 `SMEMBERS / LRANGE 0 -1`，RateLimiter 正常放行 1 RTT。
- **SessionManager 锁**：push 在锁外发送，临界区极短——既有 benchmark 结论仍成立（注意：这与 P0-2 的 `OnlineUserIDs` 全量排序是两回事，后者是新发现）。

---

## 7. 修复优先级路线图

**第一梯队 — 当前主路径就该做（与群组规模无关，直接影响 20 万连接稳态）**
1. **P0-5 `MessageIDGen` 改 per-Conn** — ✅ 已完成。
2. **P0-6 + P1-a 出站去分配 + bufio 合并 syscall** — 直击 250 万 alloc/s + 120 万 syscall/s。
3. **P0-4 服务层加批量 `ByIDs` + 去掉 `ByID` 重复 `Self`** — ✅ 已完成 users 公共根因；messages/channels 专用批量接口继续推进。
4. **P1-i `importContacts` 批量化** — ✅ 已完成。
5. **P1-l `getHistory` 翻页省略 COUNT** — ✅ 已完成 `NeedTotalCount` 默认 false。
6. **P2 `MaxContiguousPts` O(1) 化 + outbox 死任务清理** — ✅ 已完成 watermark；账号级 `user_update_events` 永久保留，retention worker 只清理 failed outbox。

**第二梯队 — 放开大规模群组 / 频道前必须拆雷（否则开闸即炸）**
7. **P0-1 广播频道去全员写扇出**（改懒算未读）— ✅ 已加同步写闸门，读取端已改为大群动态未读；仍需压测。
8. **P0-2 `OnlineUserIDs` 去全量排序**（改 `IsOnline` / 频道 viewer+member 在线索引）— ✅ 已完成主 fan-out 路径；Redis 跨实例在线关系暂缓。
9. **P0-3 + P1-g/h channel fan-out 与读 RPC 批量化、富集提到循环外、去重复解析。** — 部分完成：users 富集已批量化，模板化与 messages/channels 专用批量仍待做。
10. P1-j/k、P2 channel 各项。

**第三梯队 — 稳健性 / 尾延迟**
11. P1-b fan-out 满即丢 — ✅ 已完成 best-effort updates push；P3 注册表分片待真有重连风暴尾延迟再做。

---

## 附录 A：关键 P0 复核记录

下列 P0 在出报告前已逐条读码核实属实（非纯 agent 转述）：

| 发现 | 核实点 | 结论 |
|---|---|---|
| P0-1 | `channel.go:7958` 的 SQL `WHERE m.channel_id=$1 AND m.status='active'` 确无 LIMIT，在发送事务内 | ✅ 属实 |
| P0-2 | `session_manager.go:469-487`：`make(…, len(m.byUser))` → `range` 全 map → `sort.Slice` → `ids[:limit]`（排序在截断前） | ✅ 属实 |
| P0-3 | `channels.go:3053-3082` `for _, userID := range recipients { build(userID) }`；`:3143` `tgUsersForIDs` 循环内 `Users.ByID` | ✅ 属实 |
| P0-4 | `users/service.go:49-58` `ByID` 先 `s.Self(ctx, currentUserID)` 再 `s.users.ByID`（每次 ×2） | ✅ 属实 |
| P0-5 | `server.go:147` 字段、`:184` 建一个、`:202` 注入每个 Conn 共享；`outbound.go:432` 每条出站调 `c.msgID.New` | ✅ 属实 |

## 附录 B：复跑与验证

- **连接层**：`internal/mtprotoedge` 的 `BenchmarkSessionManager*` + `-mutexprofile`（既有 harness）。已新增 `BenchmarkSessionManagerOnlineCandidateFilter`，用于 20 万在线下候选过滤路径复跑。
- **私聊 / 消息**：`internal/loadtest` + 本机 docker PG/Redis（env-gated `TELESRV_TEST_POSTGRES_DSN` + `TELESRV_TEST_REDIS_ADDR`，未设则 Skip）。
- **PG 查询计划**：对 P1-l / P1-m / P0-1 用 `EXPLAIN (ANALYZE, BUFFERS)` 在有量级数据的分区上验证扫描行数与是否走索引。
- **channel fan-out**：在放开大规模群组前，构造「大成员群 + 高在线占比」场景压测主线 A 的端到端放大。
- **本轮回归**：`go test ./...` 已通过（2026-06-02）。2026-06-03 media/avatar 接手回归补充：`go test ./...`（PG/Redis env-gated）通过，`TestSeedMediaFromRealExport -count=1` 确认 sticker seed 74 reactions / 11 sets / 1355 docs / 2682 blobs，migration 状态 `59|f`；本轮新增头像编辑页 `messages.getEmojiProfilePhotoGroups` empty stub 属 TDesktop 兼容面补齐，不改变性能债优先级。双 TDesktop 群/频道头像 UI 回归仍待 Windows Computer Use 恢复后补实测证据。

---

## 附录 C：复核与落地轮（2026-06-02，第二轮）

对第一轮已落地的优化做正确性复核，并把剩余 P0/P1 改到位 + 加测试验证。

### 协议关键纠正：账号级 `differenceTooLong` 不可用（§2.5 参考审计救场）
原计划给账号级 `getDifference` 补 `differenceTooLong` 让落后客户端整库重置。审计 TDesktop 基线源码发现 **`api_updates.cpp:516` 对账号级 differenceTooLong 只打一行日志、不读 pts、且漏 `setRequesting(false)`**——收到后 `_ptsWaiter.requesting()` 永真，之后所有 `getDifference()` 在 `:689` 早退，**永久锁死整个 update 引擎，重连/新 session 都救不回**。参考实现 也都故意不发账号级 too-long。
→ 结论：账号级落后只能 `differenceSlice` 分批续传；**绝不能裁剪 `user_update_events` 到客户端够不着**——这直接决定了 P0 的修法（不删事件，而非加 too-long）。

### 本轮落地与验证
- **P0（retention）✅**：retention worker 改为**不再删 `user_update_events`**（pts log 永久保留，对齐参考实现），仅保留 outbox failed 清理。根除「裁剪事件→落后客户端静默丢消息」。
- **P1（watermark 死锁）✅**：先识破「预锁 watermark」会引入 watermark↔dialog 跨类型死锁，且 send 双向并发本就有 **dialog 行 pre-existing AB-BA**。终选**事务级 advisory lock 按 user_id 升序串行化同一对用户的写**（`lockUsersForUpdate`，独立锁空间、不与行锁交叉），一举消除 watermark+dialog+box 所有 AB-BA；覆盖 send/read/edit/delete/deleteHistory，delete 另配 dialog rebuild 升序。**强验证**：新增 `TestMessageStoreBidirectionalConcurrencyNoDeadlock`，临时禁用 advisory 时 116/320 操作 `deadlock detected (40P01)`（死锁点 `upsert dialog`），启用后 0 失败。
- **P1（tgUsersForIDs）✅**：批量 `ByIDs` 失败不再静默 `return nil`，记 Warn 日志（批量无部分结果，不降级逐个以免 DB 抖动放大）。
- **P1（outbox 拥塞）✅**：`pushOutboxUpdate` 区分 `retriable`——best-effort 队列拥塞保留 dispatching 行靠租约（30s）重投、**不计入 attempts 升级**；新增 `TestOutboxDispatcherDefersOnPushQueueFull`。

验证：`go build/vet ./...` 全绿；单测 16 ok / 0 FAIL；PG 集成**单独/分批**全绿（watermark / retention / 并发 / 双向死锁 / outbox 拥塞 / send-read-edit-delete 往返）。

### 发现：一批 pre-existing 集成测试失败（第一轮优化遗留，非本轮引入）
全量跑 `internal/store/postgres` 集成套件**整体不稳定**（stash 掉本轮改动的基线也大量失败，含无关的 `TestAuthKeyStoreRoundTrip`——根因是每测试建独立连接池不释放，全量连接耗尽）。逐个单独跑时，下列测试仍失败，**根因是第一轮优化改了行为但旧断言未同步**，已逐条核对非本轮 4 项修复引入：
- `TestMessageStoreDeleteHistoryBatchesHugeMaxID`：断言 `history.Count==2`，但 P1-l（`NeedTotalCount` 默认 false）使 history 返回 `Count=0`（删除断言本身通过）。
- `TestChannelStoreSendMessageFansOutDialogRows` / `...ReadOutboxDoesNotRegressSenderDialogUnread` / `...SendFailureRecordsNoopPtsGap`：channel fan-out / read outbox 行为随 P0-1 等 channel 优化变化，旧断言未更新（`channel.go` 本轮未改）。
→ **待办**：更新这些测试断言对齐新行为（count 语义 / channel fan-out），或确认行为符合预期；并考虑给集成测试套件改共享连接池以支持全量运行。属第一轮优化的测试债，独立于本轮性能修复。