# telesrv 持久化层设计

> 第一阶段（协议 + 登录 + 空账号主界面）的存储地基。后端已定：**PostgreSQL + Redis**，
> 依赖由 [`deploy/docker-compose.yml`](../deploy/docker-compose.yml) 启动；对象存储（MinIO）留第二阶段文件域。
> 决策见 README.md。

## 0. 定位与第一价值

把登录链路所需状态可靠落地，使 **server 可反复重启而不丢握手态 / 登录态**——这是真机联调能否愉快迭代的前提：

- **auth_key 丢** → TDesktop 缓存的 key 在 server 端查不到 → 客户端加密包解不开 → 触发重建密钥甚至重登。
- **登录态丢** → 每次重启都要重走 `sendCode` / `signIn`。

所以持久化第一价值是「联调可重启」，不是上规模。

## 1. 后端职责划分

| 维度 | PostgreSQL（权威、强一致、可查询） | Redis（高频、易失、TTL、原子计数） |
|---|---|---|
| 第一阶段 | `auth_keys`、`users`、`authorizations`、`account_passwords`、`temp_auth_key_bindings`、`app_configs`、`countries`、`country_codes`、`update_states`、`contacts`、`lang_packs`、`lang_pack_strings` | 验证码 `phone_code_hash → code`（短 TTL）、session、登录尝试限流 |
| 第二阶段 | `private_messages`、`message_boxes`、`dialogs`、`user_update_events`、`dispatch_outbox` | `pts` / owner `box_id` 原子自增（Redis miss 时从 PG durable log 恢复）、在线态缓存 |
| 不放这里 | 大对象/媒体（→ 二阶段 MinIO） | 任何需要持久强一致的业务事实 |

原则：**PG 存「事实」，Redis 存「态与计数」**。Redis 丢了能从 PG/重新计算恢复；PG 丢了就是数据丢失。

## 2. 第一阶段 schema（PostgreSQL）

DDL 见 [`deploy/migrations/0001_init.up.sql`](../deploy/migrations/0001_init.up.sql)、
[`deploy/migrations/0002_phase1_business.up.sql`](../deploy/migrations/0002_phase1_business.up.sql) 与
[`deploy/migrations/0003_startup_config_security.up.sql`](../deploy/migrations/0003_startup_config_security.up.sql)、
[`deploy/migrations/0004_temp_auth_key_binding_session.up.sql`](../deploy/migrations/0004_temp_auth_key_binding_session.up.sql) 与
[`deploy/migrations/0005_system_login_messages.up.sql`](../deploy/migrations/0005_system_login_messages.up.sql)、
[`deploy/migrations/0006_update_events.up.sql`](../deploy/migrations/0006_update_events.up.sql)、
[`deploy/migrations/0007_read_history_events.up.sql`](../deploy/migrations/0007_read_history_events.up.sql)、
[`deploy/migrations/0008_user_id_sequence_base.up.sql`](../deploy/migrations/0008_user_id_sequence_base.up.sql) 与
[`deploy/migrations/0009_private_message_pipeline.up.sql`](../deploy/migrations/0009_private_message_pipeline.up.sql)、
[`deploy/migrations/0010_message_performance_indexes.up.sql`](../deploy/migrations/0010_message_performance_indexes.up.sql) 与
[`deploy/migrations/0011_drop_dead_tables.up.sql`](../deploy/migrations/0011_drop_dead_tables.up.sql)、
[`deploy/migrations/0012_outbox_delete_on_deliver.up.sql`](../deploy/migrations/0012_outbox_delete_on_deliver.up.sql) 与
[`deploy/migrations/0013_contact_profiles_and_dialog_pins.up.sql`](../deploy/migrations/0013_contact_profiles_and_dialog_pins.up.sql)、
[`deploy/migrations/0014_settings_update_events.up.sql`](../deploy/migrations/0014_settings_update_events.up.sql)、
[`deploy/migrations/0015_update_event_payloads_and_outbox_auth.up.sql`](../deploy/migrations/0015_update_event_payloads_and_outbox_auth.up.sql)、
[`deploy/migrations/0016_delete_message_updates.up.sql`](../deploy/migrations/0016_delete_message_updates.up.sql)、
[`deploy/migrations/0017_dialog_folders.up.sql`](../deploy/migrations/0017_dialog_folders.up.sql)、
[`deploy/migrations/0018_user_search_indexes.up.sql`](../deploy/migrations/0018_user_search_indexes.up.sql)、
[`deploy/migrations/0019_usernames.up.sql`](../deploy/migrations/0019_usernames.up.sql) 与
[`deploy/migrations/0020_profile_message_state.up.sql`](../deploy/migrations/0020_profile_message_state.up.sql)。

> `0011` 清除了三张已被取代、运行时零引用的死表：`update_events`（一阶段 auth_key 级 update 队列，被 `user_update_events` 取代）、`messages_legacy` 与 `dialogs_legacy`（`0009` 重命名保留的迁移残骸，数据已迁入 `private_messages` / `message_boxes` / 新 `dialogs`）。`0006` / `0007` 作为历史演进记录保留，但其建立的 `update_events` 已不再使用。

第一批表：

- **`auth_keys`** —— 密钥交换产物。`auth_key_id`(BIGINT，SHA1 低 64 位小端 int64) + `body`(256B BYTEA) + `server_salt`。
- **`users`** —— 登录链路必须字段：`id` / `access_hash` / `phone`(UNIQUE) / `first_name` / `last_name` / `username` / `about` / `country_code` / `verified` / `support`。`access_hash` 为任何 `InputUser` 校验所必须，不可省；普通注册用户 ID 从 `1780243200`（2026-06-01 00:00:00 Asia/Shanghai 的 Unix 秒级时间戳）起递增，内置 777000 官方系统账号显式保留在低位区间。注册页只写手机号与姓名，后续 Settings/Profile 通过 `account.updateProfile` 更新姓名与 bio。
- **`authorizations`** —— `auth_key ↔ user` 绑定 + 设备信息（`layer` / `device_model` / `app_version` / `api_id` …）。PK 为 `auth_key_id`（一个 auth_key 一条授权），外键挂 `auth_keys` 与 `users`。
- **`account_passwords`** —— 账号 2FA/SRP 配置。第一阶段默认 `has_password=false`，但 `account.getPassword` 已走持久化查询。
- **`temp_auth_key_bindings`** —— `auth.bindTempAuthKey` 的 temp→perm 绑定记录。写入前校验 encrypted `bind_auth_key_inner`，持久化 `temp_session_id`，后续 temp auth_key RPC 在 router 入口解析为 perm auth_key 身份，并缓存在 active session 上。
- **`app_configs`** —— `help.getAppConfig` 的 data-backed JSON config，包含 TDesktop read mark、quote reply 与 native anti-spam 管理入口所需参数。
- **`countries` / `country_codes`** —— `help.getCountriesList` 的登录页国家区号目录。
- **`update_states`** —— `auth_key_id + user_id` 维度的设备状态快照。账号级 pts 以 `user_update_events` 为权威，设备退出/换号只清当前 auth_key 状态，不删除账号事件。
- **`user_update_events`** —— 按 `user_id` HASH 分区的账号级增量事件队列。承载 `new_message`、`read_history_inbox/read_history_outbox`（私聊与 channel peer）、`edit_message`、`delete_messages`、`contacts_reset`、dialog 置顶/顺序/manual unread/peer settings、dialog filter/order/reload、folder peers、channel 本地清空后的 `channel_available_messages`、当前账号 forum 展示模式 `channel_view_forum_as_messages` 与 allocator gap 的 `noop`，供 `updates.getDifference` 和 outbox worker 补偿错过的推送；设置类事件通过 `UpdateEventStore.AppendWithDispatch` 与 `dispatch_outbox` 同事务写入，并持久化 `event_peers` / `peer_settings` / `message_ids` / `dialog_filter` / `filter_order` / `folder_peers` 负载，避免状态只存在在线 push 中。
- **`contacts`** —— 当前账号通讯录关系与 owner 视角联系人资料。`contact_phone/contact_first_name/contact_last_name/note/note_entities` 均只属于 `(user_id, contact_user_id)`，同一个全局 user 在不同 owner 的通讯录中可以有不同姓名、电话和备注；`mutual` 由双方是否互存维护，删除一方联系人会清理对方 reverse mutual。
- **`private_messages`** —— 共享私聊消息主体，按 `sender_user_id` HASH 分区；`sender_user_id + random_id` 唯一保证 `messages.sendMessage/forwardMessages` 幂等；文本编辑更新共享 body/entities/edit_date；silent/noforwards/reply_to/fwd_from 元数据随消息持久化。
- **`message_boxes`** —— owner 视角消息盒，按 `owner_user_id` HASH 分区；每个账号看到自己的 `box_id`、peer、outgoing、pts、edit_date 与删除状态，历史/搜索走该表索引。删除只软删 owner 视角 message_box，`revoke` 通过 `(message_sender_id, private_message_id)` 定位其它 owner 视角并软删；编辑会同步所有可见 owner 视角盒子。该反向定位与分区键不一致，规划会展开全部 owner 分区，后续应增加 unpartitioned box 映射或先推导 owner_user_id 后再按 owner 分区点查。
- **`dialogs`** —— 当前账号会话摘要，按 `user_id` HASH 分区；只允许 `user` peer，支持 top message、置顶过滤、folder_id=0/1 主列表/归档与 offset 分页，并保存当前 owner 的 `pinned_order`、manual `unread_mark` 与 `hidden_peer_settings_bar`。列表查询 join `contacts` 时优先返回当前 owner 保存的联系人姓名/电话，避免不同账号看同一 peer 串备注；相关状态变化会写入账号级 durable update log，离线设备可通过 `updates.getDifference` 恢复。
- **`channels` / `channel_messages` / `channel_message_viewers` / `channel_update_events`** —— 超级群/频道单份消息模型，按 `channel_id` HASH 分区；`channels.pts` 是 channel-scoped durable log 水位，`channels.forum/forum_tabs` 持久化 megagroup topics 开关与 TDesktop tabs/list 布局，`channels.participants_hidden` 持久化隐藏成员设置并由 `ChannelFull.participants_hidden` 恢复 TDesktop UI，`channels.antispam` 持久化 native anti-spam 开关并由 `ChannelFull.antispam` 恢复管理入口，`channels.color_set/color/color_background_emoji_id/profile_color_set/profile_color/profile_color_background_emoji_id/emoji_status_document_id/emoji_status_until` 持久化频道外观并回填 `Channel.color/profile_color/emoji_status`，`channels.linked_chat_id` 维护 broadcast 与 discussion megagroup 的双向链接并走 `channels_linked_chat_idx` 反查，`channel_update_events(channel_id, pts)` 存 new/edit/delete/pin/noop 的恢复负载，成员权限/封禁变化不进入该 log；`channel_messages(channel_id, id)` 走 seek pagination 并保存 `views_count` 聚合列，`channel_message_viewers(channel_id,message_id,viewer_user_id)` 用主键完成 views 去重递增，`reply_to_msg_id/reply_to_top_id` 支撑 thread/comment 分页，`discussion_channel_id/discussion_message_id` 把 broadcast post 映射到 linked megagroup root，禁止按成员写扩散。
- **`channel_members` / `channel_dialogs` / `channel_unread_mentions`** —— 成员权限、读水位、owner 视角 channel dialog 与未读提及索引，分别按 `channel_id` / `user_id` / `user_id` HASH 分区；`available_min_id` 限制成员可见历史消息，`available_min_pts` 限制 `updates.getChannelDifference` 起点，避免新成员或重新加入成员恢复到入群前的消息类 durable 事件。`channel_dialogs.unread_count` 是小超级群普通未读缓存字段，不是 broadcast/大超级群真值；大频道读取 dialog/full channel 时按 `channel_members.read_inbox_max_id`、`available_min_id`、`channels.top_message_id` 与未删除消息动态派生普通未读。`channel_dialogs.default_send_as_peer_type/default_send_as_peer_id` 保存当前 owner 的默认发送身份，由 `channels.getFullChannel` 输出为 `channelFull.default_send_as`，不参与历史分页或 dialog 排序；`channel_dialogs.view_forum_as_messages` 是当前账号本地 forum 展示模式，由 `Dialog/ChannelFull.view_forum_as_messages` 恢复 UI，并通过账号级 durable update 同步多 session。`channel_unread_mentions(user_id,channel_id,message_id)` 不复制消息正文，发送时只写解析出的 active/可见/未读成员，清除后重算 `channel_dialogs.unread_mentions_count`。共同超级群查询已迁到 `user_channel_member_index(user_id, channel_id)`，排除 broadcast 与非 active/deleted 成员；后续 `channels.getLeftChannels`、joined/admined channel 列表和启动 dialog 聚合也必须从 user 维度 read model 或两步 channel_id 列表读取，不能直接用 `channel_members WHERE user_id=...` 反向扫 `channel_id` 分区。
- **`channel_invites` / `channel_invite_importers`** —— 邀请链接、导入者与 join request read model，均按 `channel_id` HASH 分区；invite 保存 `usage_count/requested_count`，importer 以 `(channel_id,user_id)` 保证同一用户只有一个 pending/approved 状态，管理页查询走 `admin/revoked/offset_link` 与 `requested/link/date/user_id` seek 索引，禁止按超大 limit 或 hash 反查做全表扫。public `channels.toggleJoinRequest` 使用 `channels.join_request` 与 `channel_invite_importers(invite_id=0, requested=true)` 表达非 invite-link pending request；管理员实时提醒用 bounded `updatePendingJoinRequests` + full channel 回填，不为每条 pending 状态生成无界 durable updates。
- **`dialog_filters` / `dialog_filter_settings`** —— 当前账号自定义 dialog filter、filter 顺序与 folder tags 开关，按 `user_id` HASH 分区；自定义 filter 从 ID 2 开始，归档只由 `dialogs.folder_id=1` 表达，避免一列同时承担归档状态和任意筛选规则。
- **`dispatch_outbox`** —— 按 `target_user_id` HASH 分区的 transactional outbox。发送事务内写入，RPC outbox worker 用 `FOR UPDATE SKIP LOCKED` 批量 claim，成功标记 delivered，失败退避重试；排除当前设备使用 `exclude_auth_key_id + exclude_session_id`，避免一个设备换号或多账号登录时误过滤。按 target 分区适合投递完成/失败按用户更新，但全局 claim/cleanup 与分区键不一致，规划会展开所有分区；上量前需引入 ready queue 或 worker shard read model。
- **`lang_packs` / `lang_pack_strings`** —— TDesktop 语言包元信息与字符串。开发 seed 来自 导出的 `.strings` 数据文件。

搜索相关索引：`0018` 启用 `pg_trgm`，为 `users.phone` 前缀、`users.username`/姓名、`contacts` owner 保存姓名和 `message_boxes.body` 建索引；`0049` 为公开 username channel/supergroup peer 搜索补 `channels.username/title` trgm 索引。`contacts.search` 先按当前 owner 视角区分联系人/非联系人，并补公开频道/超级群的 `PeerChannel + Chats`；`messages.searchGlobal` 当前只查当前账号私聊文本；跨群组/频道的大规模全局搜索后续应接全文索引或外部搜索服务。

## 3. store 接口蓝图

```text
internal/domain/
  user.go            # User（不依赖 tg.*）
  authorization.go   # Authorization（含设备信息）

internal/store/
  authkey.go         # AuthKeyStore        —— PG（去掉现有 AuthKeyData.UserID）
  session.go         # SessionStore        —— PG / Redis
  user.go            # UserStore           —— PG：ByID / ByPhone / Search / Create / Update
  authorization.go   # AuthorizationStore  —— PG：Bind / ByAuthKey / ByUser / Delete
  code.go            # CodeStore           —— Redis：Set(TTL) / Get / Del
  updatestate.go     # UpdateStateStore    —— PG：auth_key+user 维度 pts/qts/seq
  update_event.go    # UpdateEventStore    —— PG：user 维度 getDifference 事件
  dispatch_outbox.go # DispatchOutboxStore —— PG：在线 update transactional outbox
  contact.go         # ContactStore        —— PG：当前账号通讯录
  dialog.go          # DialogStore         —— PG：当前账号会话摘要
  message.go         # MessageStore        —— PG：账号视角下的私聊消息
  langpack.go        # LangPackStore       —— PG：TDesktop 语言包
  account.go         # PasswordStore       —— PG：账号 2FA/SRP 配置
  help.go            # AppConfig/Country   —— PG：启动配置与国家区号目录
  temp_auth_key.go   # TempAuthKeyBinding  —— PG：temp→perm auth key 绑定
  memory/            # 现有内存实现（保留作测试替身）
  postgres/          # pgxpool 实现 + 迁移 runner
  redisstore/        # go-redis 实现
```

## 4. 关键设计决策

1. **auth_key 与 authorization 分表**：auth_key 是协议产物、授权是业务产物（README.md §3）。从现有 [`AuthKeyData`](../internal/store/authkey.go:11) 拆出 `UserID`，新增 `domain.Authorization` + `AuthorizationStore`。授权表同时是 [`rpc.ClientInfo`](../internal/rpc/context.go) 的落库归宿。
2. **store 接口收发 domain 实体**，DTO 仅存在于实现内部；`AuthKeyData` / `SessionData` 这类纯协议数据保留为 store 层 DTO（它们不属业务 domain）。domain 不依赖 `tg.*`，下沉到接口签名安全。
3. **`auth_key_id` 存 BIGINT**：内部 `[8]byte` 在 store 边界按小端转 int64（MTProto auth_key_id 定义即 SHA1 低 64 位）。可读、可索引、与日志一致。
4. **update 状态分两层**：`user_update_events` 是账号级 durable log；`update_states(auth_key_id,user_id)` 是设备在某账号下看到的状态。这样同一设备退出后换号、或一个设备先后登录多个账号，都不会把旧账号差分串到新账号。
5. **连接身份缓存**：`authorizations` 仍是权威事实；router 在 active session 首次 RPC 时解析 temp→perm auth_key，并把业务 auth_key_id 写回连接上下文；`auth_key→user_id` 额外做 router 级业务 auth_key 缓存并用 singleflight 合并并发 miss，避免 TDesktop 启动时多个 temp session 同时打 `authorizations`。登录前的未授权结果也做缓存；`auth.bindTempAuthKey` 切换业务 auth_key 时清掉旧 raw key 的 user 缓存，`auth.signIn/signUp` 写入真实 user，`auth.logOut` 清理同业务 auth_key 的活跃连接身份与 auth_key+user update state，账号级事件不随设备退出删除。
6. **大表查询必须游标化**：`message_boxes`、`dialogs`、`user_update_events` 和 `dispatch_outbox` 从建表开始 HASH 分区；历史和会话列表使用 `box_id/date` 或 `top_message_date/top_message_id/peer_id` seek pagination，避免在 owner 分区内做大 SQL `OFFSET` 扫描。
7. **查询入口必须匹配分区键**：分区表上的二级索引不能替代分区裁剪。user→channel、message→owners、global queue claim 这类反向访问必须有独立 read model，或拆成「先取 bounded id，再按分区键批量点查」两段；禁止把动态 join 当作分区裁剪。
8. **联系人和 dialog 均是 owner 视角**：`users` 只保存全局账号资料；通讯录姓名/电话/备注、dialog 置顶顺序、manual unread、隐藏 action bar 都必须落在当前 owner 维度。后续群消息、群成员备注、会话排序可以复用这条边界，不能把个人备注写回全局 user。
9. **搜索不能无界扫大表**：用户搜索限制 query/limit，并用 phone prefix / username / owner 保存姓名索引；消息全局搜索第一版只在当前 owner 分区内执行，并有 `pg_trgm` 兜底。参考实现，未接 Meilisearch/FTS 前不做跨全库模糊扫。

## 5. 选型

| 组件 | 选型 | 理由 |
|---|---|---|
| PG 驱动/连接池 | `jackc/pgx/v5` + `pgxpool` | Go 生态主流，性能好，原生 PG 协议，不引 ORM |
| Redis 客户端 | `redis/go-redis/v9` | 事实标准 |
| 迁移 | `golang-migrate/migrate/v4`，`iofs` 嵌入 `deploy/migrations` | 版本化、可 up/down，启动自动 `up` |
| 查询层 | `sqlc` 生成 pgx/v5 代码（以 `deploy/migrations` 为 schema 源，与 golang-migrate 共用同一份 SQL） | 类型安全、消除 `rows.Scan` 样板、schema 演进编译期暴露；动态查询用 pgx 手写补充 |

## 6. 落地里程碑

- **P0 基础设施**（本文档 + docker-compose + 0001 schema + config 扩展）—— ✅
- **P1 Go 接入**：`store/postgres`（pgxpool + golang-migrate runner + sqlc `AuthKeyStore`）+ `store/redisstore`（`SessionStore`）；main 启动迁移并注入。连接层测试保持 Memory 替身（测协议不被 DB 绑架），PG/Redis 由 env-gate 往返集成测试验证。—— ✅
- **P2 业务 store**：`UserStore` / `AuthorizationStore`（PG）+ `CodeStore`（Redis）；Memory 实现抽到 `store/memory` 子包（三后端对称）。登录注册闭环已用上（auth.* + users.getUsers + updates.getState），手机号在 auth 业务层规范为纯数字以匹配 TDesktop 验证码页行为。—— ✅
- **P3 启动业务 RPC 持久化**：`UpdateStateStore` / `ContactStore` / `DialogStore` / `LangPackStore`，
  `updates.getDifference`、`contacts.getContacts`、`messages.getDialogs/getPinnedDialogs`、`langpack.*`
  已由 PG-backed 服务响应；空账号仍返回空业务数据。—— ✅
- **P3.5 启动配置与账号安全持久化**：`PasswordStore` / `AppConfigStore` / `CountryStore` /
  `TempAuthKeyBindingStore`，`account.getPassword`、`help.getAppConfig/getCountriesList`、
  `auth.bindTempAuthKey` 已由 PG-backed 服务校验并响应，temp auth_key 会映射到 perm auth_key 授权。—— ✅
- **P3.6 官方系统账号与登录消息**：内置 777000 官方账号，`auth.signIn/signUp` 成功后写入登录消息，
  `messages.getDialogs/getHistory/search/readHistory` 返回或更新账号视角下的官方会话，并支持 TDesktop
  的 offset/hash 参数；`messages.getDialogs` 的 count/hash 按企业版同样的“完整列表先统计、再分页”语义计算；
  `messages.getPeerDialogs` 按企业版同样的指定 peer 查询路径返回 dialog/top message/users/state，缺失 user peer 返回空 dialog 占位；RPC 层延迟向当前
  session 推送 `updateNewMessage`，并向其它在线 session 推送新登录 `updateServiceNotification`；
  在线 session 需先完成 `updates.getState/getDifference` 才接收主动 updates，未 ready 的推送先暂存；
  同时写入账号级 `user_update_events`，`updates.getDifference` 可补偿登录官方消息与官方会话已读事件。—— ✅
- **P4 消息闭环**：私聊文本 `messages.sendMessage/forwardMessages/readHistory/editMessage/deleteMessages/deleteHistory`、reply_to/forward header 元数据、双端 `message_boxes`、已读 inbox/outbox 回执、`user_update_events`、Redis `pts/box_id` allocator、transactional outbox、在线 session 批量推送、窗口限流、链路指标和游标分页已落地；媒体、群组/频道、完整 qts/seq 仍是后续阶段。
- **P4.1 联系人与 dialog owner 视角**：`contacts.addContact/importContacts/deleteContacts/updateContactNote/getContactIDs/getStatuses` 与 `messages.toggleDialogPin/reorderPinnedDialogs/markDialogUnread/getDialogUnreadMarks/hidePeerSettingsBar/getPeerSettings` 已接入 PG-backed 服务；集成测试覆盖不同 owner 对同一 user 的独立备注、备注 hash、reverse mutual、dialog 用户视角、置顶重排、manual unread 与隐藏 action bar。—— ✅
- **P4.2 设置类 durable updates**：联系人 reset、dialog pinned、pinned order、manual unread、peer settings、dialog filters/order/reload 与 folder peers 均写入 `user_update_events`，并同事务写 `dispatch_outbox`；`updates.getDifference` 与 outbox TL 转换支持这些事件，且保存 pinned order / peer settings flags / dialog filter payload / folder peers / exclude auth key，在线 push 不再是唯一通知路径，也不会与 reliable outbox 双重推送。—— ✅
- **P4.3 删除消息/清空历史**：`messages.deleteMessages/deleteHistory` 已支持 owner 视角软删除、`revoke` 对端清理、dialog top 重算/删除/`just_clear` 保留空 dialog、`updateDeleteMessages` durable payload（`message_ids` + `pts_count=len(message_ids)`）与 outbox 投递；后续新消息会正常重建被删除的 dialog。—— ✅
- **P4.4 Dialog 分组/归档**：`messages.getDialogFilters/updateDialogFilter/updateDialogFiltersOrder/toggleDialogFilterTags` 与 `folders.editPeerFolders` 已接入 PG-backed 服务；集成测试覆盖 folder_id 0/1 主列表/归档、自定义 filter、tags 和归档还原。—— ✅
- **P4.5 TDesktop 搜索入口**：`contacts.search` 已支持联系人/非联系人用户搜索，`contacts.getSponsoredPeers` 返回空 sponsored peers，`messages.searchGlobal` 接当前 owner 私聊文本搜索；查询保护与索引设计参考实现，避免客户端搜索框卡在 Loading。—— ✅

## 7. 与铁律的关系

- §2 类型边界：`store` 与 `domain` 均不依赖 `tg.*`，仅在 RPC 边界转换。
- §3 协议/业务隔离：`auth_keys`（协议）与 `authorizations`（业务）分表落地。
- §6 范围：第一阶段只实现登录 + 空账号主界面必经 RPC，以及登录必需的官方系统消息；二阶段已补私聊文本闭环，群组/频道/媒体仍留后续阶段。

## 8. 媒体层（documents / photos / blobs，2026-06-02）

媒体闭环新增表（migration `0057_media`）：

- `upload_parts`（owner+file_id+part）：`upload.saveFilePart/saveBigFilePart` 累积的分片，组装成 blob 后清理（transient）。
- `file_blobs`（PK `location_key`）：可下载二进制的索引，`doc:<id>[:type]` / `photo:<id>:<type>` → `backend/object_key/size/mime`；字节本身落 blob backend。
- `documents` / `photos`：Telegram 文档 / 照片元数据（id、access_hash、file_reference、dc_id、attributes/thumbs/sizes JSONB）。
- `documents.id` 持久化为 telesrv-owned 正数 id；外部导出资源的 source id 只在 seed 扫描文件名/JSON 时使用，进入 `documents` / `file_blobs` / `sticker_sets` / `available_reactions` 前已归一为服务端 id。RPC、`InputDocument` 与 `inputDocumentFileLocation` 均直接使用该 id。
- `sticker_sets`：贴纸 / 自定义 emoji 集（含有序 `document_ids`、`packs`、`system_key` 路由系统集）。
- `available_reactions`：reaction 目录（引用真实文档 id）。
- `profile_photos`（owner_peer_type+id+photo_id）：用户/频道头像历史，current=active 中 sort_order 最大者。
- 消息表（`private_messages`/`message_boxes`/`channel_messages`）新增 `media` JSONB 快照列；放宽 body 非空 CHECK 为「body 非空 OR media 非空（OR channel action）」，支持「仅媒体」消息。
- 头像反范式：`users.photo_*`（migration 0057 用户表无需改，富化在 users 服务）与 `channels.photo_*`（migration `0059`）。
- `app_configs` 增 `reactions_default` 等（migration `0058`）。

blob backend：`internal/app/files`（`BlobBackend` 接口 + `LocalFS` 本地磁盘实现，内容寻址 sha256 两级 fanout 去重，默认 `data/blobs`）。`MediaStore`（PG）只管元数据 + `file_blobs` 索引；字节由 backend 按 `object_key` 读写。

种子导入：启动时 `files.Service.SeedMedia` 从 `TELESRV_STICKER_SEED_DIR`（真实 Telegram 导出，含 `available_reactions_raw.json` + 各集 `set_info.json` + `.tgs/.webp/缩略图`）幂等导入：JSON 元数据→表，二进制→blob，`dc_id` 重写为本 server DC。实测 74 reactions / 24 sets / ~1.5k documents / ~3k blob 索引 / ~2.8k 去重磁盘文件。

类型边界：`domain.Document/Photo/MessageMedia/StickerSet/AvailableReaction` 带 json tag（store 直接 marshal JSONB），完全不依赖 `tg.*`；domain↔tg 转换集中在 `internal/rpc/convert_media.go`。
