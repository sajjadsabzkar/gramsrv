# Supergroup / Channel Module Design

Date: 2026-06-01

## Scope

本模块实现 Telegram Desktop 第一兼容目标下的超级群与频道闭环。项目不保留 legacy 普通群长期形态：`messages.createChat` 在服务端直接创建 `megagroup`，TDesktop 同步创建响应仅暴露带 `migrated_to` 的 legacy `chat` 外观，后续主路径回到 `channel` / `channelFull` 语义，避免服务端维护真正的 `migrateToMegagroup` 升级链路。

首批真实实现：

- `messages.createChat` -> create megagroup
- `channels.createChannel`
- `channels.getFullChannel`
- `channels.getParticipants`
- `channels.getParticipant`
- `channels.checkUsername/updateUsername/getAdminedPublicChannels`
- `channels.toggleSignatures`
- `channels.inviteToChannel`
- `channels.joinChannel`
- `channels.leaveChannel`
- `channels.editAdmin/editBanned/editTitle/deleteChannel`
- `channels.getAdminLog`
- `messages.updatePinnedMessage`
- `messages.editChatDefaultBannedRights`
- `messages.exportChatInvite/checkChatInvite/importChatInvite`
- `messages.getMessageReadParticipants`
- `messages.getMessageEditData`
- `updates.getChannelDifference`
- `messages.sendMessage/getHistory/readHistory/editMessage/deleteMessages/deleteHistory/forwardMessages/setTyping/getReplies/getDiscussionMessage/readDiscussion` 的 channel peer 分支
- `messages.getDialogs/getPeerDialogs/getPinnedDialogs` 展示 channel / megagroup dialog

首批兼容 stub：

- `channels.convertToGigagroup`：当前 megagroup 已是目标形态，返回 ok 或 `CHAT_NOT_MODIFIED` 语义。
- `messages.migrateChat`：不创建 legacy chat，不执行双写迁移；校验 change_info/creator 权限后把 `chat_id` 映射到既有 megagroup `channel_id`，返回 `updateChannel + tg.Channel`，满足 TDesktop `applyUpdates -> migrateTo()` 路径。
- `channels.editPhoto`：files/media 头像存储未接入前只允许 `inputChatPhotoEmpty` 删除头像 no-op 兼容；真实上传/已有照片返回 `PHOTO_INVALID`，避免 TDesktop 误以为头像已成功更新。
- `channels.togglePreHistoryHidden/toggleSlowMode/toggleForum/toggleAntiSpam/updateColor/updateEmojiStatus`：持久化 channel 设置并返回 `updateChannel`；TDesktop 通过 `ChannelFull.hidden_prehistory/slowmode_seconds/antispam`、`Channel.slowmode_enabled`、`Channel.forum/forum_tabs`、`Channel.color/profile_color/emoji_status` 恢复 UI。emoji status 当前支持 empty 与普通 document id，collectible gift 状态等 gift/read model 后续补；forum 当前保存开关/layout 与 admin log，`messages.getForumTopics*` 会在 forum 开启后返回虚拟 `General` topic(id=1) 加持久化 topic store；`messages.create/edit/pin/reorder/deleteTopicHistory` 已接入最小真实 topic mutation；anti-spam 当前只保存开关与 admin log，不接入真实垃圾消息删除管线。
- `channels.setStickers`：files/sticker store 未接入前只允许 megagroup `inputStickerSetEmpty` 清空 no-op 兼容；非空 sticker set 返回 `STICKERSET_INVALID`，避免 TDesktop 误以为群贴纸集已持久化。`channels.reorderUsernames/toggleUsername/deactivateAllUsernames` 仍先做权限校验型兼容响应；Fragment/多 username 入口不改主 username，主 username 只允许 `channels.updateUsername` 设置或清除。
- TDesktop 已有调用证据但非首批真实业务的入口统一注册为显式 stub：`channels.reportSpam/editLocation/convertToGigagroup/reportAntiSpamFalsePositive/setBoostsToUnblockRestrictions/setEmojiStickers/checkSearchPostsFlood/setMainProfileTab`。`channels.searchPosts` 已提升为公开频道/超级群帖子真实搜索，`checkSearchPostsFlood` 仍保持免费额度兼容 stub；二者共享 query 长度边界。`setBoostsToUnblockRestrictions` 按 Layer225 限制 0..8。`channels.restrictSponsoredMessages/updatePaidMessagesPrice/toggleAutotranslation` 已提升为最小真实设置持久化：分别回填 `ChannelFull.restricted_sponsored`、`Channel/ChannelFull.send_paid_messages_stars` + broadcast `Channel.broadcast_messages_allowed`、`Channel.autotranslation`，其中 `updatePaidMessagesPrice` 按 TDesktop 默认 app config 限制 stars<=10000，并允许 broadcast direct messages 用 `-1` 表示关闭；真实广告投放、boost/premium 校验、paid messages/monoforum/结算后续补。`channels.exportMessageLink`、`channels.readMessageContents`、`channels.getMessageAuthor`、`channels.deleteParticipantHistory`、`getInactiveChannels`、`getChannelRecommendations`、`toggleJoinToSend`、`toggleJoinRequest`、`toggleParticipantsHidden`、`toggleForum`、`toggleViewForumAsMessages`、`toggleAntiSpam`、`getLeftChannels`、`getGroupsForDiscussion`、`setDiscussionGroup` 已从该列表提升为真实实现。
- `channels.getGroupsForDiscussion/setDiscussionGroup` 真实维护 broadcast channel 与 megagroup 的双向 `linked_chat_id`。候选列表只返回当前用户可管理的 supergroup，不返回 legacy basic group；设置时校验 access_hash、broadcast/group 类型、管理权限与 hidden prehistory，替换链接会同步清理旧 group/old broadcast，TDesktop 通过 `Channel.has_link` 和 `ChannelFull.linked_chat_id` 刷新讨论组入口。linked broadcast 发新 post 时会在 discussion megagroup 创建一条 forwarded root message，source post 保存 `discussion_channel_id/message_id`，`messages.getDiscussionMessage/getReplies/readDiscussion` 都映射到该 root 和 target channel read state。
- TDesktop 已有源码调用但当前业务模型尚未维护独立 media/sticker/custom-emoji/saved-message tag assignment/poll/todo/scheduled 的 `messages.reportSpam/report/reportReaction/reportMessagesDelivery/reportReadMetrics/reportMusicListen/reportSponsoredMessage/getSavedReactionTags/updateSavedReactionTag/getDefaultTagReactions/getExtendedMedia/getAttachedStickers/getCustomEmojiDocuments/searchStickerSets/searchStickers/getEmojiKeywords/getEmojiKeywordsDifference/sendVote/getPollResults/getPollVotes/addPollAnswer/deletePollAnswer/getUnreadPollVotes/readPollVotes/appendTodoList/toggleTodoCompleted/getSearchCounters/getSearchResultsCalendar/getSearchResultsPositions/getOnlines/getWebPagePreview/uploadMedia/sendMedia/sendMultiMedia/getScheduledHistory/getScheduledMessages/sendScheduledMessages/deleteScheduledMessages` 均显式注册；统一做参数上限、peer 校验和空/零兼容响应，禁止落到未知 RPC。其中 `messages.readMessageContents` 已提升为 private real-content-read：清理 owner 侧 `message_boxes.media_unread/reaction_unread`，有变化才生成 durable `updateReadMessagesContents`；channel/supergroup 的 `channels.readMessageContents` 会按可见消息 ID 清理当前作者视角的 unread reaction、重算 `channel_dialogs.unread_reactions_count`，并向当前账号 session 推 `updateMessageReactions` 让 TDesktop 立即去掉 dialog reaction 角标；`messages.getMessagesViews` 已维护 channel-scoped views 去重计数并返回 replies/comment 统计；`messages.getUnreadMentions/readMentions` 已维护 channel-scoped unread mention index，history/getMessages/difference/online update 按 viewer 回填 `mentioned/media_unread`，readMentions 返回 channel pts；`messages.sendReaction/getMessagesReactions/getMessageReactionsList/getUnreadReactions/readReactions/getRecentReactions/clearRecentReactions/getTopReactions` 已提升为 private + channel/supergroup emoji reaction 最小真实实现：private 写 `private_message_reactions` 并按双端 owner-visible message box 返回/推送 `updateMessageReactions`，channel 写 `channel_message_reactions` 并按消息作者维护 unread reaction 状态，history/getMessages/search 均回填 `message.reactions`，`add_to_recent` 写账号级最近 channel reaction 列表且 get/clear 支持 hash/notModified，`getTopReactions` 按账号使用次数排序并优先用真实 `available_reactions` catalog 兜底；`messages.getSavedReactionTags/updateSavedReactionTag` 已持久化账号级 emoji saved reaction tag 标题并向其它 session 推 `updateSavedReactionTags`，但 peer 维度 count 与 saved-message tag assignment 仍返回空；`messages.getForumTopics/getForumTopicsByID/createForumTopic/editForumTopic/updatePinnedForumTopic/reorderPinnedForumTopics/deleteTopicHistory` 已从纯空响应提升为真实最小 topic store：虚拟 General + root service message topic + bounded delete；`messages.getCommonChats` 已提升为真实共同超级群查询并回填 `users.getFullUser.common_chats_count`；`messages.report` 保持 TDesktop 分步举报 UI 所需的 choose option/add comment/reported 形态但暂不落库；report/metrics/music/sponsored 这类 telemetry 入口只返回 BoolTrue/reported，不写业务状态；TDesktop 资料页 shared media count 会用 `messages.search(limit=0, filter=photo/video/document/url/gif/music/roundVoice/poll)` 取 `messages.channelMessages.count`，当前显式返回空页/count=0，避免纯文本 channel history 污染 photos/videos/files 等计数；search calendar 空结果会回填请求 offset date/id，避免空月份重复拉取；default tag、sticker/custom emoji 与 extended media 均只返回空/notModified 或明确错误，不伪造 paid media 或 tag 绑定状态；poll/todo mutating 入口在缺少媒体 store 时返回可解释错误，不伪造 `updateMessagePoll` 或 todo service message；web preview 返回 `messageMediaEmpty` 且不会抓外网，`sendMedia(inputMediaWebPage)` 降级为纯文本发送，真实 photo/document/poll/album、todo、scheduled store 留待后续模型。
- legacy chat 管理入口 `messages.getChats/getFullChat/addChatUser/deleteChatUser/editChatTitle/editChatPhoto/editChatAdmin/editChatAbout/editChatDefaultBannedRights/editChatParticipantRank` 统一映射到 megagroup/channel 语义；其中 `about` 与 default banned rights 真实持久化，default banned rights 参与普通成员发送和邀请权限校验。`editChatCreator` 已显式注册并校验 peer/user，但账号 2FA/SRP 与所有权转移事务未接入前返回可解释的密码错误，不进入 fallback。

## Reference Audit

### TDesktop

TDesktop 创建群入口仍会调用 `MTPmessages_CreateChat`，创建频道/超级群调用 `MTPchannels_CreateChannel` 并根据 UI 类型设置 `f_megagroup` 或 `f_broadcast`。`messages.createChat` 的 `ChatCreateDone` 会从 `messages.InvitedUsers.updates.chats` 取第一条 `chat`，不接受 `channel`；因此 TDesktop ctx 下同步响应必须把 `legacy chat(migrated_to=inputChannel)` 放在第一项，同时继续附带真实 `channel.id/access_hash/title/broadcast/megagroup/participants_count/date` 供 `updateNewChannelMessage` 和迁移后的 supergroup 历史使用。服务端不持久化 legacy chat，后续 `InputPeerChat` 主路径统一映射回同 id channel。

频道更新走 `ChannelData` 的独立 `PtsWaiter`。`updateNewChannelMessage` 若找不到频道资料，会延迟触发 `getDifference`；找到频道且不在处理 channel difference 时，会调用 `channel->ptsUpdateAndApply(pts, pts_count, update)`。`PtsWaiter` 依赖 `pts_count` 累加判断连续性，缺口会缓存乱序 update，1 秒后拉 `updates.getChannelDifference`。因此服务端必须保证 channel pts 单调、`pts_count` 准确、每个已分配 pts 都能从 channel durable log 补到。

管理入口会直接触发 `MTPchannels_EditAdmin`、`MTPchannels_EditBanned`、`MTPchannels_EditTitle`、`MTPchannels_DeleteChannel`；pin 使用 `MTPmessages_UpdatePinnedMessage`，邀请链接使用 `MTPmessages_ExportChatInvite/CheckChatInvite/ImportChatInvite`。TDesktop 对这些 RPC 返回的 `Updates` 会立即 apply，因此响应必须至少带 `updateChannel`、成员变更时带 `updateChannelParticipant`，pin 时带 `updatePinnedChannelMessages`，并把相关 `chats/users` 填齐。

TDesktop 源码还会在频道/群 UI、导出和搜索路径触发 `MTPmessages_GetMessagesViews`、`ReadMessageContents`、`GetUnreadMentions/ReadMentions`、`GetSearchCounters`、`GetReplies`、`GetForumTopics/GetForumTopicsByID` 和 legacy basic group wrappers。参考实现 对 `messages.getOnlines` 返回固定 1，参考实现 handler 仍未实现；telesrv 比 参考实现 多走一步：使用在线 session 快照与 active channel member 有界交集返回实时在线数，缺少在线 provider 时才退回兼容 1。basic group 管理入口再下沉到 megagroup/channel 语义；参考实现 对 default banned rights 要求 `ban_users` 权限并发布频道设置命令。telesrv 借鉴这个边界：views/mentions 已有真实最小实现，forum topic create/read/edit/pin/delete/reorder 已有持久化最小实现，复杂媒体仍返回显式 stub，legacy basic group 管理统一落到 megagroup/channel 语义，default banned rights 真实落库并影响普通成员 send/invite。

管理员日志页调用 `MTPchannels_GetAdminLog(channel, q, events_filter, admins, max_id, min_id, limit)`，TDesktop 既会用 `max_id` 向旧事件翻页，也会用 `min_id`/polling 拉新事件；响应必须是 `channels.adminLogResults{events,chats,users}`，并让 action 里的 message/participant 能被客户端渲染。服务端必须 cap `limit/admins/query`，按 `(channel_id,id)` seek，不能按客户端传入的大 id 构造数组。

已读详情浮层在 outgoing 且本地已读、成员数小于 `chat_read_mark_size_threshold`、消息未超过 `chat_read_mark_expire_period` 时调用 `MTPmessages_GetMessageReadParticipants(peer,msg_id)`。TDesktop 只接收 `Vector<readParticipantDate>`，不附带 users/chats；因此服务端必须只返回当前客户端已经能解析的 user id，并保证接口失败不会变成持续 `NOT_IMPLEMENTED` 噪声。

reply/forward 渲染依赖 `MessageReplyHeader` 与 `MessageFwdHeader` 内引用的 peer 已可解析。TDesktop 的 `ReplyFieldsFromMTP` 会优先用 `reply_to_top_id`，没有时退回 `reply_to_msg_id`；`api_updates.cpp` 的 `ForwardedInfoDataIsLoaded/ReplyDataIsLoaded` 会在 forward/reply header 引用的 peer 未加载时暂缓 apply update。因此服务端发送 channel reply 时必须用已存在、当前成员可见的 channel message 反算 `reply_to_top_id`，不能信任客户端传入；forward/reply header 如果引用 user/channel peer，当前 RPC 响应、账号级 `updates.getDifference`、在线 outbox 和 `updates.getChannelDifference` 都要补齐对应 users/chats 上下文，不能只修实时响应路径。

会话内搜索也走 channel peer：TDesktop 的 `api_messages_search.cpp` 与 `dialogs_widget.cpp` 在群/频道里直接发 `messages.search(peer=inputPeerChannel, q, offset_id, limit)`，并把 `offset_id` 当 seek cursor 继续翻页。参考实现 对 channel peer 把 `OwnerPeerId` 切到 channel id，从单份 message read model 查询，并补齐 sender/forward/reply 相关 users/channels。telesrv 因此不把 channel 搜索转成私聊 `message_boxes`，而是在 `channel_messages(channel_id,id)` 上做有界文本搜索；PG 使用 `channel_messages_body_trgm_idx` 辅助 `ILIKE` 命中，仍按 `id DESC LIMIT` 分页。

公开 username 管理入口会调用 `MTPchannels_CheckUsername`、`MTPchannels_UpdateUsername` 与 `MTPchannels_GetAdminedPublicChannels`；settings/profile 管理区还会触发 signatures、prehistory、slowmode、stickers、color、emoji status、Fragment usernames 等 RPC。主 username 是客户端可见资料，必须真实落库；Fragment/多 username、颜色、贴纸、emoji status 可先权限校验型 stub，但不能误修改主 username 或静默吞掉未知 RPC。

TDesktop 左侧搜索框通过 `MTPcontacts_Search` 查 peer，返回的 channel/supergroup 命中必须同时出现在 `results/my_results` 的 `peerChannel` 和 `chats` 向量中，否则 UI 无法 materialize peer；用户名跳转通过 `MTPcontacts_ResolveUsername`，公开 channel/supergroup 同样返回 `peerChannel + chats`。telesrv 只暴露带主 username 且未删除的公开频道/超级群，PG 使用 `channels_public_username_trgm_idx` / `channels_public_title_trgm_idx` 避免无索引模糊扫。非成员从公开搜索结果打开频道时，`channels.getFullChannel`、`messages.getPeerDialogs(InputPeerChannel)` 与 `messages.getHistory(InputPeerChannel)` 使用只读公开预览视图返回 full/dialog/history；私有频道、ban/kick/view_messages 禁止仍拒绝，发送、差分、管理等写路径仍要求 active member。

源码检索还显示导出/管理/搜索路径会触发 `MTPchannels_ExportMessageLink`、`ReadMessageContents`、`DeleteParticipantHistory`、`GetGroupsForDiscussion`、`SetDiscussionGroup`、`ToggleForum`、`ToggleJoin*`、`SearchPosts`、`CheckSearchPostsFlood`、`GetChannelRecommendations`、`GetMessageAuthor` 等 Layer 225 方法。`ToggleForum` 保存 `enabled/tabs`，仅 creator 可改，返回 `updateChannel`；`GetChannelRecommendations` 先按公开 username broadcast channel 做最小真实推荐，真实相似度/订阅画像/Premium 扩容后续补；其它未接入完整业务的入口必须显式注册、加参数上限、返回可解释 stub，并写入 compatibility matrix。

讨论组设置入口在 `edit_peer_info_box.cpp` 中：broadcast 侧无当前链接时会调用 `channels.getGroupsForDiscussion` 拉候选；保存时若选中的 megagroup 开启了 hidden prehistory，TDesktop 会先调用 `channels.togglePreHistoryHidden(group,false)` 再重试 `channels.setDiscussionGroup`。保存成功后客户端本地调用 `ChannelData::setDiscussionLink`，同时 `ChannelFull.linked_chat_id` 会在后续 full channel 刷新时恢复状态。因此 server 必须返回候选 chats，设置/解绑成功后更新双方 `linked_chat_id` 并推 `updateChannel`，不能只返回 BoolTrue。

TDesktop 导出路径的 `channels.getLeftChannels` 使用 count-offset 分页：客户端每次把 offset 增加已返回 chats 数量，`messages.chats` 立即结束，`messages.chatsSlice` 只有空 chats 才结束。telesrv 因此按 `channel_members(user_id,status='left')` 查询用户已离开的频道/超级群，pageSize 固定 100，offset 上限 10000；最终非空页返回 `messages.chats`，offset 已越过总数时返回空 `messages.chatsSlice{count}`，避免导出流程多拉或循环。

输入框与高级会话路径还会触发 `MTPmessages_GetWebPagePreview`、`UploadMedia`、`SendMedia`、`SendMultiMedia`，定时消息入口会触发 `GetScheduledMessages/SendScheduledMessages/DeleteScheduledMessages`，forum UI 会触发 topic create/edit/pin/reorder/delete。参考实现 对 web preview 空文本报错、无预览返回 `messageMediaEmpty`，media 和 scheduled 都有独立持久化；参考实现 的 web preview 是空响应，media 走 helper，scheduled/forum 多数仍未实现。telesrv 当前不伪造这些缺失模型：web preview 返回空，webpage media 可降级纯文本；真实 media/scheduled/forum 只做有界校验与可解释错误或 cleanup update。

投票和 todo 是 channel/supergroup 消息媒体的后续模型，不应在缺少 store 时假成功。TDesktop 的 `api_polls.cpp` 会在 `sendVote/addPollAnswer/deletePollAnswer/getPollResults` 成功后 apply updates，投票人列表通过 `messages.getPollVotes` 的 `next_offset` 翻页；`api_unread_things.cpp` 和 `menu_send.cpp` 还会拉取/清除未读投票，todo 则在 `api_todo_lists.cpp` 中 append/toggle 后 apply updates。参考实现 只实现旧层 poll 三件套，先按 peer+msg_id 取消息并提取 poll_id，`getPollVotes` 把 limit 压到 50；参考实现 对 poll 已有 domain event 与 sender/self update 分离，todo append 依赖 `MessageMediaToDo`，toggle/read-unread/add/delete 多数仍未实现。telesrv 当前做显式有界兼容：read-only poll 入口返回空，投票/新增/删除答案和 todo 变更返回 `MESSAGE_ID_INVALID`，并保留 `OPTIONS_TOO_MUCH/OPTION_INVALID/TODO_NOT_MODIFIED/BROADCAST_FORBIDDEN` 等客户端可理解错误，等 media store 接入后再生成真实 `updateMessagePoll`、todo service message 与 channel pts。

资料页、贴纸、custom emoji 与 reaction tag 也会在群/频道页面后台触发，不应落到 fallback。TDesktop `getCommonChats` 用 `max_id/limit` 分页共同群，且资料页按钮依赖 `UserFull.common_chats_count`；参考实现 都只把共同 channel 中的 megagroup 作为共同群。telesrv 只查双方 active membership 的 megagroup/supergroup，排除 broadcast/left/kicked/deleted，PG 走 `user_channel_member_index(user_id, channel_id) WHERE active megagroup` 交集和 channel id seek 分页，避免 `users.getFullUser` 私聊打开路径反向规划 `channel_members` 的 64 个 channel 分区。`getExtendedMedia` 成功后只 apply updates，paid media 未接入时空 updates 是安全结果；attached stickers、custom emoji documents、sticker search、emoji keyword difference 都接受空 vector/空 found results；reaction top/recent/saved tag titles 用 hash 缓存，rename tag 会本地先改名再调用 `updateSavedReactionTag`。sticker/custom emoji 不查外部索引，saved-message tag assignment/count/per-peer 暂不持久化，避免为了兼容 UI 预取而引入无界查询或半截状态。

`updates.getChannelDifference` 有三种客户端路径：

- 无新增：`updates.channelDifferenceEmpty{final=true, pts, timeout}`。
- 正常补差：`updates.channelDifference{final, pts, new_messages, other_updates, chats, users}`。
- 差量过长：`updates.channelDifferenceTooLong{dialog, messages, chats, users}`，客户端会用返回 dialog 的 `pts` 重置 channel 本地状态，并拉历史范围校验。

### gotd / TL Layer 225

gotd 已提供所有需要的 Layer 225 类型和 dispatcher：

- `MessagesCreateChatRequest` 返回 `messages.InvitedUsers`。
- `ChannelsCreateChannelRequest` 返回 `UpdatesClass`，包含 `Broadcast/Megagroup/Forum/TTLPeriod`。
- `UpdatesGetChannelDifferenceRequest` 返回 `UpdatesChannelDifferenceClass`，请求 limit 对普通用户建议 10-100，服务端必须 cap。
- `UpdateNewChannelMessage` 携带 `message/pts/pts_count`。
- `UpdateEditChannelMessage`、`UpdateDeleteChannelMessages`、`UpdatePinnedChannelMessages` 都走 channel pts；delete 的 `pts_count` 必须等于本次删除 id 数，pin/edit 为 1。
- `UpdateChannelParticipant` 携带 actor、prev/new participant，适合 editAdmin/editBanned 的在线瞬时更新；Layer 225 该 update 不带 `pts/pts_count`，不能作为 channel pts durable event。
- `ChatInviteExported`、`ChatInvite`、`ChatInviteAlready` 覆盖邀请链接导出、预览和已加入状态。
- `Channel` 和 `ChannelFull` 有 TDesktop 最小必需字段：`AccessHash`、`Broadcast`、`Megagroup`、`HasLink`、`ParticipantsCount`、`AdminRights`、`BannedRights`、`DefaultBannedRights`、`LinkedChatID`、`ReadInboxMaxID`、`ReadOutboxMaxID`、`UnreadCount`、`NotifySettings`、`ExportedInvite`、`Pts`。
- `channels.getGroupsForDiscussion#f5dad378` 无入参，返回 `messages.Chats`；`channels.setDiscussionGroup#40582bb2` 接 `broadcast:InputChannel group:InputChannel`，返回 Bool，并显式定义 `LINK_NOT_MODIFIED/BROADCAST_ID_INVALID/MEGAGROUP_ID_INVALID/MEGAGROUP_PREHISTORY_HIDDEN` 等错误。`channelAdminLogEventActionChangeLinkedChat` 可记录管理日志。
- `messages.getReplies#22ddd30c` / `messages.getDiscussionMessage#446972fd` / `messages.readDiscussion#f731a9f4` 使用 channel peer + message id 定位 thread；`messageReplies` 的 `comments/channel_id/replies_pts/max_id/read_max_id` 是 TDesktop 展示 comment button、recent replies 与 unread state 的直接输入。

实现时 `tg.*` 仍只能出现在 `internal/rpc`，domain/app/store 使用自有模型。

### 参考实现 A

参考实现 有一个关键语义可借鉴：`MessageSubType.AutoCreateChannelFromChat`。普通群创建最终进入 channel 创建 saga，先创建 channel creator/member/invite，再发送 `messageActionChannelCreate` 服务消息。它在 `SendMessageSaga` 中按 owner peer 分配 message id 与 pts；当 `ToPeer == Channel` 时只创建 channel owner 的 outbox message，不为每个成员创建 inbox message，随后 `SetChannelPts` 更新 channel read model 的 `Pts/TopMessageId/LastSenderPeerId/LastSendDate`。

成员加入/邀请/退出会创建或更新成员 dialog，并且仅 megagroup 生成 `ChatAddUser/ChatDeleteUser/Join` 等服务消息；broadcast channel 不向普通成员写这类群服务消息。它还把 channel update 保存和在线推送分开：channel durable update 供 `getChannelDifference`，在线推送只给活跃成员/管理员/被 mention 用户。

ack/globalSeqNo 的设计也值得保留为边界：`msgs_ack` 只确认某个 server msg_id/RPC response 已送达，ack cache 再把它映射回 pts/globalSeqNo 更新设备已确认水位。业务 pts 仍由 owner/channel 事件流决定，不能让 MTProto ack 直接产生业务事件。

管理语义方面，参考实现 的 editAdmin/editBanned 都先做权限校验，再构造 participant update；title 变更会产生可见服务消息；pin 走 `messages.updatePinnedMessage` 并只推进 channel pts；invite link 支持导出、检查和导入，join-approval/request_needed 是单独路径，不能误当作已经入群。TDesktop 对 `messages.importChatInvite` 明确分支处理 `INVITE_REQUEST_SENT` 与 `USERS_TOO_MUCH`，因此 request-needed 的首版兼容 stub 必须返回 `INVITE_REQUEST_SENT`，usage limit 满必须返回 `USERS_TOO_MUCH`，不能退化成坏链接/不可访问错误。

username 与管理项方面，参考实现 的 `channels.checkUsername` 只校验格式、access_hash 和全局 username 占用；`channels.updateUsername` 要求 channel owner，大小写不敏感相同值返回 `USERNAME_NOT_MODIFIED`；`getAdminedPublicChannels` 返回当前用户管理的公开 channel 列表。`toggleSignatures/togglePreHistoryHidden/toggleSlowMode/updateColor` 都先校验权限后发布命令；`updateEmojiStatus` handler 仍是空 updates，但 read model/converter 已有 `EmojiStatus` 字段。telesrv 采用这个边界：主 username/signatures/prehistory/slowmode/color/profile_color/普通 emoji status 真实持久化，Fragment usernames、贴纸、collectible emoji status 等依赖额外模型的入口继续显式 stub 或错误。

参考实现 的 `messages.toggleNoForwards` 已实现 channel 路径：先校验 access hash，再发布 `ToggleChannelNoForwardsCommand`，通过 channel 聚合返回 updates；`messages.setChatAvailableReactions` 和 `messages.setChatTheme` handler 仍未实现，但 `ChannelFullReadModel/ChannelFullMapper` 已有 `ReactionType/AvailableReactions/ReactionsLimit` 到 `chatReactions*` 的映射。telesrv 因此不把 reactions 当纯兼容噪声，而是持久化到 channel setting，并在 full channel 中恢复给 TDesktop。

参考实现 对 TDesktop 长尾入口多采用空/BoolTrue 响应：`readMessageContents/reportSpam/setEmojiStickers/getInactiveChannels/getChannelRecommendations/checkSearchPostsFlood/setMainProfileTab` 等直接返回兼容值；`channels.getMessageAuthor` handler 存在但未实现。参考实现 的 `messages.readMessageContents` / `channels.readMessageContents` 会先查当前用户可见消息、过滤 mention/media_unread/reaction 内容状态，再向 not-me 推 `updateReadMessagesContents` 或 `updateChannelReadMessagesContents`；未找到 `channels.getMessageAuthor` 对应实现。telesrv 已为 private 接入 owner 侧 `media_unread/reaction_unread` 持久状态与 durable `read_message_contents`，channel mention/media unread 则来自 `channel_unread_mentions` viewer-state；channel unread reaction 继续由 `channels.readMessageContents/readReactions` 清理。`getMessageAuthor` 则按 Layer225/TDesktop monoforum 右键菜单的最小可用语义，只查可见 channel message 的 `SenderUserID` 并返回 user，完整 monoforum 管理员权限留后续模型。`deleteParticipantHistory` 做管理员权限后按固定 page size 分批删除；`searchPosts` 按 参考实现 的 PublicPosts 语义落到本项目公开 username channel_messages 查询，只返回公开频道/超级群文本消息，满页用 `next_rate + offset_peer + offset_id` seek 翻页，付费 flood/限额继续由 `checkSearchPostsFlood` 免费 stub 承接；`getInactiveChannels` 按 TDesktop Premium 限额弹窗消费方式返回当前用户 active 频道/超级群，`dates[i]` 与 `chats[i]` 对齐并按最久未活跃排序；`getChannelRecommendations` 则从空响应提升为公开 broadcast channel 推荐，指定来源时排除 source，无来源时排除当前账号已加入频道；`getGroupsForDiscussion/setDiscussionGroup` 是讨论组链路。telesrv 借鉴其删除边界：按 sender 分页取一批 message id，生成一条 channel delete update，通过 `offset` 提示客户端续删，禁止一次性展开超大历史。

参考实现 的 `channels.getAdminLog` 当前返回空结果，但保留了 read model/query 模型：按 `channel_id`、action types、skip/limit 拉取 admin log event。telesrv 采用“有真实 event store、但只实现 TDesktop 首批可见 action”的路线，避免管理入口只能打开空页。

参考实现 的 `messages.getMessageReadParticipants` 先按 owner peer 校验 message 存在和 7 天过期窗口，再查询 `ReadingHistoryReadModel(TargetPeerId, MessageId >= msg_id, ReaderPeerId != self)` 并返回 `TReadParticipantDate{user_id,date}`。telesrv 采用同一语义边界，但落到 channel 单份消息模型：成员 `read_inbox_max_id >= msg_id`、`available_min_id < msg_id` 且有显式 `read_inbox_date` 才算已读者。

参考实现 的 `messages.getMessageEditData` 先校验 access_hash/message，再按编辑时间窗口返回 caption 可编辑标记；参考实现 对私聊/旧群先校验作者或管理员编辑权限，再返回 `messages.messageEditData{caption=false}`，channel 分支在旧项目被商业版挡住。telesrv 当前没有媒体 caption 编辑，因此采用更窄且可验证的语义：对私聊和 channel peer 都先校验消息存在、可见和作者/`edit_messages` 权限，再返回 `caption=false`，避免 TDesktop 编辑入口触发未知 RPC 或绕过权限。

参考实现 的 `channels.readHistory` 会读取 `max_id` 对应消息的 sender，触发 `UpdateReadChannelOutbox` saga，并向 sender 推送 `TUpdateReadChannelOutbox{channel_id,max_id}`。`channels.getFullChannel` 还会把 dialog read model 的 `ReadInboxMaxId/ReadOutboxMaxId` 写回 `ChannelFull`，dialog mapper 也持久返回 `ReadOutboxMaxId`。TDesktop 收到 `updateReadChannelOutbox` 后只调用 `History::outboxRead(max_id)`；如果在线 update 丢失或设备离线，TDesktop 会在 `messages.getDialogs/messages.getPeerDialogs` 的 dialog 字段、`channels.getFullChannel` 的 channel full 字段中重新应用 `read_outbox_max_id`。telesrv 采用同一客户端语义，但在 store 内 bounded 扫描最近 read delta，可一次推进多个相关发送者，同时保留 fanout/scan 硬上限。

参考实现 的 `EditPeerFoldersSaga` 明确把 `TInputPeerChannel` 转成 `PeerType.Channel` 并发布 `UpdateDialogFolderCommand`，最终回 `updateFolderPeers`；dialog unread/pinned/read-outbox 都在同一个 dialog read model 上表达。TDesktop 侧 `updateDialogPinned/updatePinnedDialogs/updateDialogUnreadMark/updateFolderPeers` 也按 `DialogPeer/Peer` 泛化消费，因此 telesrv 不能只更新私聊 `dialogs` 表，channel peer 必须更新 `channel_dialogs` 并仍走账号级 user pts/update。

参考实现 的 send/forward 请求转换器保留 Layer225 `InputReplyTo`，message mapper 再把业务 `ReplyTo/InputReplyTo` 转回 `MessageReplyHeader`；它还用 `MessageForwardedEvent/MessageReplyUpdatedEvent` 维护原消息回复统计。参考实现 的 message/dialog app service 会把 `FwdHeader.FromId/SavedFromPeer/SavedFromId` 放进额外 peer 集合，确保客户端 apply update 前能解析 forward 来源。telesrv 保留 `reply_to_msg_id/top_id/quote` 与 forward header 的客户端可见语义，在响应、durable difference、outbox 投递里补齐可解析的 user/channel peer 上下文，并用 `reply_to_top_id` + linked discussion root 维护首版 channel replies 统计/已读。

参考实现 的 admin/ban 流程先校验 `add_admins/ban_users`，再更新成员 read model，并通过 `updateChannel`/`updateChannelParticipant` 让在线客户端刷新状态；TDesktop 对 `updateChannelParticipant` 本身不走 channel pts 检查，且基线 `applyUpdateNoPtsCheck()` 不处理它。telesrv 因此不把 editAdmin/editBanned 写入 `channel_update_events`，也不为纯权限/封禁状态变化分配 channel pts；在线响应/推送只携带 `updateChannelParticipant + updateChannel`，离线客户端通过 full channel、participants 列表或后续可见消息的 channel pts 路径恢复状态。若操作产生可见 service message（例如加人/踢人消息），那条 service message 作为 `updateNewChannelMessage` 单独占 channel pts。

### 参考实现 B

参考实现 的 channel 模型包含 `broadcast`、`megagroup`、`top_message`、`pts`、`participants_count`、`default_banned_rights`、`hidden_prehistory`、`slowmode`、`forum`、`noforwards` 等业务字段。它用 Redis key `channel_pts` 缓存 channel pts，并用 `channel_pts_updates` 持久化 `channel_id/pts/pts_count/update_type/update_data/date`，`updates.getChannelDifferenceV2` 按 `channel_id AND pts > ? ORDER BY pts ASC` 返回差量。

参考实现 对 `messages.toggleNoForwards` 先把 peer 限定为 chat/channel，再在 chat service 校验创建者权限，更新 `noforwards` 与版本后返回 chat update；`chat.setChatAvailableReactions` 会校验成员/管理员权限，把 reaction type 与 reaction list 持久化。telesrv 借鉴“设置是 channel state，不是消息事件”的边界，但权限放宽到 change_info 管理员，与当前 channel 管理设置统一。

参考实现 的不足需要避免：部分差量恢复时靠反解 message 的 `from_id` 临时判断 `out`，代码里有 sender TODO。telesrv 的 `channel_update_events` 必须显式保存 `sender_user_id`、`affected_user_ids`、`message_ids` 等负载，不能把客户端展示所需信息藏在 TL JSON 里。

参考实现 的 admin log 通过 `channel_admin_logs` 保存 actor、event、JSON action、query、date，并在 BFF 先校验 creator/admin 后调用 service。它的不足是只按最近 24h/channel 扫描且过滤 TODO；telesrv 改为 channel hash 分区、`id` 单调分页、actor/type 索引和 request cap，保留语义但不继承实现。

参考实现 的 `messages.getMessageReadParticipants` 在 channel 版本中通过 dialog service 拉取已读成员 id，旧 chat 版本则按参与者 dialog 的 `read_inbox_max_id` 判断是否覆盖目标消息；日期字段仍返回 0。telesrv 保留“按成员读水位判断”的核心语义，并新增 `read_inbox_date`，避免 UI 已读详情只能显示无时间。

参考实现 的 `messages.getMessageEditData` 私聊路径会按 owner message id 取 message box，并拒绝非作者；旧群允许具备编辑权限的管理员，最终仍返回 `caption=false`。telesrv 的 channel/supergroup 路径沿用这个权限边界，但查询单份 `channel_messages` 与当前 viewer 的 member 状态，不落 legacy chat。

参考实现 频道 participant/dialog 模型都保存 `read_outbox_max_id`，BFF `channels.getFullChannel` 会用当前 dialog 的 `ReadOutboxMaxId` 覆盖 `ChannelFull.ReadOutboxMaxId`，另有 `message_read_outbox` 记录读回执排查维度。telesrv 当前先把发送者 `channel_members/channel_dialogs.read_outbox_max_id` 与在线 `updateReadChannelOutbox`、离线 dialog/full channel 恢复路径打通；单条读者时间仍由 `read_inbox_date` 支撑。

参考实现 的 channel outbox 在写入 reply 消息前会按 `channel_id + reply_to_msg_id` 读取被回复消息：如果目标消息已有 `reply_to_top_id` 就继承，否则把当前 `reply_to_msg_id` 作为 top，最终把 `ReplyTo/ReplyToTopId` 一起落库；forum topic 发送后会更新 topic 的 top message。TDesktop 在 topic 输入框里会发送 `reply_to_msg_id=0 + top_msg_id=topicRootId`，参考实现 的 `InputReplyToMessage.TopMsgId` 也按 topic/thread 维度保存并在 header 上打 `forum_topic`。telesrv 借鉴该语义，但改成事务内校验目标或 topic root 未删除且对当前成员可见，非法目标返回 `REPLY_MESSAGE_ID_INVALID`，topic 内普通消息返回 `messageReplyHeader{forum_topic, reply_to_top_id}` 并更新 topic top message。

参考实现 的 `updates.getChannelDifference` 还会在当前 participant 的 `AvailableMinPts > req.pts` 时把请求 pts 抬到 `AvailableMinPts`，再去读 `channel_pts_updates`。这是避免新成员用 `pts=0` 拉到入群前消息类 durable 事件的关键边界；telesrv 因此在 `channel_members` 中同时保存 `available_min_pts`，加入/导入/受邀/重新加入时设为加入前 `channels.pts`，消息历史可见性仍由 `available_min_id` 独立控制。

参考实现 的 `channels.deleteHistory` 本地清空路径不写 channel pts，而是返回并同步 `updateChannelAvailableMessages{channel_id, available_min_id}`；TDesktop 在 `api_updates.cpp` 收到后设置 channel `available_min_id` 并对已加载 history 执行 `clearUpTill`，`ChannelData::setAvailableMinId` 本身不会做 max-clamp。telesrv 采用同一客户端语义：本地清空只更新当前账号成员/dialog 水位，同时写账号级 durable update 供其它设备在线推送或 `updates.getDifference` 离线恢复；返回和推送的 `available_min_id` 必须是实际应用后的单调水位 `max(old_available_min_id, requested_max_id)`，避免多设备乱序或 stale 请求把 TDesktop 本地可见下界回退。

## Domain Model

新增 domain model：

- `Channel`：`ID`、`AccessHash`、`Title`、`About`、`Username`、`CreatorUserID`、`Broadcast`、`Megagroup`、`Forum`、`ForumTabs`、`Date`、`ParticipantsCount`、`AdminsCount`、`KickedCount`、`BannedCount`、`DefaultBannedRights`、`TopMessageID`、`PinnedMessageID`、`Pts`、`Deleted`、`NoForwards`、`TTLPeriod`。
- `Channel` 还保存设置字段：`PreHistoryHidden`、`ParticipantsHidden`、`AntiSpam`、`SlowmodeSeconds`、`Signatures`、`ReactionPolicy`、`Color`、`ProfileColor`、`EmojiStatus`。`ParticipantsHidden` 会让 `ChannelFull.participants_hidden` 可见，并让非管理员成员列表/已读详情按隐藏成员语义收敛；`AntiSpam` 会让 `ChannelFull.antispam` 可见，供 TDesktop 管理员页恢复开关；`SlowmodeSeconds>0` 会让 `tg.Channel.slowmode_enabled` 和 `tg.ChannelFull.slowmode_seconds` 可见；`Color/ProfileColor/EmojiStatus` 转成 `tg.Channel.color/profile_color/emoji_status`；`ReactionPolicy` 转成 `ChannelFull.available_reactions/reactions_limit/paid_reactions_available`。
- `ChannelInvite`：`ChannelID`、`InviteID`、`Hash`、`AdminUserID`、`Title`、`Permanent`、`Revoked`、`RequestNeeded`、`ExpireDate`、`UsageLimit`、`UsageCount`、`Date`。
  - `importInvite` 必须在同一事务内锁定对应 invite row 后检查并递增 `usage_count`，避免多个客户端同时导入一次性链接时突破 `usage_limit`。
- `ChannelMember`：`ChannelID`、`UserID`、`InviterUserID`、`Role`、`Status`、`JoinedAt`、`LeftAt`、`AdminRights`、`BannedRights`、`Rank`、`AvailableMinID`、`AvailableMinPts`、`ReadInboxMaxID`、`ReadInboxDate`、`ReadOutboxMaxID`、`UnreadMark`。
- `ChannelMember.AvailableMinID`：当前成员可见历史下界；开启 prehistory hidden 后，新加入/导入/受邀成员初始化为加入前 `channel.top_message_id`，只看后续消息和自己的加入服务消息。
- `ChannelMember.AvailableMinPts`：当前成员可恢复 channel difference 的 pts 下界；新加入/导入/受邀/重新加入成员初始化为加入前 `channels.pts`，`updates.getChannelDifference(pts=0)` 也会先抬到该值，避免入群前消息类 durable event 泄漏。
- `ChannelMember.ReadInboxDate`：当前成员最后一次推进 `read_inbox_max_id` 的时间，用于 `messages.getMessageReadParticipants` 返回 `readParticipantDate.date`；不参与 channel pts。
- `ChannelMember.SlowmodeLastSendDate`：普通成员最近一次成功发言时间，用于服务端按 channel 维度返回 `SLOWMODE_WAIT_X`；creator/admin 不受首批 slowmode 限制。
- `ChannelMessage`：`ChannelID`、`ID`、`RandomID`、`SenderUserID`、`From`、`SendAs`、`Date`、`EditDate`、`Post`、`Silent`、`NoForwards`、`Body`、`Entities`、`ReplyTo`、`Forward`、`Action`、`Pts`、`Deleted`；`Mentioned` / `MediaUnread` 是按当前 viewer 回填的瞬时字段，不是全局消息真值。
- `ChannelDialog`：当前 user 对 channel 的会话摘要，保存 `FolderID`、`Pinned`、`PinnedOrder`、`TopMessageID`、`ReadInboxMaxID`、`ReadOutboxMaxID`、`UnreadCount`、`UnreadMentions`、`UnreadMark`、`ViewForumAsMessages`、`NotifySettings`。channel message id 是 channel-scoped，跨频道 dialog 排序不能只看 `top_message_id`；统一按 `pinned DESC, pinned_order DESC, top_message_date DESC, top_message_id DESC, channel_id DESC` 排序，并用 `offset_date + offset_id + offset_peer(channel_id)` 做 seek cursor。folder include/exclude/read/archive/type 条件必须在 SQL `LIMIT` 前下推，避免 TDesktop dialogs 翻页重复顶部频道或自定义分组漏掉旧频道。
- `ChannelUnreadMention`：按 `(user_id, channel_id, message_id)` 保存未读提及，`top_message_id` 支持 thread/topic 级清除，`media_unread` 表示该未读提及同时对应媒体内容未读；它是 owner 视角状态，不写入 `channel_update_events`，`messages.readMentions` 返回当前 channel pts 供客户端清本地 badge。
- `ChannelUpdateEvent`：`ChannelID`、`Pts`、`PtsCount`、`Type`、`Date`、`MessageID`、`MessageIDs`、`SenderUserID`、`UserIDs`、`Payload`。
- `ChannelAdminLogEvent`：`ChannelID`、`ID`、`UserID`、`Date`、`Type`、前后字符串/布尔/整数、前后 participant、相关 message、`Query`；用于 `channels.getAdminLog`，不参与 channel pts。

`domain.PeerType` 扩展为：

- `user`
- `channel`

首批不持久化 `chat` peer；RPC 层只在 `messages.createChat` 的 TDesktop 同步响应暴露 migrated legacy `chat` 外观，主消息/media/history/read 等入口遇到 `InputPeerChat/InputChat` 均映射到同 id megagroup/channel。

## Storage

大表从第一版开始分区：

| table | partition key | purpose |
|---|---|---|
| `channels` | hash(`id`) | channel/supergroup 主体 |
| `channel_members` | hash(`channel_id`) | 成员、权限、读水位、可见历史边界 |
| `user_channel_member_index` | primary key(`user_id`, `channel_id`) | user 维度成员索引；供共同群、`users.getFullUser.common_chats_count` 等 user→channel 热路径使用 |
| `channel_messages` | hash(`channel_id`) | 单份 channel message；TDesktop 看到的 message id 即 `(channel_id, id)` 中的 `id` |
| `channel_message_viewers` | hash(`channel_id`) | `(channel_id,message_id,viewer_user_id)` 去重视图，支持 `messages.getMessagesViews(increment=true)` 幂等递增 |
| `channel_message_reactions` | hash(`channel_id`) | `(channel_id,message_id,reacted_user_id,reaction)` 当前 reaction 状态，支持 `sendReaction/getMessagesReactions/getMessageReactionsList` |
| `private_message_reactions` | message id fk | `(private_message_id,user_id,reaction)` 当前私聊 reaction 状态，双端 message box 共享聚合，支持 private `sendReaction/getMessagesReactions/getMessageReactionsList` |
| `user_saved_reaction_tags` | user_id prefix | 账号级 saved-message reaction tag 标题；支持 `messages.getSavedReactionTags/updateSavedReactionTag`，不承载 message assignment/count |
| `channel_unread_mentions` | hash(`user_id`) | owner 视角未读提及/媒体未读索引，支持 `messages.getUnreadMentions/readMentions`，不复制 channel message body |
| `channel_update_events` | hash(`channel_id`) | channel pts durable log，供 `updates.getChannelDifference` |
| `channel_dialogs` | hash(`user_id`) | 当前账号对 channel 的 dialog/read/folder/pin/mute 摘要 |
| `dialog_drafts` | hash(`user_id`) | user/channel peer 云草稿；支持 forum `top_msg_id`，不复制 channel message |
| `channel_invites` | hash(`channel_id`) | 默认 invite link、管理员导出的邀请链接、usage/requested 计数 |
| `channel_invite_importers` | hash(`channel_id`) | invite importer/read model 与 pending join request；每个 `(channel_id,user_id)` 只保留当前状态 |
| `channel_admin_log_events` | hash(`channel_id`) | 管理日志；按 channel 内单调 `id` seek pagination，不复制 channel 消息正文 |

关键索引：

- `channel_usernames(username_lower)` 主键保存公开 channel username 占用；`channels.username` 保存展示值，避免 PG 分区表唯一索引必须包含分区键的问题。
- `channel_members(channel_id, user_id)` 主键；父表上的 `(user_id, channel_id)` 索引不能裁剪 `channel_id` HASH 分区，只可作为单分区内辅助索引，禁止用它承载 user→channel 热路径。
- `user_channel_member_index(user_id, channel_id) WHERE status='active' AND megagroup AND NOT broadcast AND NOT deleted` 支持共同群 count/list；由 channel member upsert、leave 和 delete channel 事务同步维护。后续凡是从 user 入口列 joined/admined/left channel，都应扩展这张 user 维度 read model 或两步取 bounded channel_id 后再按 `channel_id/id = ANY($1)` 访问分区表。
- `channel_members(channel_id, read_inbox_max_id, user_id) WHERE status='active'` 支持小群已读详情按读水位有界查询；同时用 `available_min_id < msg_id` 排除加入前不可见历史。
- `channel_messages(channel_id, id DESC) WHERE deleted=false` 历史 seek pagination。
- `channel_messages(channel_id, sender_user_id, id DESC) WHERE deleted=false` 支持 `channels.deleteParticipantHistory` 按发送者有界取最近一页消息，避免成员历史删除全表扫。
- `channel_messages(channel_id, sender_user_id, random_id)` 唯一，保障 channel send 幂等。
- `channel_messages(channel_id, reply_to_top_id, id DESC) WHERE reply_to_top_id > 0 AND NOT deleted` 支持 `messages.getReplies` thread/comment seek pagination。
- `channel_messages(discussion_channel_id, discussion_message_id) WHERE discussion_channel_id <> 0 AND discussion_message_id <> 0 AND NOT deleted` 支持 broadcast post 到 discussion root 的反查与排查。
- `channel_message_viewers(channel_id, message_id, viewer_user_id)` 主键去重；`messages.getMessagesViews` 先按最多 100 个 id 过滤可见 `channel_messages`，只对首次插入的 viewer 更新 `channel_messages.views_count`，不按 viewer 做 count 聚合扫描。
- `channel_message_reactions(channel_id, message_id, reaction_date DESC, reacted_user_id DESC, reaction_value ASC)` 支持最近 reaction 回填和列表 seek；`(channel_id,message_id,reaction_type,reaction_value,reaction_date DESC,reacted_user_id DESC)` 支持按单个 emoji 过滤，不使用 SQL OFFSET。
- `channel_unread_mentions(user_id, channel_id, top_message_id, message_id DESC)` 支持当前账号 unread mentions seek；发送/编辑时只为解析出的 active/可见/未读成员插入，单条消息最多 100 个候选，清除时单批最多 1000 条；`media_unread` 随同一行保存，供 history/getMessages/getChannelDifference/online update 按 viewer 回填 `message.media_unread`。
- `channel_update_events(channel_id, pts)` 主键；`(channel_id, pts)` 升序扫描差量。
- `channel_dialogs(user_id, folder_id, pinned DESC, pinned_order DESC, top_message_date DESC, top_message_id DESC, channel_id DESC)` 支持 dialogs seek；`default_send_as_peer_type/default_send_as_peer_id` 只保存当前 owner 的默认发送身份，不参与列表排序。
- `dialog_drafts(user_id, date DESC, peer_type, peer_id, top_message_id)` 支持 `messages.getAllDrafts/clearAllDrafts` 有界扫描；draft 内容用 domain JSON 保存，业务层不持有 `tg.*`。
- `channel_invites(channel_id, admin_user_id, revoked, created_at DESC, hash DESC)` 支持 TDesktop invite links 管理页按 admin/revoked/offset seek 翻页；`channel_invite_hashes(hash)` 用于导入链接按 hash 快速定位，撤销 invite 不删除 hash 映射，避免管理页 detail 失效。
- `channel_invite_importers(channel_id, invite_id, requested, date DESC, user_id DESC)` 与 `(channel_id, requested, date DESC, user_id DESC)` 支持 `messages.getChatInviteImporters` 的 link/requested/filter 查询；`q` 过滤走有界 candidate 后关联 user，不允许全表搜索。
- `channel_admin_log_events(channel_id, id DESC)` 支持 admin log 翻页；`(channel_id, actor_user_id, id DESC)` 支持 admins 过滤；`(channel_id, event_type, id DESC)` 支持 events_filter。

Redis 可恢复计数：

- `counter:channel_id`：channel id，值域应避开 user id。
- `counter:channel_msg_id:{channel_id}`：channel message id。
- `counter:channel_pts:{channel_id}`：channel pts。

管理日志 `id` 使用 `channels.admin_log_seq` 在同一事务内 `UPDATE ... RETURNING` 分配，原因是它只服务管理页 seek，不参与高频消息 pts；后续如果 admin log 写入成为瓶颈，再迁移到可恢复 Redis counter + noop 占位策略。

Redis miss 恢复来源：

- channel id 从 `MAX(channels.id)` 恢复。
- message id 从 `MAX(channel_messages.id WHERE channel_id=?)` 恢复。
- channel pts 从 `MAX(channel_update_events.pts WHERE channel_id=?)` 恢复。

分配必须使用 Redis Lua 的初始化+递增原子脚本；批量 delete/clear 这类 `pts_count>1` 的操作必须一次性分配连续 range，PG fallback 也要实现 `NextChannelPtsN(current+count)`，不能用多次读取 `MAX(pts)+1` 模拟。事务失败但 pts 已分配时必须写 `noop` channel update 占位，避免 TDesktop channel PtsWaiter 永久 gap。

## Viewer State / Difference Nudge

`tg.Message.mentioned` 与 `media_unread` 对 channel/supergroup 来说是 viewer-specific 状态，来源只允许是 `channel_unread_mentions` 等 owner 视角 unread 表，不写入 `channel_messages` 全局行：

1. 发送或编辑 channel message 时，RPC 层解析 mention-name entity / `@username`，store 只为 active、可见、未读且不是 sender 自己的成员写 unread mention。带媒体的 mention 同行保存 `media_unread=true`。
2. `messages.getHistory`、`messages.getMessages`、`updates.getChannelDifference` 与在线 `updateNewChannelMessage/updateEditChannelMessage` 都按当前 viewer 重取 unread mention 状态，再设置 TL `mentioned/media_unread`。未被 mention 的 viewer 看到同一条消息时两个 flag 必须为 false。
3. `messages.readMentions` 清理当前 owner 的 unread mention 后，后续 history/difference 不再带 `mentioned/media_unread`。编辑移除 mention 会删除旧 unread mention；新增 mention 会补写新 owner 的 unread mention，供在线和离线差量恢复。
4. 账号级 `updates.getDifference` 不承载 channel message 本体。若请求 date 之后当前用户 active joined channel 有 `channel_update_events`，RPC 层追加计算型 `updateChannelTooLong(channel_id, pts)` nudge，提示 TDesktop 随后调用 `updates.getChannelDifference`；该 nudge 不写 `user_update_events`，不推进账号 pts，也不影响 account difference 连续性。
5. create/invite/join/leave/import/hide request 等 membership/state 操作即使没有 service message pts，也必须在 RPC response 和在线推送里带 `updateChannel`。megagroup 有可见 service message 时返回/推送 `updateNewChannelMessage` 占 channel pts，同时追加 `updateChannel` 刷新 participant count/self state；broadcast invite/join/leave 不占 channel pts，但仍带 viewer-specific chats 与 `updateChannel`。

## Create Flow

`messages.createChat`：

1. RPC 层解析 users/title/ttl，校验至少 1 个非自己用户，限制单次邀请数量。
2. 调用 `ChannelService.CreateMegagroupFromCreateChat`，内部强制 `Broadcast=false, Megagroup=true`。
3. 创建 channel、creator member、受邀成员、默认 invite link。
4. 分配 channel message id + channel pts，写 `messageActionChannelCreate` 或 `messageActionChatCreate` 兼容服务消息。
5. 为 creator 与受邀成员创建 `channel_dialogs`，但不复制 message body。
6. 写 `channel_update_events(updateNewChannelMessage)`。
7. 给 creator 当前 RPC 返回 `messages.InvitedUsers{updates}`；TDesktop ctx 下该同步响应的 `chats` 第一项是带 `migrated_to` 的 legacy `chat` 外观，第二项保留真实 `channel`。其它在线成员通过 outbox/active channel push 收到真实 channel updates，不推 legacy 外观。

`channels.createChannel`：

- `broadcast=true` 创建频道，`megagroup=true` 创建超级群。
- 如果两个 flag 都没传，TDesktop 路径按 broadcast 处理；服务端可返回 `CHANNEL_INVALID` 防错误客户端。
- `for_import` 依赖 `messages.initHistoryImport` 与导入 session，本阶段没有历史导入模型，返回 `CHAT_INVALID`；`geo_point/address` 依赖 geogroup/location 模型，本阶段返回 `ADDRESS_INVALID`。这两类高级 flags 不能落 `NOT_IMPLEMENTED`，也不能伪造成普通群创建成功。
- `ttl_period` 只接受非负值，负数返回 `TTL_PERIOD_INVALID`；真实 TTL 自动删除管线后续接入。
- broadcast 的普通成员不能发普通消息；creator/admin 可发 `post` 消息。
- megagroup 的 slowmode 只限制普通成员发普通消息；服务端检查 `slowmode_last_send_date + slowmode_seconds`，重复 `random_id` 命中幂等结果时不重新触发限速。
- 如果 `pre_history_hidden=true`，invite/join/import invite 写成员时用加入前的 `top_message_id` 初始化 `available_min_id`；无论是否隐藏历史，都用加入前 `channels.pts` 初始化 `available_min_pts`。为了避免旧历史在惰性 unread 计算中变成未读，invite/join/import invite 都会把 `read_inbox_max_id` 初始化到加入前 top；主动 join/rejoin/import invite 生成的自服务消息会继续把当前成员 read 水位推进到该服务消息，邀请服务消息则保留为被邀请者的一条未读。

## Send / History Flow

频道/超级群消息只写一份 `channel_messages`：

1. 校验成员状态、ban/default banned rights、broadcast 发送权限、普通成员 invite/send 默认限制、slowmode、noforwards。
2. 如带 `reply_to`，按当前 channel + 当前成员 `available_min_id` 校验目标消息存在、未删除、可见，并反算 `reply_to_top_id`：目标已有 top 则继承，否则 top=目标 msg_id；quote 文本/entities/offset 原样保留，但 `quote_text` 上限 1024、`quote_offset` 按原消息文本 offset 收口到 4096，不能按 message id 放行。
3. 通过 Redis 分配 `channel_msg_id` 与 `channel_pts`。
4. 写 `channel_messages` 与 `channel_update_events(updateNewChannelMessage, pts_count=1)`。
   - `random_id` 幂等重试必须返回原始 `updateNewChannelMessage` 的 durable snapshot；即使该消息之后被编辑或删除，也不能把重试响应污染成 edit/delete 事件或当前消息状态。
5. 更新 `channels.top_message_id/pts`。
6. 更新发送者 `channel_members/channel_dialogs.read_inbox_max_id/read_outbox_max_id/top_message`，自己的新消息立即视为已读/已发。
7. 不写成员 message box；Postgres 用单条 set-based SQL 推进 active/可见成员的 `channel_dialogs.top_message_id/unread_count` 缓存，dialog 查询仍以 `channels.top_message_id` + 当前 member 的 `available_min_id/read_inbox_max_id` 可重算可见 top 与 unread，避免缓存陈旧影响 TDesktop 离线恢复。
8. 在线推送只 fanout 给当前活跃 channel 成员 session，且每个接收者按自己的 viewer user id 重新生成 `tg.Message.out`，不能复用发送者视角的 update；离线设备走 `updates.getChannelDifference`。

`messages.getHistory(InputPeerChannel)` 直接查 `channel_messages(channel_id, id)`，按 `offset_id/max_id/min_id` 做 seek pagination，`offset_id=0` 时支持 `offset_date` date cursor，禁止 SQL 大 OFFSET。`channels.getMessages` 必须按 `id = ANY(...)` 精确批量读取，不允许把稀疏 ID 转成 min/max 范围后再 `LIMIT`，否则 TDesktop 拉 pinned/reply/跳转消息会漏旧消息。`channels.getChannels/channels.getMessages` 这类精确 ID vector 入口统一 cap=100，不能按客户端传入数量无界查库或构造响应。返回消息时按请求 user 动态设置 `out`、`post`、`from_id/send_as` 与 reply/forward 信息。

## Dialog / Read / Unread

`channel_dialogs` 是 owner 视角：

- `read_inbox_max_id`：当前用户已读到的最大 channel message id。
- 被邀请加入已有 megagroup/channel 时，成员的初始 `read_inbox_max_id` 固定到邀请发生前的 `channels.top_message_id`；如果 megagroup 产生 invite service message，则只让这条新服务消息成为未读，不能把入群前历史全部计入 unread。主动 join/rejoin/import invite 也先把 read 水位固定到加入前 top，再把自己的 join service message 标为已读，避免 TDesktop 打开会话时把旧消息批量推进成读回执。`pre_history_hidden=true` 时 `available_min_id` 也落到同一个旧 top，既隐藏旧历史又避免未读数膨胀。成员的 `available_min_pts` 固定到加入前 `channels.pts`，避免离线补偿返回入群前权限/成员事件。
- `joinChannel/importInvite` 必须尊重既有 member 状态：`active` 重复加入返回 `USER_ALREADY_PARTICIPANT`，`kicked`、`banned` 或 `view_messages` ban 不允许直接重新加入；`left` 用户重新加入时恢复 `participants_count`。`inviteToChannel` 对单个已加入用户返回 `USER_ALREADY_PARTICIPANT`，对被踢/被禁看消息用户只有 creator 或具备 `ban_users` 的 admin 才能通过邀请恢复，普通成员单人邀请返回 `USER_KICKED`，多人邀请跳过无权恢复的目标。`leaveChannel` 的在线 recipients 必须包含离开的 user 本人，用于同账号其它设备同步离群状态。
- `importInvite` 的 invite link 错误码需要保持 TDesktop 可解释：`request_needed` 当前不落真实申请流，但返回 `INVITE_REQUEST_SENT`；`usage_limit` 用尽返回 `USERS_TOO_MUCH`；被踢/禁看仍返回 `INVITE_HASH_INVALID`，避免泄漏私有成员状态。
- `read_outbox_max_id`：当前用户发送的 channel/supergroup 消息被其它成员读到的最大值。`readHistory` 只扫描 bounded recent delta，推进涉及发送者的 member/dialog outbox watermark，并通过 `updateReadChannelOutbox` 在线通知发送方；不做全员 O(n) fanout。该水位必须同时从 `messages.getDialogs/messages.getPeerDialogs` 的 dialog 和 `channels.getFullChannel` 的 `ChannelFull` 返回，作为离线设备或丢失实时 update 后的恢复路径。
- `unread_count`：发送事务推进 active/可见成员的 `channel_dialogs` 缓存，计算方式仍是 `max(0, channels.top_message_id - read_inbox_max_id)`；删除/隐藏历史会用 `available_min_id` 修正，读请求必须可重算，不能只信缓存。
- `top_message_id/top_message_date`：`channel_dialogs` 中的值是当前 owner 状态缓存，发送时会随最新消息推进；本地清空历史后可能落后，PG 查询仍必须优先用 `channels.top_message_id > channel_members.available_min_id` 判断当前可见 top，并 join 当前 top `channel_messages` 取真实 `message_date` 排序。
- `folder/pin/manual unread/notify settings/default send-as/view-forum-as-messages` 仍按当前 user 保存，并通过 user_update_events 通知多设备；频道消息本身通过 channel_update_events 补。`messages.toggleDialogPin/reorderPinnedDialogs/markDialogUnread/getDialogUnreadMarks` 和 `folders.editPeerFolders` 必须把 `InputPeerChannel` 分流到 `channel_dialogs`，与私聊 `dialogs` 合并返回；这些是账号级 dialog 状态，不推进 channel pts。Layer 225 `markDialogUnread/getDialogUnreadMarks` 的 `parent_peer` 只用于 monoforum/SavedSublist；当前没有 monoforum subdialog store，因此先校验 parent channel 与 sublist peer 后 BoolTrue/空列表 no-op，避免 TDesktop 后台 `NOT_IMPLEMENTED`，真实 monoforum unread marks 留后续模型。`messages.saveDefaultSendAs` 写 `channel_dialogs.default_send_as_peer_*`，`channels.toggleViewForumAsMessages` 写 `channel_dialogs.view_forum_as_messages` 并发 `updateChannelViewForumAsMessages`，`channels.getFullChannel` 输出 `channelFull.default_send_as/view_forum_as_messages` 供 TDesktop 恢复本地选择。`folder_id=0/1` 是物理主列表/归档状态，必须在 SQL `LIMIT` 前下推；`folder_id>=2` 是 TDesktop 自定义 filter 规则，不能拿它和 `channel_dialogs.folder_id` 直接相等比较。

`messages.readHistory(InputPeerChannel)`：

1. 锁当前 user 的 `channel_dialogs`。
2. 将 `read_inbox_max_id` 推进到 `min(req.max_id, channel.top_message_id)`。
3. 若水位前进，写 `channel_members.read_inbox_date=req.date`；再更新 `unread_count/manual unread`。
4. 对当前 user 产生账号级 `updateReadChannelInbox` 或兼容的 dialog/user update；该事件用于多设备 dialog 未读状态同步。
   - 当前实现记录一条 `user_update_events(read_history_inbox, peer_type='channel')`，在线/离线设备转换为 TL `updateReadChannelInbox`；实时 fallback 使用 channel pts 填 `updateReadChannelInbox.pts`，可靠 outbox/difference 使用账号 pts 作为 durable cursor，TDesktop 在 pts 不匹配时仍会推进 `max_id` 并通过 dialog entry 取回精确 unread。
5. 对读水位前进区间做 bounded sender scan：最多查看最近 `MaxChannelReadOutboxScanMessages=1000` 条、最多通知 `MaxChannelReadOutboxFanout=128` 个发送者；每个发送者只推进自身 `read_outbox_max_id` 并在线推 `updateReadChannelOutbox(channel_id,max_id)`，避免客户端传超大 `max_id` 时生成无界 update。

`messages.getMessageReadParticipants(InputPeerChannel)`：

- 只对 megagroup 返回真实小群读者；broadcast channel 或成员数超过 `chat_read_mark_size_threshold` 返回空列表，避免大频道 O(n) 放大。
- 若 `channels.participants_hidden=true`，已读详情直接返回空；TDesktop 在隐藏成员模式下也会关闭 read participants UI，服务端不通过该接口泄漏成员身份。
- 先校验当前用户可见 channel 与目标 message，删除消息或不可见历史返回 `MESSAGE_ID_INVALID`。
- 消息超过 `chat_read_mark_expire_period` 返回空列表；客户端正常不会发起过期请求。
- 查询 `channel_members` 时只取 `status='active'`、未禁看消息、`available_min_id < msg_id`、`read_inbox_max_id >= msg_id` 且 `read_inbox_date > 0` 的成员，并排除请求者自己；单次最多 50 个。初始 read 水位只用于 unread 基线，不应让新加入成员凭默认水位出现在旧消息的已读列表中。
- 返回的 date 来自 `read_inbox_date`，旧数据未记录日期时允许为 0，TDesktop 会按无具体时间展示。

## Channel Difference

`updates.getChannelDifference(channel, pts, limit)`：

- 校验 access_hash、成员/可见权限、ban。
- `limit` cap：普通用户 `1..100`，超过按 100；内部硬上限 1000，拒绝负数和超大值。
- 若 `pts < 0` 或 `pts > current_channel_pts` 返回 `PERSISTENT_TIMESTAMP_INVALID`，避免客户端用未来水位跳过 durable log。
- 从 `channel_update_events` 读取 `pts > req.pts ORDER BY pts ASC LIMIT cap+1`。
- `channel_update_events.payload` 对 new/edit/pin 等消息类事件保存 domain 快照；`updates.getChannelDifference` 必须优先使用事件时刻的 message snapshot，不能回读当前 `channel_messages` 覆盖旧事件，否则连续编辑、删除后的离线补偿会丢失中间状态。
- 没有事件返回 `channelDifferenceEmpty{final=true, pts=current_channel_pts, timeout=30}`。
- 事件数 `<= cap` 返回 `channelDifference{final=true, pts=max_pts, new_messages, other_updates, chats, users}`。
- 如果当前 member 的 `available_min_pts > req.pts`，先把请求 pts 抬到 `available_min_pts`，从源头跳过入群/重新加入前的消息类 durable 事件。
- 对 `available_min_id` 之后才可见的成员，普通差量仍扫描 durable log 并推进返回 `pts`，但会过滤 `new/edit/delete/pin` 中 `message_id <= available_min_id` 的消息内容和 id；若本页全被过滤，返回 `channelDifferenceEmpty{pts=max_scanned_pts}`，避免隐藏历史或本地清空后的旧消息通过差量恢复泄露。部分可见的 delete/pin 事件只裁剪 `messages` 向量，保留原始 `pts_count`；TDesktop 在线 update 用 `pts_count` 推进 channel PTS，差量响应最终用 `channelDifference.pts` 初始化，不要求 `len(messages)==pts_count`。
- 若 `current_channel_pts - req.pts > cap`，返回 `channelDifferenceTooLong`，包含带当前 channel pts 的 dialog、最新一页有界消息、channel、相关 users，避免大频道旧 pts 客户端循环拉取大量差量页。
- 否则事件数 `<= cap` 返回 `channelDifference{final=true, pts=max_pts, new_messages, other_updates, chats, users}`；事件数达到 cap 但仍未追上当前 pts 时返回 `final=false`，客户端会继续拉下一页。
- `new_messages/other_updates` 中 message 的 `from/send_as/fwd_from/reply_to/action` 涉及的 user/channel 必须随本页 `users/chats` 返回；账号级 `user_update_events` 同样持久带出 fwd/reply 的 users/channels，供 `updates.getDifference` 和 `dispatch_outbox` 复用，避免在线响应正常但离线恢复缺 peer。

## Input Hash And Visibility

- 外部 `InputChannel` / `InputPeerChannel` 带非零 `access_hash` 时，RPC 层必须先和当前 domain channel 的 `access_hash` 比对；不匹配返回 `CHANNEL_PRIVATE` 或在 vector 查询中跳过该项，禁止只靠 channel_id + member 关系兜底。
- 这个校验不仅覆盖 `channels.*` 和 `messages.send/edit/delete/read/history/search` 主路径，也覆盖 `messages.getPeerSettings`、`messages.saveDraft`、`messages.getDialogs` offset peer、dialog pin/unread/folder、`messages.updateDialogFilter`、`folders.editPeerFolders`、reply header 的 `reply_to_peer_id` 等旁路入口。
- `InputChannelFromMessage` / `InputPeerChannelFromMessage` 属于 min channel 解析路径，没有 access_hash 字段；服务端只接受正 channel_id，并继续执行成员/可见性校验。
- legacy basic group wrapper 由服务端内部映射到 megagroup，内部构造的 `InputChannel{access_hash=0}` 允许跳过 hash 比对；真实客户端传来的非零 hash 一律校验。

硬约束：

- `updateNewChannelMessage.PtsCount=1`。
- `updateEditChannelMessage.PtsCount=1`。
- `updateDeleteChannelMessages.PtsCount=len(message_ids)`，单次 cap 1000。
- 全清历史不能生成十几万条 update，也不能一个超大 vector；`messages.deleteHistory(InputPeerChannel)` 返回 `affectedHistory.offset` 供客户端/管理端续删，`channels.deleteHistory` 的 TL 返回 `Updates` 且 TDesktop 不读取 offset，所以该入口只执行一个有界 page 或本地 `available_min_id` 清空，不能在同步 RPC 内循环展开全量历史。
- 所有 history/search 入口的 `add_offset` 必须在 RPC/store 层 clamp 到小窗口；即使 channel 单份历史当前按 seek 查询忽略大 offset，也不能让私聊或后续 channel around-load 分支把客户端传入的极端值变成无界 slice/SQL OFFSET。
- `channels.deleteParticipantHistory` 同样只删除一个有界 page，按 `(channel_id, sender_user_id, id DESC)` seek 最近消息，`offset=1` 表示后面仍可能有更多该成员历史需要客户端/管理端继续请求。

## Admin Log

`channels.getAdminLog` 是管理页的只读审计视图，不推进 channel pts，也不写入普通用户 update。当前真实记录这些 TDesktop 首批能展示的事件：

- 元信息/设置：title、username、signatures、prehistory、slowmode、anti-spam。
- 成员权限：invite、join、leave、promote/demote、ban/unban、kick/unkick。
- 消息管理：pin/unpin、channel post/send、edit、delete。

请求边界：

- `limit` cap 100；`admins` cap 100；`q` cap 128。
- `max_id`/`min_id` 只作为 seek 条件：`id < max_id`、`id > min_id`，禁止展开区间数组。
- `q` 只做有界页面搜索，覆盖元信息字符串与消息类事件 body；后续若管理日志量级增大，再接 PG full-text/trigram 索引。
- `events_filter` 空表示不过滤；非空只映射当前支持的 action types，未来 forum/group_call/subscription 等先保持无结果而不是伪造。
- `admins` 过滤 actor_user_id，不按受影响成员过滤；TDesktop 侧该参数语义就是管理员 actor 列表。

返回 `channels.adminLogResults` 时，RPC 层按事件收集 actor、participant、message sender，填充 `users`；`chats` 返回当前 channel。domain/store 层只保存自有模型，TL action 转换集中在 `internal/rpc`。

当前实现状态：

- 已实现 `messages.createChat -> megagroup`、`channels.createChannel`、成员 invite/join/leave、channel 单份文本发送、history、dialogs、read、typing 与 `updates.getChannelDifference`；`channels.createChannel` 的 history import/geogroup 高级 flags 暂不建模，已映射为显式 TL 错误而非 `NOT_IMPLEMENTED`。
- 已实现 channel 文本 edit：只更新 `channel_messages` 单份消息，写 `channel_update_events(edit_channel_message)`，在线推 `updateEditChannelMessage`；TDesktop 文本编辑携带的 `inputMediaWebPage/inputMediaEmpty` 降级为文本编辑，真实 media/reply_markup/quick replies 仍返回显式 TL 错误等待后续模型。
- 已实现 channel delete：`channels.deleteMessages` 与 `channels.deleteHistory(for_everyone)` 软删单份消息，单批最多 1000 个 id，写一条 `delete_channel_messages` 事件，`pts_count=len(ids)`；需要多页续删的管理路径应使用 `messages.deleteHistory(InputPeerChannel)` / `channels.deleteParticipantHistory` 这类带 `offset` 的响应继续推进，避免复现 参考实现 按超大范围构造 id vector 的 OOM 风险。
- 已实现 `channels.deleteParticipantHistory`：管理员按 participant sender 删除一页消息，PG 走 sender history 部分索引，单批最多 1000 个 id，返回 `affectedHistory.offset` 供续删，在线推 `updateDeleteChannelMessages`。
- 已实现当前用户本地清空 channel history：只推进该用户 `available_min_id/read_inbox` 与 `channel_dialogs`，不生成 channel pts，也不对成员写扩散；同时写账号级 `channel_available_messages` durable update，并给同账号其它 session 推 `updateChannelAvailableMessages`；重复或 stale 清空请求返回实际单调水位，不能把客户端 `available_min_id` 回退。
- 已实现 channel reply：发送时校验被回复消息是同 channel 内当前成员可见消息，继承/计算 `reply_to_top_id`，保留 quote metadata，并在 TL 层返回 `messageReplyHeader`；非法、本地清历史后不可见目标或超界 `quote_offset` 返回 `REPLY_MESSAGE_ID_INVALID`。
- 阶段外高级发送/转发 flags 不再返回 `NOT_IMPLEMENTED`：quick reply、effect、paid、suggested post、monoforum/todo/poll/story reply 分别映射到 `SHORTCUT_INVALID`、`EFFECT_ID_INVALID`、`PAYMENT_UNSUPPORTED`/`STARS_AMOUNT_INVALID`、`SUGGESTED_POST_PEER_INVALID`、`REPLY_TO_MONOFORUM_PEER_INVALID`、`REPLY_MESSAGE_ID_INVALID`、`POLL_OPTION_INVALID`、`STORY_ID_INVALID`；后续接入对应模型前不伪造成功 update。
- 已实现 linked discussion/comment 基础闭环：broadcast post 写入单份 source message 时同步在 linked megagroup 写一条 forwarded root message；source message 保存 discussion ref 并在 `messageReplies` 填 `comments/channel_id/replies_pts/max_id/read_max_id`；`messages.getDiscussionMessage` 返回 root、`messages.getReplies` 读 linked group thread，`messages.readDiscussion` 推进 linked group read watermark。
- 已实现文本转发的 channel 路径：channel→channel、channel→user、user→channel；目的 channel 仍按单份消息写入并生成 channel pts；请求携带的目标会话 `reply_to` 会正常校验和持久化，TDesktop 转发到 forum topic 时发送的 `top_msg_id` 会映射为 topic-only reply 并更新 topic top message，但源消息自身 reply 不继承；RPC 响应、账号级 difference/outbox 与 channel difference 都会为 `fwd_from/reply_to` 中可解析的 user/channel peer 补齐 users/chats，避免 TDesktop 因 header peer 未加载而延迟 apply。
- 已实现 `messages.search(InputPeerChannel)`：支持 channel/supergroup 单份消息文本搜索、`from_id` 用户过滤、`min_date/max_date`、`offset_id/max_id/min_id` 与 limit cap，结果附带当前 channel 与消息 sender/forward/reply 所需 users/chats。
- 已实现 `messages.searchGlobal` 的 channel/supergroup 分支：TDesktop 主搜索的 Channels/Groups tab 会按当前账号 active membership 搜索单份 channel message，支持 `broadcasts_only/groups_only/users_only`、`offset_rate+offset_peer+offset_id` seek、folder_id=0/1 下推与 limit cap=50；未加入/left/kicked/view_messages banned 的频道不暴露，media filter 在 media store 接入前返回空结果。
- 已实现 `messages.getMessageReadParticipants(InputPeerChannel)`：基于小 megagroup 成员读水位返回 `readParticipantDate`，含 50 人阈值、7 天过期窗口、`available_min_id` 可见性过滤与 PG 索引。
- 已实现 `messages.getMessageEditData`：私聊与 channel peer 都做 message/peer/作者或管理员编辑权限校验；当前文本-only 编辑没有媒体 caption 状态，返回 `caption=false`。
- 已实现 `messages.getMessagesViews(InputPeerChannel)`：TDesktop 每秒最多 100 条批量增量，服务端按 `(channel_id,message_id,viewer_user_id)` 持久去重并维护 `channel_messages.views_count` 聚合列；本地清历史前不可见、已删除或不存在的 id 不递增且返回空 view，replies/comment 信息继续从 discussion/thread model 回填。
- 已实现 `channels.exportMessageLink`：复制频道/超级群消息链接前会校验 channel message 对当前成员真实存在且未被删除/本地清历史隐藏；公开 username 走 `t.me/{username}/{msg_id}`，私有 channel 走 `t.me/c/{channel_id}/{msg_id}`，普通 reply/thread 链接支持 `?thread={root_id}`。`grouped/html` 与 linked discussion 的 `?comment=` 细分链接留后续。
- 已实现管理面最小真实能力：`channels.editAdmin/editBanned/editTitle/deleteChannel`、`channels.getParticipants` 的 admins/kicked/banned/search 等过滤、`messages.updatePinnedMessage/unpinAllMessages`、`messages.exportChatInvite/checkChatInvite/importChatInvite`。`channels.deleteChannel` 按 TDesktop/参考实现预期返回并推送 `updateChannel + channelForbidden`，同时 dialog 列表过滤 deleted channel。`channels.editPhoto/messages.editChatPhoto` 在头像 media store 接入前只接受 `inputChatPhotoEmpty` no-op 删除，uploaded/existing photo 明确返回 `PHOTO_INVALID`，不伪造 `messageActionChatEditPhoto`。
- `channels.editAdmin/editBanned` 只更新成员状态/计数并写 admin log，不占 channel pts、不写 `channel_update_events`；在线响应/推送包含 `updateChannelParticipant + updateChannel`。离线客户端通过 `channels.getFullChannel/getParticipants/getParticipant` 或后续可见消息触发的 channel state 刷新补偿；如果操作另行产生可见 service message，则由该 service message 进入 channel pts。
- 已实现公开 username 管理：`channels.checkUsername/updateUsername/getAdminedPublicChannels`，PG 用 `channel_usernames(username_lower)` 与 users username 查询避免跨 peer 占用；主 username 的清除只走 `channels.updateUsername("")`。
- 已实现 `channels.toggleSignatures`：权限校验后持久化 `channels.signatures`，返回/在线推 `updateChannel`。
- 已实现 `channels.updateColor/updateEmojiStatus`：颜色分别持久化 `color/profile_color` 与 background emoji id，并保留 color flag 显式 0；普通 emoji status 保存 document id/until，`emojiStatusEmpty` 清空，collectible gift 状态因缺少 gift/read model 先返回 `EMOJI_STATUS_INVALID`。响应、在线推送、`channels.getChannels` 都回填 `Channel.color/profile_color/emoji_status`。
- 已实现 `channels.toggleViewForumAsMessages`：这是当前账号本地论坛展示模式，不修改 topic 列表；服务端写 `channel_dialogs.view_forum_as_messages`，返回/可靠投递 `updateChannelViewForumAsMessages` 给同账号其它 session，并在 `messages.getDialogs` 与 `channels.getFullChannel` 回填 `Dialog/ChannelFull.view_forum_as_messages`。
- 已实现 forum topic 最小真实闭环：`channels.toggleForum` 打开 forum 后，`messages.getForumTopics/getForumTopicsByID` 返回虚拟 General + `channel_forum_topics` 分区表里的 topic；`messages.createForumTopic` 写 `messageActionTopicCreate` root service message，topic_id 绑定该 message id，并用 channel pts 通过 `updateNewChannelMessage` 在线推送。TDesktop 的 topic 输入框 `reply_to_msg_id=0/top_msg_id=topicRootId` 已支持：服务端校验 topic 存在且未关闭/隐藏，消息只写一份 `channel_messages`，保存 `reply_to_top_id` 和 `forum_topic` 标志，`messages.getReplies(topicRoot)` 返回 `messages.channelMessages{pts,topics}` 且 topic 用 short constructor；发送 topic 消息会更新 topic top message，`messages.forwardMessages.top_msg_id` 也走同一 topic-only reply 语义，避免转发到主题落到主会话。`messages.editForumTopic` 写 `messageActionTopicEdit` service message 且 reply_to_top_id 指向 topic root；`messages.updatePinnedForumTopic/reorderPinnedForumTopics` 更新 topic pinned/order 并推送对应 TL update；`messages.deleteTopicHistory` 每页最多删 `MaxDeleteHistoryBatch` 条 root/thread message，通过 `affectedHistory.offset` 续删，最后一页才隐藏 topic。topic page 走 limit cap=100 与 seek 条件，不按客户端超大 offset/id 展开数组。
- 已实现 `channels.toggleAntiSpam`：服务端写 `channels.antispam`，返回/推送 `updateChannel`，在 `channels.getFullChannel` 回填 `ChannelFull.antispam`，并记录 `channelAdminLogEventActionToggleAntiSpam`；真实 native anti-spam bot 和自动删垃圾消息管线后续单独接入。
- 已实现 `channels.getSendAs` + `messages.saveDefaultSendAs` 的保守 current-channel 链路，并让 `messages.sendMessage/forwardMessages(InputPeerChannel)` 接受同一套 `send_as` 校验结果：列表始终包含 self，只有 creator、broadcast post admin、megagroup anonymous admin 才追加 current channel，且响应必须带 current channel chat + self user 供 TDesktop 解析；不暴露 public channel 扩展候选。`channels.getSendAs` handler 在一次 `GetChannel` 中完成 access_hash 校验、成员读取与候选构造，避免 TDesktop 打开输入框时重复查同一 channel。保存 current channel 会写入 `channel_dialogs` 并在 `channelFull.default_send_as` 恢复；保存 self 清空默认身份。发送/转发未显式带 `send_as` 时会读取已保存默认身份并重新校验，默认值因权限变化失效时自动降级 self，避免陈旧状态阻塞发送。
- 已实现 `messages.toggleNoForwards`：legacy chat/channel 内容保护入口持久化 `channels.noforwards`，返回/在线推 `updateChannel`；后续 channel/supergroup 单份消息写入时自动继承 noforwards，避免转发受保护内容。
- 已注册 `messages.setChatTheme`，并实现 `messages.setChatAvailableReactions`：前者对 private peer 返回空 updates、对 legacy chat/channel 返回 channel context；后者参考实现 的 `available_reactions` 存储语义，用 domain policy + PG JSONB 持久化，`channels.getFullChannel` 返回 `available_reactions/reactions_limit/paid_reactions_available`，并对 reaction vector/reactions_limit 做 cap。
- 已实现 private + channel/supergroup emoji message reaction 最小闭环：`messages.getAvailableReactions` 优先返回 seed 自真实导出的 reaction documents；`messages.sendReaction` 对 private peer 写 `private_message_reactions`，按共享 `private_message_id` 聚合但以各 owner 的 peer/msg_id 构造 `updateMessageReactions`，并写账号级 `message_reactions` durable event，离线设备经 `updates.getDifference` 收到带最新 `message.reactions` 的 message + `updateMessageReactions`；对 channel/supergroup 写 `channel_message_reactions`，替换或清除当前用户对单条 channel message 的 emoji reaction，返回并在线推 update，离线/重开由 `messages.getHistory` / `updates.getChannelDifference` / `messages.getMessagesReactions` 补偿；channel `sendReaction` 每次还累计账号级 `user_top_reactions`，`getTopReactions` 按使用次数排序并优先用真实 available reaction catalog 有界补齐；`add_to_recent` 会写账号级 `user_recent_reactions`，`messages.getRecentReactions/clearRecentReactions` 按 hash 有界返回/清空最近 emoji reaction；`messages.updateSavedReactionTag` 会写账号级 `user_saved_reaction_tags` 并推 `updateSavedReactionTags` 给同账号其它 session，`getSavedReactionTags` 全局请求按 hash 返回标题列表；channel 反应者不是消息作者时标记 unread 并重算 `channel_dialogs.unread_reactions_count`，`messages.getUnreadReactions/readReactions` 按消息作者视角拉取/清理未读 reaction；TDesktop 打开会话时的 `channels.readMessageContents` 会按 visible message ids 清理这些 unread reaction 并推 `updateMessageReactions`，避免 dialog reaction icon 重启后重新出现；`messages.getMessagesReactions` 按请求 id 返回 reaction 聚合清理/刷新 UI，`messages.getMessageReactionsList` 返回具体用户列表（channel 用 keyset offset，private 当前受 recent 嵌入上限约束）。custom emoji、paid reaction、saved-message tag assignment/count/per-peer ranking 留后续模型。
- 已实现 `channels.togglePreHistoryHidden/toggleSlowMode`：prehistory 仅 creator 可改，并通过 `available_min_id/read_inbox_max_id` 裁剪新成员历史，同时用 `available_min_pts` 裁剪新成员 difference 起点；slowmode 允许 change_info 管理员改；PG 持久化 `pre_history_hidden/slowmode_seconds/slowmode_last_send_date`，TDesktop 可从 channel/full channel 看到状态，普通成员过快发言返回 `SLOWMODE_WAIT_X`。
- 已实现 `channels.toggleParticipantsHidden`：creator/具备 `ban_users` 的管理员可切换 `channels.participants_hidden`，返回/推送 `updateChannel`；TDesktop 通过 `ChannelFull.participants_hidden` 恢复隐藏成员 UI，普通成员的 participants 列表只保留 aggregate count，read participants 返回空，避免用已读详情绕过隐藏成员设置。
- 已实现 `channels.getAdminLog`：持久化 channel-scoped admin log，支持 actor/filter/query/max_id/min_id/limit 的有界查询，并返回真实 TL admin log actions；当前覆盖 metadata、成员权限、pin、send/edit/delete，forum/group_call/subscription 等长尾 action 留待对应业务模型。
- 已注册 `stats.*` 统计域兼容入口：TDesktop 统计页会调用 `stats.getBroadcastStats/getMegagroupStats/getMessageStats/getMessagePublicForwards/loadAsyncGraph`，当前先做 access_hash、admin、类型、msg_id、limit/offset 边界校验并返回可解析空图表或空 public forwards；真实聚合表、预计算窗口和异步 graph token 后续单独设计，避免在首版频道模块里引入无界统计扫描。
- 已注册 `premium.*` boost 域兼容入口：TDesktop 频道统计页、颜色/权限入口和 boost 弹窗会调用 `premium.getBoostsStatus/getBoostsList/getMyBoosts/applyBoost/getUserBoosts`；当前仅做 channel/access_hash/admin/user/limit/offset/slots 边界校验并返回零状态或空列表，不落库、不生成虚假 boost 关系。真实 boost slots、giveaway、premium audience 聚合后续独立建模。
- 已实现 invite management：TDesktop 管理 invite links、admins with invites、importers、join requests 时会调用 `messages.getExportedChatInvites/getExportedChatInvite/editExportedChatInvite/deleteExportedChatInvite/deleteRevokedExportedChatInvites/getAdminsWithInvites/getChatInviteImporters/hideChatJoinRequest/hideAllChatJoinRequests`；当前已持久化 invite 列表、detail/edit/revoke/delete、按 admin 统计、importer/read model、`request_needed` pending join request、单个 approve/dismiss 与 bounded `hideAll`。`getExportedChatInvites` 使用 `offset_date + offset_link` seek，`getChatInviteImporters` limit cap=100，`hideAll` 单批最多 1000，避免按客户端超大参数生成无界更新；subscription/chatlist/paid invite 与 join-request service notification 仍留后续。
- 已实现 public join settings：`channels.toggleJoinToSend`/`channels.toggleJoinRequest` 持久化 `channels.join_to_send/join_request` 并返回带 flags 28/29 的 `tg.Channel`；`join_request` 仅 public megagroup 可开启，非成员 `channels.joinChannel` 会写入 `channel_invite_importers(invite_id=0, requested=true)` 并返回 `INVITE_REQUEST_SENT`，之后可通过 `messages.getChatInviteImporters(requested=true)` 查询和 `messages.hideChatJoinRequest` approve/dismiss。admin 侧 `channels.getFullChannel` 会回填 `requests_pending/recent_requesters`，request-needed import/public join 以及 approve/dismiss 会向有界管理员集合推 `updatePendingJoinRequests`；该状态不写入无界 durable update log，离线管理员重新打开 full channel 时补偿。
- 已修正当前普通成员的 `channels.getParticipant(inputPeerSelf)` 与 participants 列表 TL constructor：TDesktop `requestSelf` 期望普通本人是 `channelParticipantSelf`，creator/admin 仍分别返回 creator/admin self 语义；避免客户端记录 `Got self regular participant`，且不改变 domain/store 成员模型。
- PG channel pts 已补失败保护：现有 channel 的 send/edit/deleteHistory/deleteMessages/pin 和加入/退出/标题等可见 service message 在事务失败或权限失败后写 `noop` 占位，避免 Redis 分配过的 channel pts 形成 TDesktop `PtsWaiter` 永久 gap。
- 2026-06-01 双 TDesktop 在线/离线实测已覆盖超级群发送、reply、forward、edit 与离线恢复：Bob 对 Alice 消息 reply，Bob 将回复 forward 回同一超级群，随后编辑该频道消息；Bob 关闭期间 Alice 发送 channel 消息，Bob 重启后 dialog 未读数=1，打开群后看到离线消息；Alice/Bob 双窗口均实时显示，server 日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg` / panic。删除、清历史、踢/禁言仍需用户行动时确认后做 UI 实测。
- 2026-06-02 Computer Use 双 TDesktop 复测已覆盖当前非破坏性频道/超级群 UI 路径：Alice/Bob 在 `E2E Super 0307` 中双向发送 `cu-round-alice-*` / `cu-round-bob-*` 并实时互见，成员栏显示 2 members/online，Alice 全局搜索 Bob 的新消息返回 `Found 1 message`；Alice 在 `CU Public Search 44238` 频道发布 `cu-channel-round-*` 后频道消息流和左侧 preview 同步更新。server 日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg` / panic，客户端本轮无新增 `Bad participant` / `Got self regular participant`；清空搜索框产生的 `SEARCH_QUERY_EMPTY` 保持可解释。
- 2026-06-02 09:55 Computer Use reaction/sticker 启动复测：Debug/Alice 与 DebugBob/Bob 同时打开 `E2E Super 0307`，互发 `cu-stubfix-alice-*` / `cu-stubfix-bob-*` 后双方消息列表和左侧 preview 均可见；打开 emoji 面板触发 `messages.getAvailableReactions` / `messages.getStickerSet` / `messages.getAvailableEffects`，server 日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg` / panic，Debug 当前 `log.txt` 无新增 `Unexpected messages.stickerSetNotModified` / participant 告警。右键消息菜单可打开但本轮未显示 reaction 快捷项，真实 reaction sticker animations/custom UI 仍留后续。
- 2026-06-02 10:10 Computer Use 主干路径复测：Debug/Alice 与 DebugBob/Bob 在 `E2E Super 0307` 中双向在线发送 `cu-continue-*`，消息流与左侧 preview 双端可见，未读通过 `channels.readHistory` 清除；Alice 在 `CU Public Search 44238` 发布 `cu-channel-continue-*`，频道消息流和左侧 preview 同步更新。server 日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg` / panic，Debug 当前日志无新增 API error；DebugBob 主日志仅有 08:05 前旧 sticker/participant 噪声。
- 2026-06-02 10:49 Computer Use unread reaction 回归冒烟：本轮 server 启动后自动应用 migration 0051，Debug/Alice 与 DebugBob/Bob 同时打开 `E2E Super 0307`；Alice 发送 `cu-unreadrx-alice-1780368577183` 后 Bob 左侧 preview 出现新消息/未读 badge，Bob 发送 `cu-super-bob-1780356886095` 后 Alice 消息流实时显示。server 日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg` / panic，Debug/DebugBob 最新 `DebugLogs/log_10_45.txt` 无新增 API error/Unexpected；当前 TDesktop reaction 快捷入口仍未稳定显示，未读 reaction 语义由 `internal/rpc` router 测试覆盖。
- 2026-06-02 11:20 Computer Use recent reactions 相关冒烟：本轮 server 启动后自动应用 migration 0052，Debug/Alice 与 DebugBob/Bob 同时打开 `E2E Super 0307`；Bob 发送 `cu-recent-bob-1780370400982` 后双方消息流和左侧 preview 可见，Bob 打开 emoji 面板后 Emoji/Stickers/GIFs 面板正常渲染。server 日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg` / panic，Debug/DebugBob 最新日志无新增 API error/Unexpected；TDesktop 本轮未重新请求 `messages.getRecentReactions`，recent get/clear 的 hash/notModified/clear 后空列表语义由 `internal/rpc` router 测试覆盖。
- 2026-06-02 11:36 Computer Use top reactions 回归冒烟：本轮 server 启动后自动应用 migration 0053，Debug/Alice 与 DebugBob/Bob 同时打开 `E2E Super 0307`；Bob 发送 `cu-toprx-bob-1780371301984` 后双方消息流和左侧 preview 可见，Bob/Alice emoji 面板正常渲染，Bob 右键最新消息菜单可打开。server 日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg` / panic，Debug 当前日志无新增 API error，DebugBob 仅有 08:05 前旧 sticker/participant 噪声；本轮 TDesktop 没重新请求 `messages.getTopReactions`，top 排序、静态 catalog 兜底、hash/notModified 语义由 `internal/rpc` router 测试覆盖。
- 2026-06-02 12:23 Computer Use saved reaction tag title 回归冒烟：本轮 server 启动后自动应用 migration 0054，Debug/Alice 与 DebugBob/Bob 同时打开 `E2E Super 0307`；Bob 发送 `cu-savedtags-final-bob-1780374193558` 后另一客户端消息流可见并触发 channel difference。server 日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg` / panic/error；本轮 TDesktop 未直接触发 `messages.getSavedReactionTags/updateSavedReactionTag`，账号级标题持久化、hash/notModified 与 `updateSavedReactionTags` 推送语义由 `internal/rpc` router 测试覆盖。
- 2026-06-03 unread reaction / 打开历史卡顿回归：Alice Debug 中 `AAAA` 的 unread reaction 由 Bob 对 Alice 自己消息的 reaction 产生；修复后 `channels.readMessageContents` 按 visible message ids 清理该作者视角 unread reaction 并回推 `updateMessageReactions`。Computer Use 验证 Alice 点开 `AAAA` 后 PG `channel_dialogs.unread_reactions_count=0`、`channel_message_reactions.unread=false`；重启 Alice 后 `AAAA` 列表无红色 unread reaction 图标；Bob DebugBob 同时启动并可打开 `AAAA`。同轮修复 `channels.getSendAs` 内重复 `GetChannel`，打开历史时 `messages.getHistory` 为十毫秒级，日志无新增 `NOT_IMPLEMENTED` / `Unhandled RPC` / `bad_msg`。
- `channels.editPhoto/setStickers/setEmojiStickers/reorderUsernames/toggleUsername/deactivateAllUsernames` 仍为兼容 stub：头像删除只做 no-op channel state 响应，真实上传/已有照片返回 `PHOTO_INVALID`；群贴纸/custom emoji 只允许 megagroup `inputStickerSetEmpty` no-op 清空，非空 sticker set 返回 `STICKERSET_INVALID`；Fragment/多 username 入口不修改主 username，避免数据破坏。
- Layer 225 `OnChannels*` dispatcher 与 legacy chat/channel 入口已全部显式注册；未进入首批真实业务的入口均为参数有界、权限校验或空结果 stub，避免 TDesktop 实测出现 `NOT_IMPLEMENTED`/unknown RPC。

## Permissions

首批权限模型：

- creator：全权限。
- admin：按 `AdminRights` 判断 invite/delete/edit/bans/change_info/post_messages。
- member：megagroup 可发消息，broadcast 默认不可发。
- left/kicked/banned：不可读写；banned rights 可限制 `send_messages/view_messages/invite_users/pin_messages/change_info`。
- default banned rights：channel 级默认禁言，member 无显式权限时继承。

TDesktop 最小入口：

- `channels.getFullChannel` 必须返回当前用户 admin/banned/default rights、participants/read/unread/notify/invite link/pts。
- `channels.getParticipants` 支持 recent/admins/search/kicked/banned/bots/contacts/mentions 的最小分页，limit cap 200；banned/kicked 只向 admin 暴露。
- `channels.editAdmin/editBanned/editTitle/deleteChannel/updateUsername/toggleSignatures/togglePreHistoryHidden/toggleParticipantsHidden/toggleForum/toggleAntiSpam/toggleSlowMode/updateColor/updateEmojiStatus/toggleViewForumAsMessages/getAdminLog/exportMessageLink` 已有真实权限/可见性校验；`channels.editPhoto/setStickers/setEmojiStickers/reorderUsernames/toggleUsername/deactivateAllUsernames` 暂为兼容 stub，后续接 files/Fragment username/stickers/custom-emoji/boost gating 等模型。

## Online Push

在线推送分两类：

- user 维度：dialog/folder/pin/notify/read state 等 owner 状态，继续写 `user_update_events + dispatch_outbox`。
- channel 维度：消息/edit/delete/pin/member service message，写 `channel_update_events`，在线 fanout 给 active channel sessions。

为避免大频道 O(n) 离线写扩散：

- 不把每条 channel message 写入所有成员的 `dispatch_outbox`。
- 参考实现 的 status online sessions / `MutableChannelOnlinePush` 边界，当前实现先从 `SessionManager.OnlineUserIDs` 取有界在线 user 快照，再用 `channel_members(channel_id,user_id)` 过滤 active member，最后合并操作显式 recipient（例如 leave/kick 后的本人）。这样避免“大频道取前 500 个成员却漏掉真实在线用户”，也避免每条消息按全体成员写扩散；后续可升级为 `channel_id -> active sessions` 订阅索引。
- `messages.setTyping(InputPeerChannel)` 是纯瞬时在线推送：TDesktop topic 输入框带来的 `top_msg_id` 只在 `updateChannelUserTyping` 中透传给在线成员，先做 `0..MaxMessageBoxID` 边界校验，非法返回 `MSG_ID_INVALID`，避免异常 topic id 扩散到其它 session。
- 离线成员不接 outbox；重新打开会话或 gap 时通过 `updates.getChannelDifference` 补齐。

## Safety / Performance

- 所有 list/id/vector 参数必须 cap：participants limit 200、offset 最大 10000、participants/admin-log 搜索 q 最大 128 字符、history/search/searchPosts q 最大 256 字符、searchPosts limit 50、paid message stars 10000、boost unrestrict 0..8、history 100、get ids 100、delete ids 1000、forward ids 100、channel difference 100。
- 所有实时 fanout 必须有硬上限：首版 `MaxChannelRealtimeFanout=500`，不能因为一个大频道消息或 typing 动作把全体成员读入内存。
- channel history/search 只返回有界页；PG 用 `limit+1` 判断是否还有下一页，不为 UI 计数执行 `count(*) over()` 或全量 count。
- 禁止按客户端传入 `int64 max/max_id` 构造数组；deleteHistory 必须批量 seek。
- `channel_messages` 和 `channel_update_events` 查询必须带 `channel_id` 分区键。
- `channel_dialogs` 查询必须带 `user_id` 分区键。
- 从 user 入口访问 channel membership 时不得直接扫 `channel_members WHERE user_id=...`；该表按 `channel_id` 分区，实测会展开 64 个分区。必须走 `user_channel_member_index`，或先取 bounded channel_id 列表再用 `channel_id/id = ANY($1)` 二段查询。
- 对 `channels` / `channel_members` 这类分区表，SQL 内动态 join key 不能保证分区裁剪；`index -> partitioned table` 单条 join 实测仍会展开全部分区。
- 单条消息文本沿用 4096 code point 限制，entities cap 256。
- 大频道 unread 不能在应用层逐成员循环；当前兼容实现只允许数据库 set-based 缓存推进并保留 read watermark 可重算路径，后续高规模频道需拆后台 fanout 或更强惰性缓存。
- 所有 channel pts 分配失败/事务失败要有 noop gap 补偿与指标。
- 压测目标：单机 1000 msg/s megagroup send p99 < 150ms；`getChannelDifference` p99 < 100ms；10 万成员频道单条 broadcast 不产生 O(n) 数据库写。

## Implementation Plan

1. Domain/store：补 `PeerTypeChannel`、channel/member/message/dialog/update DTO 与 store interfaces。
2. Migration：新增分区表、索引、Redis allocator source。
3. App：新增 `internal/app/channels`，封装创建、成员、发送、history、read、difference。
4. RPC：注册 `channels.*`、`messages.createChat`、`updates.getChannelDifference`，给 messages 现有 RPC 增加 channel peer 分支。
5. TL 转换：只在 `internal/rpc` 实现 domain ↔ `tg.Channel/ChannelFull/Message/UpdateNewChannelMessage`。
6. Online push：新增 channel active fanout，user dialog 状态继续用 reliable outbox。
7. Tests：domain/store/app/rpc 单元测试，PG/Redis 集成测试覆盖 pts 连续、差量、deleteHistory 分批、权限。
8. TDesktop：双端/多端验证创建超级群、频道、邀请、退出、在线/离线消息、history、read、edit/delete、forward/reply、typing、dialog/filter/归档展示。

## 媒体与频道头像（2026-06-02）

- `channel_messages` 增 `media` JSONB 快照列（与私聊同构），`SendChannelMessage` 透传 `req.Media`，讨论组联动消息一并带 media；`scanChannel*`/`channelMessageColumns` 统一加 `media::text`，所有 history/getMessages/replies/difference 读取路径自动带出。`tgChannelMessage` 在 media 非空时 `SetMedia`。放宽 `channel_messages` content CHECK 为 `body<>'' OR action<>'{}' OR media<>'{}'`。
- 频道头像：`channels` 表反范式 `photo_id/photo_dc_id/photo_stripped`（migration `0059`），`channelColumns` + 全部 5 处 channel scanner 同步；`channels.editPhoto`/`messages.editChatPhoto` 经 `resolveInputChatPhoto` 上传或引用照片，admin（change_info）校验后落列并返回 `updateChannel` + 推 channel state；`tgChannel.Photo`(ChatPhoto)/`tgChannelFull.ChatPhoto` 渲染真实头像（`getFile` 按 `photo:<id>:<type>` 解析忽略 access_hash，合成 a/c 尺寸即可下载）。
- 2026-06-03 接手审计修正：`SendChannelMessage` 的空内容校验已把 `req.Media` 纳入，允许超级群/频道发送无 caption 的 photo/document/sticker；新增 PG 集成测试覆盖 channel media 经 `ListChannelDifference` 恢复，防止离线 TDesktop 拉差分丢媒体。
- 范围外：in-history `MessageActionChatEditPhoto` service 消息留 todo。
