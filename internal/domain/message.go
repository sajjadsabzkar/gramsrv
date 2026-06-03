package domain

// MessageEntityType 标识消息实体类型。
type MessageEntityType string

const (
	MessageEntityBold MessageEntityType = "bold"
)

const (
	// MaxMessageTextLength matches the first-stage text message limit exposed to Telegram clients.
	MaxMessageTextLength = 4096
	// MaxMessageReplyQuoteLength matches TDesktop's quote_length_max app config default.
	MaxMessageReplyQuoteLength = 1024
	// MaxMessageReplyQuoteOffset bounds quote_offset, which is an offset inside message text, not a message id.
	MaxMessageReplyQuoteOffset = MaxMessageTextLength
	// MaxMessageEntityCount limits styled text entity vectors in message text and quotes.
	MaxMessageEntityCount = 256
	// MaxMessageBoxID 是 TL int / PostgreSQL int4 可安全表达的最大 message id。
	MaxMessageBoxID = 1<<31 - 1
	// MaxDeleteMessageIDs 限制单次 deleteMessages/updateDeleteMessages 的 owner 视角 id 数量。
	// 大批量历史清理走 deleteHistory 分批推进，避免单个 RPC 构造超大数组或 durable payload。
	MaxDeleteMessageIDs = 1000
	// MaxGetMessageIDs 限制 getMessages / channels.getMessages 精确 ID 批量。
	MaxGetMessageIDs = 100
	// MaxDeleteHistoryBatch 限制单次 deleteHistory 实际清理的 message box 数量。
	// affectedHistory.Offset > 0 时客户端可继续调用，服务端不一次性 RETURNING 全历史。
	MaxDeleteHistoryBatch = 1000
	// MaxForwardMessageIDs 限制单次 forwardMessages 的 owner 视角 id 数量。
	MaxForwardMessageIDs = 100
	// MaxMessageHistoryAddOffset 限制 history/search 的 add_offset 绝对值。
	// TDesktop 正常只使用小窗口偏移；服务端必须拒绝把客户端传入的超大值变成 SQL OFFSET 或 slice capacity。
	MaxMessageHistoryAddOffset = 100
)

// ClampMessageHistoryAddOffset bounds Telegram history/search add_offset to a small local window.
func ClampMessageHistoryAddOffset(v int) int {
	if v > MaxMessageHistoryAddOffset {
		return MaxMessageHistoryAddOffset
	}
	if v < -MaxMessageHistoryAddOffset {
		return -MaxMessageHistoryAddOffset
	}
	return v
}

// ValidateMessageReplyBounds validates reply fields that are independent of peer visibility.
func ValidateMessageReplyBounds(reply *MessageReply) error {
	if reply == nil {
		return nil
	}
	if reply.MessageID < 0 || reply.MessageID > MaxMessageBoxID {
		return ErrReplyMessageIDInvalid
	}
	if reply.TopMessageID < 0 || reply.TopMessageID > MaxMessageBoxID {
		return ErrReplyMessageIDInvalid
	}
	if reply.MessageID == 0 && reply.TopMessageID == 0 {
		return ErrReplyMessageIDInvalid
	}
	if reply.QuoteOffset < 0 || reply.QuoteOffset > MaxMessageReplyQuoteOffset {
		return ErrReplyMessageIDInvalid
	}
	return nil
}

// MessageEntity 是业务层消息实体，不依赖 TL 类型。
type MessageEntity struct {
	Type   MessageEntityType
	Offset int
	Length int
}

// Message 是账号视角下的一条私聊消息。
type Message struct {
	ID          int   // 当前 owner 视角下的 message box id，暴露给 Telegram 客户端。
	UID         int64 // 共享私聊消息主体 id，不暴露给客户端。
	RandomID    int64
	OwnerUserID int64
	Peer        Peer
	From        Peer
	Date        int
	EditDate    int
	Out         bool
	Silent      bool
	NoForwards  bool
	Body        string
	Entities    []MessageEntity
	ReplyTo     *MessageReply
	Forward     *MessageForward
	Reactions   *ChannelMessageReactions
	Pts         int
	Media       *MessageMedia
}

// MessageReply describes a message reply/thread header without depending on TL types.
type MessageReply struct {
	MessageID     int
	Peer          Peer
	TopMessageID  int
	ForumTopic    bool
	QuoteText     string
	QuoteEntities []MessageEntity
	QuoteOffset   int
}

// MessageForward 描述一条转发消息的原始作者信息。
type MessageForward struct {
	From           Peer
	FromName       string
	Date           int
	ChannelPost    int
	SavedFrom      Peer
	SavedFromMsgID int
}

// MessageList 是账号视角下的消息查询结果。
type MessageList struct {
	Messages []Message
	Users    []User
	Count    int
	Hash     int64
}

// MessageFilter 描述历史/搜索查询条件。
type MessageFilter struct {
	HasPeer        bool
	Peer           Peer
	Query          string
	OffsetID       int
	OffsetDate     int
	AddOffset      int
	Limit          int
	MaxID          int
	MinID          int
	Hash           int64
	NeedTotalCount bool
}

// SendPrivateTextRequest 是私聊文本/媒体发送命令。
type SendPrivateTextRequest struct {
	SenderUserID    int64
	RecipientUserID int64
	RandomID        int64
	Message         string
	Entities        []MessageEntity
	Media           *MessageMedia
	Silent          bool
	NoForwards      bool
	ReplyTo         *MessageReply
	Forward         *MessageForward
	Date            int
	OriginAuthKeyID [8]byte
	OriginSessionID int64
}

// SendPrivateTextResult 描述一次私聊文本发送的双端结果。
type SendPrivateTextResult struct {
	SenderMessage    Message
	RecipientMessage Message
	SenderEvent      UpdateEvent
	RecipientEvent   UpdateEvent
	Duplicate        bool
}

// SetPrivateMessageReactionsRequest replaces the current user's reactions for one private message.
type SetPrivateMessageReactionsRequest struct {
	UserID      int64
	Peer        Peer
	MessageID   int
	Reactions   []MessageReaction
	Big         bool
	AddToRecent bool
	Date        int
}

// PrivateMessageReactionsRequest fetches reaction summaries for exact private message ids.
type PrivateMessageReactionsRequest struct {
	OwnerUserID int64
	Peer        Peer
	IDs         []int
}

// PrivateMessageReactionsResult describes private reaction updates in owner-visible boxes.
type PrivateMessageReactionsResult struct {
	Messages  []Message
	Reactions ChannelMessageReactions
}

// ForwardPrivateMessagesRequest 是私聊文本消息转发命令。
type ForwardPrivateMessagesRequest struct {
	OwnerUserID     int64
	FromPeer        Peer
	ToUserID        int64
	MessageIDs      []int
	RandomIDs       []int64
	Silent          bool
	NoForwards      bool
	DropAuthor      bool
	ReplyTo         *MessageReply
	Date            int
	OriginAuthKeyID [8]byte
	OriginSessionID int64
}

// ForwardPrivateMessagesResult 描述一次私聊转发的 owner 维度结果。
type ForwardPrivateMessagesResult struct {
	OwnerUserID       int64
	SenderMessages    []Message
	RecipientMessages []Message
	SenderEvents      []UpdateEvent
	RecipientEvents   []UpdateEvent
	Duplicates        []bool
}

// ReadHistoryRequest 是账号视角的 messages.readHistory 命令。
type ReadHistoryRequest struct {
	OwnerUserID     int64
	Peer            Peer
	MaxID           int
	Date            int
	OriginAuthKeyID [8]byte
	OriginSessionID int64
}

// ReadHistoryResult 描述一次会话已读操作的业务结果。
type ReadHistoryResult struct {
	OwnerUserID      int64
	Peer             Peer
	MaxID            int
	StillUnreadCount int
	Changed          bool
	InboxEvent       UpdateEvent
	OutboxChanged    bool
	OutboxUserID     int64
	OutboxEvent      UpdateEvent
}

// ReadMessageContentsRequest marks media/mention contents as read for exact owner-visible messages.
type ReadMessageContentsRequest struct {
	OwnerUserID int64
	IDs         []int
}

// ReadMessageContentsResult contains owner-visible message IDs that existed and can be synced.
type ReadMessageContentsResult struct {
	OwnerUserID int64
	MessageIDs  []int
}

// OutboxReadDateRequest 是 messages.getOutboxReadDate 查询。
type OutboxReadDateRequest struct {
	OwnerUserID int64
	Peer        Peer
	ID          int
}

// EditMessageRequest 是账号视角下编辑一条已发送私聊文本消息的命令。
type EditMessageRequest struct {
	OwnerUserID     int64
	Peer            Peer
	ID              int
	Message         string
	Entities        []MessageEntity
	EditDate        int
	OriginAuthKeyID [8]byte
	OriginSessionID int64
}

// EditedMessageForUser 描述一次编辑对某个 owner 视角造成的影响。
type EditedMessageForUser struct {
	UserID  int64
	Message Message
	Event   UpdateEvent
}

// EditMessageResult 描述消息编辑后的 owner 维度结果。
type EditMessageResult struct {
	OwnerUserID int64
	Edited      []EditedMessageForUser
}

// Self 返回当前请求账号的编辑结果。
func (r EditMessageResult) Self() EditedMessageForUser {
	for _, item := range r.Edited {
		if item.UserID == r.OwnerUserID {
			return item
		}
	}
	return EditedMessageForUser{UserID: r.OwnerUserID}
}

// Changed 表示本次编辑是否实际影响了任何 owner 视角。
func (r EditMessageResult) Changed() bool {
	return len(r.Edited) > 0
}

// DeleteMessagesRequest 是账号视角下按消息 ID 删除消息的命令。
type DeleteMessagesRequest struct {
	OwnerUserID     int64
	IDs             []int
	Revoke          bool
	Date            int
	OriginAuthKeyID [8]byte
	OriginSessionID int64
}

// DeleteHistoryRequest 是账号视角下清空某个 peer 历史的命令。
type DeleteHistoryRequest struct {
	OwnerUserID     int64
	Peer            Peer
	MaxID           int
	JustClear       bool
	Revoke          bool
	Date            int
	OriginAuthKeyID [8]byte
	OriginSessionID int64
}

// DeletedMessagesForUser 描述一次删除对某个 owner 视角造成的影响。
type DeletedMessagesForUser struct {
	UserID     int64
	MessageIDs []int
	Event      UpdateEvent
}

// DeleteMessagesResult 描述消息删除后的 owner 维度结果。
type DeleteMessagesResult struct {
	OwnerUserID int64
	Deleted     []DeletedMessagesForUser
	Offset      int
}

// Self 返回当前请求账号的删除结果。
func (r DeleteMessagesResult) Self() DeletedMessagesForUser {
	for _, item := range r.Deleted {
		if item.UserID == r.OwnerUserID {
			return item
		}
	}
	return DeletedMessagesForUser{UserID: r.OwnerUserID}
}

// Changed 表示本次删除是否实际影响了任何 owner 视角。
func (r DeleteMessagesResult) Changed() bool {
	for _, item := range r.Deleted {
		if len(item.MessageIDs) > 0 {
			return true
		}
	}
	return false
}
