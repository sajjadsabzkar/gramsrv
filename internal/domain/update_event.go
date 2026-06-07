package domain

// UpdateEventType 标识 update 队列事件类型。
type UpdateEventType string

const (
	UpdateEventNewMessage          UpdateEventType = "new_message"
	UpdateEventReadHistoryInbox    UpdateEventType = "read_history_inbox"
	UpdateEventReadHistoryOutbox   UpdateEventType = "read_history_outbox"
	UpdateEventReadMessageContents UpdateEventType = "read_message_contents"
	UpdateEventEditMessage         UpdateEventType = "edit_message"
	UpdateEventMessageReactions    UpdateEventType = "message_reactions"
	UpdateEventContactsReset       UpdateEventType = "contacts_reset"
	UpdateEventDialogPinned        UpdateEventType = "dialog_pinned"
	UpdateEventPinnedDialogs       UpdateEventType = "pinned_dialogs"
	UpdateEventDialogUnreadMark    UpdateEventType = "dialog_unread_mark"
	UpdateEventPeerSettings        UpdateEventType = "peer_settings"
	UpdateEventDeleteMessages      UpdateEventType = "delete_messages"
	UpdateEventDialogFilter        UpdateEventType = "dialog_filter"
	UpdateEventDialogFilterOrder   UpdateEventType = "dialog_filter_order"
	UpdateEventDialogFilters       UpdateEventType = "dialog_filters"
	UpdateEventFolderPeers         UpdateEventType = "folder_peers"
	UpdateEventChannelAvailable    UpdateEventType = "channel_available_messages"
	UpdateEventChannelViewForum    UpdateEventType = "channel_view_forum_as_messages"
	UpdateEventNoop                UpdateEventType = "noop"
)

// UpdateEvent 是账号视角的增量事件，按 user_id + pts 顺序持久化。
type UpdateEvent struct {
	UserID           int64
	Type             UpdateEventType
	Pts              int
	PtsCount         int
	Date             int
	Message          Message
	Peer             Peer
	Peers            []Peer
	Bool             bool
	Settings         PeerSettings
	MessageIDs       []int
	MaxID            int
	StillUnreadCount int
	Users            []User
	Channels         []Channel
	FilterID         int
	DialogFilter     *DialogFolder
	FilterOrder      []int
	FolderPeers      []FolderPeerUpdate
	TagsEnabled      bool
}

// UpdateDifference 是 updates.getDifference 的业务层结果。
type UpdateDifference struct {
	State         UpdateState
	Events        []UpdateEvent
	ChannelNudges []ChannelDifferenceNudge
	// Partial 为 true 表示连续事件被 limit 截断、后面还有（映射 updates.differenceSlice，
	// 客户端据 State 继续翻页）；false 表示已到当前连续末尾（updates.difference）。
	Partial bool
}

// ChannelDifferenceNudge is a computed account-level hint that a channel diff is dirty.
type ChannelDifferenceNudge struct {
	ChannelID int64
	Pts       int
}
