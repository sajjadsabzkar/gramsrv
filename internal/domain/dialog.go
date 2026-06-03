package domain

// PeerType 标识 dialog 所属 peer 类型。
type PeerType string

const (
	PeerTypeUser    PeerType = "user"
	PeerTypeChannel PeerType = "channel"
)

const (
	// DialogMainFolderID 是 TDesktop 主会话列表 folder_id。
	DialogMainFolderID = 0
	// DialogArchiveFolderID 是 Telegram 约定的归档会话 folder_id。
	DialogArchiveFolderID = 1
	// DialogCustomFolderMinID 起才允许用户自定义 filter。
	DialogCustomFolderMinID = 2
	// MaxDialogFolders 限制单用户自定义 filter 数量，避免无界配置拖垮启动同步。
	MaxDialogFolders = 100
	// MaxDialogFolderPeers 限制单 filter 中 include/exclude/pinned peer 数。
	MaxDialogFolderPeers = 100
	// MaxDialogFolderTitleRunes 对齐 Telegram folder title 的短标题语义。
	MaxDialogFolderTitleRunes = 64
	// MaxDialogDraftsPerUser bounds messages.getAllDrafts / clearAllDrafts work.
	MaxDialogDraftsPerUser = 1000
)

// Peer 是业务层 peer 值对象，不依赖 TL 类型。
type Peer struct {
	Type PeerType
	ID   int64
}

// Dialog 是账号的一条会话摘要。
type Dialog struct {
	Peer                  Peer
	ChannelLeft           bool
	FolderID              int
	TopMessage            int
	TopMessageDate        int
	ReadInboxMaxID        int
	ReadOutboxMaxID       int
	UnreadCount           int
	UnreadMentions        int
	UnreadReactions       int
	Pinned                bool
	PinnedOrder           int
	UnreadMark            bool
	ViewForumAsMessages   bool
	PeerSettingsBarHidden bool
	Draft                 *DialogDraft
}

// DialogDraftWebPage stores a draft link preview without depending on TL input media types.
type DialogDraftWebPage struct {
	URL             string
	ForceLargeMedia bool
	ForceSmallMedia bool
	Optional        bool
}

// DialogDraft is a cloud draft for one peer/topic, expressed only in domain types.
type DialogDraft struct {
	Peer         Peer
	TopMessageID int
	Date         int
	NoWebpage    bool
	InvertMedia  bool
	Message      string
	Entities     []MessageEntity
	ReplyTo      *MessageReply
	WebPage      *DialogDraftWebPage
	Effect       int64
}

// Empty reports whether this draft should clear the cloud draft slot.
func (d DialogDraft) Empty() bool {
	replyOnlyTopic := d.ReplyTo != nil && d.ReplyTo.MessageID == 0 && d.ReplyTo.TopMessageID > 0
	return !d.NoWebpage &&
		!d.InvertMedia &&
		d.Message == "" &&
		len(d.Entities) == 0 &&
		(d.ReplyTo == nil || replyOnlyTopic) &&
		d.WebPage == nil &&
		d.Effect == 0
}

// DialogList 是 dialogs 查询结果。
type DialogList struct {
	Dialogs         []Dialog
	Messages        []Message
	ChannelMessages []ChannelMessage
	Users           []User
	Channels        []Channel
	State           UpdateState
	Hash            int64
	Count           int
}

// DialogFilter 是会话列表查询条件。
type DialogFilter struct {
	PinnedOnly    bool
	ExcludePinned bool
	HasFolderID   bool
	FolderID      int
	Folder        *DialogFolder
	OffsetDate    int
	OffsetID      int
	HasOffsetPeer bool
	OffsetPeer    Peer
	Limit         int
	Hash          int64
}

// DialogFolderPeer 是 folder/filter 规则中的 peer，保留 access_hash 供 RPC 层回写 InputPeer。
type DialogFolderPeer struct {
	Peer       Peer
	AccessHash int64
}

// DialogFolder 是用户自定义会话分组规则。它只表达业务含义，不依赖 TL 生成类型。
type DialogFolder struct {
	ID              int
	Contacts        bool
	NonContacts     bool
	Groups          bool
	Broadcasts      bool
	Bots            bool
	ExcludeMuted    bool
	ExcludeRead     bool
	ExcludeArchived bool
	TitleNoanimate  bool
	Title           string
	TitleEntities   []MessageEntity
	Emoticon        string
	HasEmoticon     bool
	Color           int
	HasColor        bool
	PinnedPeers     []DialogFolderPeer
	IncludePeers    []DialogFolderPeer
	ExcludePeers    []DialogFolderPeer
	IsChatlist      bool
}

// DialogFolderList 是 messages.getDialogFilters 的业务响应。
type DialogFolderList struct {
	TagsEnabled bool
	Folders     []DialogFolder
}

// FolderPeerUpdate 描述 folders.editPeerFolders 的单个归档/还原变更。
type FolderPeerUpdate struct {
	Peer     Peer
	FolderID int
}
