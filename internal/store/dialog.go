package store

import (
	"context"

	"telesrv/internal/domain"
)

// DialogStore 持久化用户会话摘要。
type DialogStore interface {
	ListByUser(ctx context.Context, userID int64, filter domain.DialogFilter) (domain.DialogList, error)
	ListByPeers(ctx context.Context, userID int64, peers []domain.Peer) (domain.DialogList, error)
	Upsert(ctx context.Context, userID int64, dialog domain.Dialog) error
	SaveDraft(ctx context.Context, userID int64, draft domain.DialogDraft) error
	DeleteDraft(ctx context.Context, userID int64, peer domain.Peer, topMessageID int) (bool, error)
	ListDrafts(ctx context.Context, userID int64, limit int) ([]domain.DialogDraft, error)
	ClearDrafts(ctx context.Context, userID int64, limit int) ([]domain.DialogDraft, error)
	MarkRead(ctx context.Context, userID int64, peer domain.Peer, maxID int) (domain.ReadHistoryResult, error)
	SetPinned(ctx context.Context, userID int64, peer domain.Peer, pinned bool) (bool, error)
	ReorderPinned(ctx context.Context, userID int64, order []domain.Peer, force bool) error
	SetUnreadMark(ctx context.Context, userID int64, peer domain.Peer, unread bool) (bool, error)
	ListUnreadMarked(ctx context.Context, userID int64) ([]domain.Peer, error)
	SetPeerSettingsBarHidden(ctx context.Context, userID int64, peer domain.Peer) (bool, error)
	PeerSettingsBarHidden(ctx context.Context, userID int64, peer domain.Peer) (bool, error)
	ListFolders(ctx context.Context, userID int64) (domain.DialogFolderList, error)
	GetFolder(ctx context.Context, userID int64, folderID int) (domain.DialogFolder, bool, error)
	UpsertFolder(ctx context.Context, userID int64, folder domain.DialogFolder) error
	DeleteFolder(ctx context.Context, userID int64, folderID int) error
	ReorderFolders(ctx context.Context, userID int64, order []int) error
	SetFolderTagsEnabled(ctx context.Context, userID int64, enabled bool) error
	EditPeerFolders(ctx context.Context, userID int64, peers []domain.FolderPeerUpdate) error
}
