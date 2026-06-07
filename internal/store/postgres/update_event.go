package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// UpdateEventStore 用 PostgreSQL 实现 store.UpdateEventStore。
type UpdateEventStore struct {
	db sqlcgen.DBTX
	q  *sqlcgen.Queries
}

// NewUpdateEventStore 基于 pgx 连接池（或事务）创建 UpdateEventStore。
func NewUpdateEventStore(db sqlcgen.DBTX) *UpdateEventStore {
	return &UpdateEventStore{db: db, q: sqlcgen.New(db)}
}

func (s *UpdateEventStore) Append(ctx context.Context, userID int64, event domain.UpdateEvent) error {
	beginner, ok := s.db.(interface {
		Begin(context.Context) (pgx.Tx, error)
	})
	if !ok {
		if err := appendUserUpdateEvent(ctx, s.q, userID, event); err != nil {
			return fmt.Errorf("append update event: %w", err)
		}
		return nil
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin append update event: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if err := appendUserUpdateEvent(ctx, sqlcgen.New(tx), userID, event); err != nil {
		return fmt.Errorf("append update event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit append update event: %w", err)
	}
	committed = true
	return nil
}

// AppendWithDispatch 将账号级 update 事件与在线投递 outbox 放入同一个 PG 事务。
// 设置类 RPC 不像消息发送那样已有业务大事务；这里至少保证“事件已持久化”与
// “可靠在线投递任务已入队”同生共死，避免进程在手动 push 前退出造成在线通知漏投。
func (s *UpdateEventStore) AppendWithDispatch(ctx context.Context, userID int64, event domain.UpdateEvent, excludeAuthKeyID [8]byte, excludeSessionID int64) error {
	beginner, ok := s.db.(interface {
		Begin(context.Context) (pgx.Tx, error)
	})
	if !ok {
		if err := s.Append(ctx, userID, event); err != nil {
			return err
		}
		if err := s.q.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
			TargetUserID:     userID,
			Pts:              int32(event.Pts),
			EventType:        string(event.Type),
			ExcludeAuthKeyID: authKeyIDToInt64(excludeAuthKeyID),
			ExcludeSessionID: excludeSessionID,
		}); err != nil {
			return fmt.Errorf("enqueue dispatch: %w", err)
		}
		return nil
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin append update dispatch: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	qtx := sqlcgen.New(tx)
	if err := appendUserUpdateEvent(ctx, qtx, userID, event); err != nil {
		return fmt.Errorf("append update event: %w", err)
	}
	if err := qtx.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
		TargetUserID:     userID,
		Pts:              int32(event.Pts),
		EventType:        string(event.Type),
		ExcludeAuthKeyID: authKeyIDToInt64(excludeAuthKeyID),
		ExcludeSessionID: excludeSessionID,
	}); err != nil {
		return fmt.Errorf("enqueue dispatch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit append update dispatch: %w", err)
	}
	committed = true
	return nil
}

func appendUserUpdateEvent(ctx context.Context, q *sqlcgen.Queries, userID int64, event domain.UpdateEvent) error {
	var messageID *int32
	if event.Message.ID != 0 {
		id := int32(event.Message.ID)
		messageID = &id
	}
	var peerType *string
	var peerID *int64
	peer := event.Peer
	if peer.ID == 0 {
		peer = event.Message.Peer
	}
	if peer.ID != 0 {
		t := string(peer.Type)
		id := peer.ID
		peerType = &t
		peerID = &id
	}
	peers, err := encodeEventPeers(event.Peers)
	if err != nil {
		return err
	}
	settings, err := encodePeerSettings(event.Settings)
	if err != nil {
		return err
	}
	messageIDs, err := encodeEventMessageIDs(event.MessageIDs)
	if err != nil {
		return err
	}
	dialogFilter, err := encodeEventDialogFilter(event.DialogFilter)
	if err != nil {
		return err
	}
	filterOrder, err := encodeEventFilterOrder(event.FilterOrder)
	if err != nil {
		return err
	}
	folderPeers, err := encodeEventFolderPeers(event.FolderPeers)
	if err != nil {
		return err
	}
	if err := q.AppendUserUpdateEvent(ctx, sqlcgen.AppendUserUpdateEventParams{
		UserID:           userID,
		Pts:              int32(event.Pts),
		PtsCount:         int32(event.PtsCount),
		Date:             int32(event.Date),
		EventType:        string(event.Type),
		EventBool:        event.Bool,
		EventPeers:       peers,
		PeerSettings:     settings,
		MessageIds:       messageIDs,
		DialogFilter:     dialogFilter,
		FilterOrder:      filterOrder,
		FolderPeers:      folderPeers,
		MaxID:            pgInt32NonNegative(event.MaxID),
		StillUnreadCount: int32(event.StillUnreadCount),
		FilterID:         pgInt32NonNegative(event.FilterID),
		TagsEnabled:      event.TagsEnabled,
		MessageBoxID:     messageID,
		PeerType:         peerType,
		PeerID:           peerID,
	}); err != nil {
		return err
	}
	if _, err := advanceContiguousPts(ctx, q, userID); err != nil {
		return fmt.Errorf("advance update watermark: %w", err)
	}
	return nil
}

func (s *UpdateEventStore) ListAfter(ctx context.Context, userID int64, pts, limit int) ([]domain.UpdateEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.q.ListUserUpdateEventsAfter(ctx, sqlcgen.ListUserUpdateEventsAfterParams{
		UserID:     userID,
		Pts:        int32(pts),
		LimitCount: int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("list update events: %w", err)
	}
	out := make([]domain.UpdateEvent, 0, len(rows))
	for _, row := range rows {
		entities, err := decodeMessageEntities(row.MessageEntitiesJson)
		if err != nil {
			return nil, fmt.Errorf("decode message entities: %w", err)
		}
		silent, noforwards, reply, forward, err := messageMetadataFromFields(
			row.Silent,
			row.Noforwards,
			row.ReplyToMsgID,
			row.ReplyToPeerType,
			row.ReplyToPeerID,
			row.ReplyToTopID,
			row.QuoteText,
			row.QuoteEntitiesJson,
			row.QuoteOffset,
			row.FwdFromPeerType,
			row.FwdFromPeerID,
			row.FwdFromName,
			row.FwdDate,
		)
		if err != nil {
			return nil, fmt.Errorf("decode message metadata: %w", err)
		}
		peers, err := decodeEventPeers(row.EventPeersJson)
		if err != nil {
			return nil, fmt.Errorf("decode event peers: %w", err)
		}
		settings, err := decodePeerSettings(row.PeerSettingsJson)
		if err != nil {
			return nil, fmt.Errorf("decode peer settings: %w", err)
		}
		messageIDs, err := decodeEventMessageIDs(row.MessageIdsJson)
		if err != nil {
			return nil, fmt.Errorf("decode message ids: %w", err)
		}
		dialogFilter, err := decodeEventDialogFilter(row.DialogFilterJson)
		if err != nil {
			return nil, fmt.Errorf("decode dialog filter: %w", err)
		}
		filterOrder, err := decodeEventFilterOrder(row.FilterOrderJson)
		if err != nil {
			return nil, fmt.Errorf("decode filter order: %w", err)
		}
		folderPeers, err := decodeEventFolderPeers(row.FolderPeersJson)
		if err != nil {
			return nil, fmt.Errorf("decode folder peers: %w", err)
		}
		media, err := decodeMessageMedia(row.MediaJson)
		if err != nil {
			return nil, fmt.Errorf("decode message media: %w", err)
		}
		out = append(out, domain.UpdateEvent{
			UserID:           row.UserID,
			Type:             domain.UpdateEventType(row.EventType),
			Pts:              int(row.Pts),
			PtsCount:         int(row.PtsCount),
			Date:             int(row.Date),
			Peer:             domain.Peer{Type: domain.PeerType(row.EventPeerType), ID: row.EventPeerID},
			Peers:            peers,
			Bool:             row.EventBool,
			Settings:         settings,
			MessageIDs:       messageIDs,
			MaxID:            int(row.MaxID),
			StillUnreadCount: int(row.StillUnreadCount),
			FilterID:         int(row.FilterID),
			DialogFilter:     dialogFilter,
			FilterOrder:      filterOrder,
			FolderPeers:      folderPeers,
			TagsEnabled:      row.TagsEnabled,
			Message: domain.Message{
				ID:             int(row.MessageID),
				UID:            row.PrivateMessageID,
				OwnerUserID:    row.OwnerUserID,
				Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
				From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
				Date:           int(row.MessageDate),
				EditDate:       int(row.EditDate),
				Out:            row.Outgoing,
				Silent:         silent,
				NoForwards:     noforwards,
				Body:           row.Body,
				Entities:       entities,
				ReplyTo:        reply,
				Forward:        forward,
				Media:          media,
				MediaUnread:    row.MediaUnread,
				ReactionUnread: row.ReactionUnread,
			},
			Users:    usersFromUpdateEventRow(row),
			Channels: channelsFromUpdateEventRow(row),
		})
	}
	return out, nil
}

func (s *UpdateEventStore) Current(ctx context.Context, userID int64) (int, error) {
	pts, err := s.q.MaxUserPts(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("max user pts: %w", err)
	}
	return int(pts), nil
}

func (s *UpdateEventStore) AdvanceContiguousPts(ctx context.Context, userID int64) (int, error) {
	beginner, ok := s.db.(interface {
		Begin(context.Context) (pgx.Tx, error)
	})
	if !ok {
		pts, err := advanceContiguousPts(ctx, s.q, userID)
		if err != nil {
			return 0, fmt.Errorf("advance update watermark: %w", err)
		}
		return pts, nil
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin advance update watermark: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	pts, err := advanceContiguousPts(ctx, sqlcgen.New(tx), userID)
	if err != nil {
		return 0, fmt.Errorf("advance update watermark: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit advance update watermark: %w", err)
	}
	committed = true
	return pts, nil
}

// contiguousWindow 是计算最大连续 pts 时回看的顶部 pts 数量。
// 瞬时空洞只来自最近在途的发送事务（提交即填实、回退即补 noop），单用户在途量远小于此，
// 故窗口内若无空洞即可认定窗口下方连续。生产极端高 fan-in 可调大。
const contiguousWindow = 4096

// MaxContiguousPts 见 store.UpdateEventStore 接口说明。正常路径 O(1) 读账号水位；
// 缺行通常来自迁移前数据，允许一次性从 durable 事件计算并补写。
func (s *UpdateEventStore) MaxContiguousPts(ctx context.Context, userID int64) (int, error) {
	pts, err := s.q.GetUserUpdateWatermark(ctx, userID)
	if err == nil {
		return int(pts), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("get update watermark: %w", err)
	}
	return s.AdvanceContiguousPts(ctx, userID)
}

func advanceContiguousPts(ctx context.Context, q *sqlcgen.Queries, userID int64) (int, error) {
	if err := q.EnsureUserUpdateWatermark(ctx, userID); err != nil {
		return 0, err
	}
	locked, err := q.LockUserUpdateWatermark(ctx, userID)
	if err != nil {
		return 0, err
	}
	contiguous := int(locked)
	for {
		rows, err := q.NextUserPtsAfter(ctx, sqlcgen.NextUserPtsAfterParams{
			UserID:     userID,
			Pts:        int32(contiguous),
			LimitCount: contiguousWindow,
		})
		if err != nil {
			return 0, err
		}
		if len(rows) == 0 {
			break
		}
		advanced := false
		for _, row := range rows {
			count := maxInt(int(row.PtsCount), 1)
			expected := contiguous + count
			if int(row.Pts) != expected {
				if contiguous > int(locked) {
					if err := saveUserUpdateWatermark(ctx, q, userID, contiguous); err != nil {
						return 0, err
					}
				}
				return contiguous, nil
			}
			contiguous = int(row.Pts)
			advanced = true
		}
		if len(rows) < contiguousWindow || !advanced {
			break
		}
	}
	if contiguous > int(locked) {
		if err := saveUserUpdateWatermark(ctx, q, userID, contiguous); err != nil {
			return 0, err
		}
	}
	return contiguous, nil
}

func saveUserUpdateWatermark(ctx context.Context, q *sqlcgen.Queries, userID int64, contiguous int) error {
	return q.SaveUserUpdateWatermark(ctx, sqlcgen.SaveUserUpdateWatermarkParams{
		UserID:        userID,
		ContiguousPts: int32(contiguous),
	})
}

func computeContiguousPtsFromRecent(ctx context.Context, q *sqlcgen.Queries, userID int64) (int, error) {
	rows, err := q.RecentUserPts(ctx, sqlcgen.RecentUserPtsParams{
		UserID:     userID,
		WindowSize: contiguousWindow,
	})
	if err != nil {
		return 0, fmt.Errorf("recent user pts: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	nextByStart := make(map[int]int, len(rows))
	floor := int(rows[0].Pts) - maxInt(int(rows[0].PtsCount), 1)
	for _, p := range rows {
		count := maxInt(int(p.PtsCount), 1)
		v := int(p.Pts)
		start := v - count
		nextByStart[start] = v
		if start < floor {
			floor = start
		}
	}
	contiguous := floor
	for {
		next, ok := nextByStart[contiguous]
		if !ok {
			break
		}
		contiguous = next
	}
	return contiguous, nil
}

// BatchByCursor 按 (user_id, pts) 一次性批量取多条账号事件，供 outbox worker 取代逐条 ListAfter。
// 返回顺序不保证与 cursors 一致，调用方按 (UserID,Pts) 自行索引。
func (s *UpdateEventStore) BatchByCursor(ctx context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error) {
	if len(cursors) == 0 {
		return nil, nil
	}
	userIDs := make([]int64, len(cursors))
	ptsList := make([]int32, len(cursors))
	for i, c := range cursors {
		userIDs[i] = c.UserID
		ptsList[i] = int32(c.Pts)
	}
	rows, err := s.q.BatchListDispatchEvents(ctx, sqlcgen.BatchListDispatchEventsParams{
		UserIds: userIDs,
		PtsList: ptsList,
	})
	if err != nil {
		return nil, fmt.Errorf("batch list dispatch events: %w", err)
	}
	out := make([]domain.UpdateEvent, 0, len(rows))
	for _, row := range rows {
		entities, err := decodeMessageEntities(row.MessageEntitiesJson)
		if err != nil {
			return nil, fmt.Errorf("decode message entities: %w", err)
		}
		silent, noforwards, reply, forward, err := messageMetadataFromFields(
			row.Silent,
			row.Noforwards,
			row.ReplyToMsgID,
			row.ReplyToPeerType,
			row.ReplyToPeerID,
			row.ReplyToTopID,
			row.QuoteText,
			row.QuoteEntitiesJson,
			row.QuoteOffset,
			row.FwdFromPeerType,
			row.FwdFromPeerID,
			row.FwdFromName,
			row.FwdDate,
		)
		if err != nil {
			return nil, fmt.Errorf("decode message metadata: %w", err)
		}
		peers, err := decodeEventPeers(row.EventPeersJson)
		if err != nil {
			return nil, fmt.Errorf("decode event peers: %w", err)
		}
		settings, err := decodePeerSettings(row.PeerSettingsJson)
		if err != nil {
			return nil, fmt.Errorf("decode peer settings: %w", err)
		}
		messageIDs, err := decodeEventMessageIDs(row.MessageIdsJson)
		if err != nil {
			return nil, fmt.Errorf("decode message ids: %w", err)
		}
		dialogFilter, err := decodeEventDialogFilter(row.DialogFilterJson)
		if err != nil {
			return nil, fmt.Errorf("decode dialog filter: %w", err)
		}
		filterOrder, err := decodeEventFilterOrder(row.FilterOrderJson)
		if err != nil {
			return nil, fmt.Errorf("decode filter order: %w", err)
		}
		folderPeers, err := decodeEventFolderPeers(row.FolderPeersJson)
		if err != nil {
			return nil, fmt.Errorf("decode folder peers: %w", err)
		}
		media, err := decodeMessageMedia(row.MediaJson)
		if err != nil {
			return nil, fmt.Errorf("decode message media: %w", err)
		}
		out = append(out, domain.UpdateEvent{
			UserID:           row.UserID,
			Type:             domain.UpdateEventType(row.EventType),
			Pts:              int(row.Pts),
			PtsCount:         int(row.PtsCount),
			Date:             int(row.Date),
			Peer:             domain.Peer{Type: domain.PeerType(row.EventPeerType), ID: row.EventPeerID},
			Peers:            peers,
			Bool:             row.EventBool,
			Settings:         settings,
			MessageIDs:       messageIDs,
			MaxID:            int(row.MaxID),
			StillUnreadCount: int(row.StillUnreadCount),
			FilterID:         int(row.FilterID),
			DialogFilter:     dialogFilter,
			FilterOrder:      filterOrder,
			FolderPeers:      folderPeers,
			TagsEnabled:      row.TagsEnabled,
			Message: domain.Message{
				ID:             int(row.MessageID),
				UID:            row.PrivateMessageID,
				OwnerUserID:    row.OwnerUserID,
				Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
				From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
				Date:           int(row.MessageDate),
				EditDate:       int(row.EditDate),
				Out:            row.Outgoing,
				Silent:         silent,
				NoForwards:     noforwards,
				Body:           row.Body,
				Entities:       entities,
				ReplyTo:        reply,
				Forward:        forward,
				Media:          media,
				MediaUnread:    row.MediaUnread,
				ReactionUnread: row.ReactionUnread,
			},
			Users:    usersFromBatchDispatchRow(row),
			Channels: channelsFromBatchDispatchRow(row),
		})
	}
	return out, nil
}

func usersFromUpdateEventRow(row sqlcgen.ListUserUpdateEventsAfterRow) []domain.User {
	return mergeEventUsers(
		domain.User{
			ID:          row.PeerUserID,
			AccessHash:  row.PeerAccessHash,
			Phone:       row.PeerPhone,
			FirstName:   row.PeerFirstName,
			LastName:    row.PeerLastName,
			Username:    row.PeerUsername,
			CountryCode: row.PeerCountryCode,
			Verified:    row.PeerVerified,
			Support:     row.PeerSupport,
		},
		domain.User{
			ID:          row.FromUserUserID,
			AccessHash:  row.FromUserAccessHash,
			Phone:       row.FromUserPhone,
			FirstName:   row.FromUserFirstName,
			LastName:    row.FromUserLastName,
			Username:    row.FromUserUsername,
			CountryCode: row.FromUserCountryCode,
			Verified:    row.FromUserVerified,
			Support:     row.FromUserSupport,
		},
		domain.User{
			ID:          row.FwdUserID,
			AccessHash:  row.FwdUserAccessHash,
			Phone:       row.FwdUserPhone,
			FirstName:   row.FwdUserFirstName,
			LastName:    row.FwdUserLastName,
			Username:    row.FwdUserUsername,
			CountryCode: row.FwdUserCountryCode,
			Verified:    row.FwdUserVerified,
			Support:     row.FwdUserSupport,
		},
		domain.User{
			ID:          row.ReplyUserID,
			AccessHash:  row.ReplyUserAccessHash,
			Phone:       row.ReplyUserPhone,
			FirstName:   row.ReplyUserFirstName,
			LastName:    row.ReplyUserLastName,
			Username:    row.ReplyUserUsername,
			CountryCode: row.ReplyUserCountryCode,
			Verified:    row.ReplyUserVerified,
			Support:     row.ReplyUserSupport,
		},
	)
}

// usersFromBatchDispatchRow 与 usersFromUpdateEventRow 等价，只是行类型为 BatchListDispatchEventsRow
// （两条查询列完全一致；改一处列时务必同步另一处）。
func usersFromBatchDispatchRow(row sqlcgen.BatchListDispatchEventsRow) []domain.User {
	return mergeEventUsers(
		domain.User{
			ID:          row.PeerUserID,
			AccessHash:  row.PeerAccessHash,
			Phone:       row.PeerPhone,
			FirstName:   row.PeerFirstName,
			LastName:    row.PeerLastName,
			Username:    row.PeerUsername,
			CountryCode: row.PeerCountryCode,
			Verified:    row.PeerVerified,
			Support:     row.PeerSupport,
		},
		domain.User{
			ID:          row.FromUserUserID,
			AccessHash:  row.FromUserAccessHash,
			Phone:       row.FromUserPhone,
			FirstName:   row.FromUserFirstName,
			LastName:    row.FromUserLastName,
			Username:    row.FromUserUsername,
			CountryCode: row.FromUserCountryCode,
			Verified:    row.FromUserVerified,
			Support:     row.FromUserSupport,
		},
		domain.User{
			ID:          row.FwdUserID,
			AccessHash:  row.FwdUserAccessHash,
			Phone:       row.FwdUserPhone,
			FirstName:   row.FwdUserFirstName,
			LastName:    row.FwdUserLastName,
			Username:    row.FwdUserUsername,
			CountryCode: row.FwdUserCountryCode,
			Verified:    row.FwdUserVerified,
			Support:     row.FwdUserSupport,
		},
		domain.User{
			ID:          row.ReplyUserID,
			AccessHash:  row.ReplyUserAccessHash,
			Phone:       row.ReplyUserPhone,
			FirstName:   row.ReplyUserFirstName,
			LastName:    row.ReplyUserLastName,
			Username:    row.ReplyUserUsername,
			CountryCode: row.ReplyUserCountryCode,
			Verified:    row.ReplyUserVerified,
			Support:     row.ReplyUserSupport,
		},
	)
}

func channelsFromUpdateEventRow(row sqlcgen.ListUserUpdateEventsAfterRow) []domain.Channel {
	return mergeEventChannels(
		eventChannelFromFields(
			row.FwdChannelID, row.FwdChannelAccessHash, row.FwdChannelCreatorUserID, row.FwdChannelTitle, row.FwdChannelAbout, row.FwdChannelUsername,
			row.FwdChannelBroadcast, row.FwdChannelMegagroup, row.FwdChannelForum, row.FwdChannelNoforwards, row.FwdChannelSignatures, row.FwdChannelPreHistoryHidden,
			int(row.FwdChannelSlowmodeSeconds), row.FwdChannelDefaultBannedRights, int(row.FwdChannelParticipantsCount), int(row.FwdChannelAdminsCount),
			int(row.FwdChannelKickedCount), int(row.FwdChannelBannedCount), int(row.FwdChannelTopMessageID), int(row.FwdChannelPinnedMessageID),
			int(row.FwdChannelPts), int(row.FwdChannelTtlPeriod), int(row.FwdChannelDate), row.FwdChannelDeleted,
		),
		eventChannelFromFields(
			row.ReplyChannelID, row.ReplyChannelAccessHash, row.ReplyChannelCreatorUserID, row.ReplyChannelTitle, row.ReplyChannelAbout, row.ReplyChannelUsername,
			row.ReplyChannelBroadcast, row.ReplyChannelMegagroup, row.ReplyChannelForum, row.ReplyChannelNoforwards, row.ReplyChannelSignatures, row.ReplyChannelPreHistoryHidden,
			int(row.ReplyChannelSlowmodeSeconds), row.ReplyChannelDefaultBannedRights, int(row.ReplyChannelParticipantsCount), int(row.ReplyChannelAdminsCount),
			int(row.ReplyChannelKickedCount), int(row.ReplyChannelBannedCount), int(row.ReplyChannelTopMessageID), int(row.ReplyChannelPinnedMessageID),
			int(row.ReplyChannelPts), int(row.ReplyChannelTtlPeriod), int(row.ReplyChannelDate), row.ReplyChannelDeleted,
		),
	)
}

func channelsFromBatchDispatchRow(row sqlcgen.BatchListDispatchEventsRow) []domain.Channel {
	return mergeEventChannels(
		eventChannelFromFields(
			row.FwdChannelID, row.FwdChannelAccessHash, row.FwdChannelCreatorUserID, row.FwdChannelTitle, row.FwdChannelAbout, row.FwdChannelUsername,
			row.FwdChannelBroadcast, row.FwdChannelMegagroup, row.FwdChannelForum, row.FwdChannelNoforwards, row.FwdChannelSignatures, row.FwdChannelPreHistoryHidden,
			int(row.FwdChannelSlowmodeSeconds), row.FwdChannelDefaultBannedRights, int(row.FwdChannelParticipantsCount), int(row.FwdChannelAdminsCount),
			int(row.FwdChannelKickedCount), int(row.FwdChannelBannedCount), int(row.FwdChannelTopMessageID), int(row.FwdChannelPinnedMessageID),
			int(row.FwdChannelPts), int(row.FwdChannelTtlPeriod), int(row.FwdChannelDate), row.FwdChannelDeleted,
		),
		eventChannelFromFields(
			row.ReplyChannelID, row.ReplyChannelAccessHash, row.ReplyChannelCreatorUserID, row.ReplyChannelTitle, row.ReplyChannelAbout, row.ReplyChannelUsername,
			row.ReplyChannelBroadcast, row.ReplyChannelMegagroup, row.ReplyChannelForum, row.ReplyChannelNoforwards, row.ReplyChannelSignatures, row.ReplyChannelPreHistoryHidden,
			int(row.ReplyChannelSlowmodeSeconds), row.ReplyChannelDefaultBannedRights, int(row.ReplyChannelParticipantsCount), int(row.ReplyChannelAdminsCount),
			int(row.ReplyChannelKickedCount), int(row.ReplyChannelBannedCount), int(row.ReplyChannelTopMessageID), int(row.ReplyChannelPinnedMessageID),
			int(row.ReplyChannelPts), int(row.ReplyChannelTtlPeriod), int(row.ReplyChannelDate), row.ReplyChannelDeleted,
		),
	)
}

// mergeEventUsers 合并事件依赖用户，跳过 ID=0 并按 ID 去重。
func mergeEventUsers(items ...domain.User) []domain.User {
	users := make([]domain.User, 0, len(items))
	add := func(u domain.User) {
		if u.ID == 0 {
			return
		}
		for _, existing := range users {
			if existing.ID == u.ID {
				return
			}
		}
		users = append(users, u)
	}
	for _, item := range items {
		add(item)
	}
	return users
}

func eventChannelFromFields(id, accessHash, creatorUserID int64, title, about, username string, broadcast, megagroup, forum, noforwards, signatures, preHistoryHidden bool, slowmodeSeconds int, defaultRights string, participantsCount, adminsCount, kickedCount, bannedCount, topMessageID, pinnedMessageID, pts, ttlPeriod, date int, deleted bool) domain.Channel {
	if id == 0 {
		return domain.Channel{}
	}
	ch := domain.Channel{
		ID:                id,
		AccessHash:        accessHash,
		CreatorUserID:     creatorUserID,
		Title:             title,
		About:             about,
		Username:          username,
		Broadcast:         broadcast,
		Megagroup:         megagroup,
		Forum:             forum,
		NoForwards:        noforwards,
		Signatures:        signatures,
		PreHistoryHidden:  preHistoryHidden,
		SlowmodeSeconds:   slowmodeSeconds,
		ParticipantsCount: participantsCount,
		AdminsCount:       adminsCount,
		KickedCount:       kickedCount,
		BannedCount:       bannedCount,
		TopMessageID:      topMessageID,
		PinnedMessageID:   pinnedMessageID,
		Pts:               pts,
		TTLPeriod:         ttlPeriod,
		Date:              date,
		Deleted:           deleted,
	}
	_ = json.Unmarshal([]byte(defaultRights), &ch.DefaultBannedRights)
	return ch
}

func mergeEventChannels(items ...domain.Channel) []domain.Channel {
	channels := make([]domain.Channel, 0, len(items))
	seen := make(map[int64]struct{}, len(items))
	for _, ch := range items {
		if ch.ID == 0 {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		channels = append(channels, ch)
	}
	return channels
}

type eventPeerJSON struct {
	Type string `json:"type"`
	ID   int64  `json:"id"`
}

func encodeEventPeers(peers []domain.Peer) ([]byte, error) {
	if len(peers) == 0 {
		return []byte("[]"), nil
	}
	wire := make([]eventPeerJSON, 0, len(peers))
	for _, peer := range peers {
		if peer.ID == 0 {
			continue
		}
		wire = append(wire, eventPeerJSON{Type: string(peer.Type), ID: peer.ID})
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal event peers: %w", err)
	}
	return raw, nil
}

func decodeEventPeers(raw string) ([]domain.Peer, error) {
	if raw == "" {
		return nil, nil
	}
	var wire []eventPeerJSON
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return nil, err
	}
	out := make([]domain.Peer, 0, len(wire))
	for _, peer := range wire {
		if peer.ID == 0 {
			continue
		}
		out = append(out, domain.Peer{Type: domain.PeerType(peer.Type), ID: peer.ID})
	}
	return out, nil
}

func encodeEventMessageIDs(ids []int) ([]byte, error) {
	if len(ids) == 0 {
		return []byte("[]"), nil
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("marshal event message ids: %w", err)
	}
	return raw, nil
}

func decodeEventMessageIDs(raw string) ([]int, error) {
	if raw == "" {
		return nil, nil
	}
	var ids []int
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func encodeEventDialogFilter(folder *domain.DialogFolder) ([]byte, error) {
	if folder == nil {
		return []byte("{}"), nil
	}
	raw, err := json.Marshal(folder)
	if err != nil {
		return nil, fmt.Errorf("marshal event dialog filter: %w", err)
	}
	return raw, nil
}

func decodeEventDialogFilter(raw string) (*domain.DialogFolder, error) {
	if raw == "" || raw == "{}" {
		return nil, nil
	}
	var folder domain.DialogFolder
	if err := json.Unmarshal([]byte(raw), &folder); err != nil {
		return nil, err
	}
	return &folder, nil
}

func encodeEventFilterOrder(order []int) ([]byte, error) {
	if len(order) == 0 {
		return []byte("[]"), nil
	}
	raw, err := json.Marshal(order)
	if err != nil {
		return nil, fmt.Errorf("marshal event filter order: %w", err)
	}
	return raw, nil
}

func decodeEventFilterOrder(raw string) ([]int, error) {
	if raw == "" {
		return nil, nil
	}
	var order []int
	if err := json.Unmarshal([]byte(raw), &order); err != nil {
		return nil, err
	}
	return order, nil
}

func encodeEventFolderPeers(peers []domain.FolderPeerUpdate) ([]byte, error) {
	if len(peers) == 0 {
		return []byte("[]"), nil
	}
	raw, err := json.Marshal(peers)
	if err != nil {
		return nil, fmt.Errorf("marshal event folder peers: %w", err)
	}
	return raw, nil
}

func decodeEventFolderPeers(raw string) ([]domain.FolderPeerUpdate, error) {
	if raw == "" {
		return nil, nil
	}
	var peers []domain.FolderPeerUpdate
	if err := json.Unmarshal([]byte(raw), &peers); err != nil {
		return nil, err
	}
	return peers, nil
}

type peerSettingsJSON struct {
	AddContact            bool `json:"add_contact,omitempty"`
	BlockContact          bool `json:"block_contact,omitempty"`
	ShareContact          bool `json:"share_contact,omitempty"`
	NeedContactsException bool `json:"need_contacts_exception,omitempty"`
	HiddenPeerSettingsBar bool `json:"hidden_peer_settings_bar,omitempty"`
}

func encodePeerSettings(settings domain.PeerSettings) ([]byte, error) {
	raw, err := json.Marshal(peerSettingsJSON{
		AddContact:            settings.AddContact,
		BlockContact:          settings.BlockContact,
		ShareContact:          settings.ShareContact,
		NeedContactsException: settings.NeedContactsException,
		HiddenPeerSettingsBar: settings.HiddenPeerSettingsBar,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal peer settings: %w", err)
	}
	return raw, nil
}

func decodePeerSettings(raw string) (domain.PeerSettings, error) {
	if raw == "" {
		return domain.PeerSettings{}, nil
	}
	var wire peerSettingsJSON
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return domain.PeerSettings{}, err
	}
	return domain.PeerSettings{
		AddContact:            wire.AddContact,
		BlockContact:          wire.BlockContact,
		ShareContact:          wire.ShareContact,
		NeedContactsException: wire.NeedContactsException,
		HiddenPeerSettingsBar: wire.HiddenPeerSettingsBar,
	}, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
