package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

const channelDialogQueryLimit = 500
const channelMemberFilterBatch = 1000
const retryableChannelTxAttempts = 3

// ChannelStore 用 PostgreSQL 实现 store.ChannelStore。
type ChannelStore struct {
	db     sqlcgen.DBTX
	ids    store.ChannelIDAllocator
	pts    store.ChannelPtsAllocator
	msgIDs store.ChannelMessageIDAllocator
}

// ChannelStoreOption 调整 PostgreSQL ChannelStore 依赖。
type ChannelStoreOption func(*ChannelStore)

// WithChannelAllocators 注入 Redis-backed channel id / pts / message id allocator。
func WithChannelAllocators(ids store.ChannelIDAllocator, pts store.ChannelPtsAllocator, msgIDs store.ChannelMessageIDAllocator) ChannelStoreOption {
	return func(s *ChannelStore) {
		s.ids = ids
		s.pts = pts
		s.msgIDs = msgIDs
	}
}

// NewChannelStore 基于 pgx 连接池（或事务）创建 ChannelStore。
func NewChannelStore(db sqlcgen.DBTX, opts ...ChannelStoreOption) *ChannelStore {
	s := &ChannelStore{db: db}
	for _, opt := range opts {
		opt(s)
	}
	if s.ids == nil {
		s.ids = pgChannelIDAllocator{db: db}
	}
	if s.pts == nil {
		s.pts = pgChannelPtsAllocator{db: db}
	}
	if s.msgIDs == nil {
		s.msgIDs = pgChannelMessageIDAllocator{db: db}
	}
	return s
}

func (s *ChannelStore) CreateChannel(ctx context.Context, req domain.CreateChannelRequest) (domain.CreateChannelResult, error) {
	if req.CreatorUserID == 0 || strings.TrimSpace(req.Title) == "" {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if !req.Broadcast && !req.Megagroup {
		req.Broadcast = true
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.CreateChannelResult{}, fmt.Errorf("create channel: db does not support transactions")
	}
	channelID, err := s.ids.NextChannelID(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("allocate channel id: %w", err)
	}
	accessHash, err := randomChannelAccessHash()
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	msgID, err := s.msgIDs.NextChannelMessageID(ctx, channelID)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("allocate channel message id: %w", err)
	}
	pts, err := s.pts.NextChannelPts(ctx, channelID)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("allocate channel pts: %w", err)
	}
	date := req.Date
	if date == 0 {
		date = nowUnix()
	}
	members := []domain.ChannelMember{creatorChannelMember(channelID, req.CreatorUserID, date)}
	for _, userID := range uniqueChannelUserIDs(req.MemberUserIDs, req.CreatorUserID) {
		members = append(members, domain.ChannelMember{
			ChannelID:     channelID,
			UserID:        userID,
			InviterUserID: req.CreatorUserID,
			Role:          domain.ChannelRoleMember,
			Status:        domain.ChannelMemberActive,
			JoinedAt:      date,
		})
	}
	channel := domain.Channel{
		ID:                channelID,
		AccessHash:        accessHash,
		CreatorUserID:     req.CreatorUserID,
		Title:             strings.TrimSpace(req.Title),
		About:             req.About,
		Broadcast:         req.Broadcast,
		Megagroup:         req.Megagroup,
		Forum:             req.Forum,
		ForumTabs:         req.ForumTabs,
		ParticipantsCount: len(members),
		AdminsCount:       1,
		TopMessageID:      msgID,
		Pts:               pts,
		TTLPeriod:         req.TTLPeriod,
		Date:              date,
	}
	msg := domain.ChannelMessage{
		ChannelID:    channelID,
		ID:           msgID,
		SenderUserID: req.CreatorUserID,
		From:         domain.Peer{Type: domain.PeerTypeUser, ID: req.CreatorUserID},
		Date:         date,
		Post:         channel.Broadcast,
		Action:       &domain.ChannelMessageAction{Type: domain.ChannelActionCreate, Title: channel.Title},
		Pts:          pts,
	}
	event := domain.ChannelUpdateEvent{
		ChannelID:    channelID,
		Type:         domain.ChannelUpdateNewMessage,
		Pts:          pts,
		PtsCount:     1,
		Date:         date,
		Message:      msg,
		SenderUserID: req.CreatorUserID,
	}

	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("begin create channel: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	if err := insertChannelTx(ctx, tx, channel); err != nil {
		return domain.CreateChannelResult{}, err
	}
	for _, member := range members {
		if err := upsertChannelMemberTx(ctx, tx, channel, member); err != nil {
			return domain.CreateChannelResult{}, err
		}
	}
	if err := insertChannelMessageTx(ctx, tx, msg); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := insertChannelEventTx(ctx, tx, event); err != nil {
		return domain.CreateChannelResult{}, err
	}
	for _, member := range members {
		readMax := 0
		if member.UserID == req.CreatorUserID {
			readMax = msgID
		}
		if err := upsertChannelDialogTx(ctx, tx, member.UserID, channel, msg, readMax, readMax); err != nil {
			return domain.CreateChannelResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("commit create channel: %w", err)
	}
	committed = true
	return domain.CreateChannelResult{
		Channel:    channel,
		Members:    append([]domain.ChannelMember(nil), members...),
		Message:    msg,
		Event:      event,
		Recipients: channelMemberIDs(members),
	}, nil
}

func (s *ChannelStore) GetChannel(ctx context.Context, viewerUserID, channelID int64) (domain.ChannelView, error) {
	channel, member, preview, err := s.getChannelForViewer(ctx, s.db, viewerUserID, channelID)
	if err != nil {
		return domain.ChannelView{}, err
	}
	if preview {
		return domain.ChannelView{
			Channel: channel,
			Self:    member,
			Dialog:  previewChannelDialog(viewerUserID, channel, member),
		}, nil
	}
	dialog, err := s.getChannelDialog(ctx, s.db, viewerUserID, channel)
	if err != nil {
		return domain.ChannelView{}, err
	}
	return domain.ChannelView{Channel: channel, Self: member, Dialog: dialog}, nil
}

func (s *ChannelStore) SaveChannelDefaultSendAs(ctx context.Context, req domain.SaveChannelDefaultSendAsRequest) (domain.ChannelView, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelView{}, domain.ErrChannelInvalid
	}
	var sendAsType sql.NullString
	var sendAsID sql.NullInt64
	if req.SendAs != nil {
		if req.SendAs.Type != domain.PeerTypeUser && req.SendAs.Type != domain.PeerTypeChannel {
			return domain.ChannelView{}, domain.ErrChannelInvalid
		}
		sendAsType = sql.NullString{String: string(req.SendAs.Type), Valid: true}
		sendAsID = sql.NullInt64{Int64: req.SendAs.ID, Valid: true}
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelView{}, err
	}
	topMessageID := channel.TopMessageID
	if topMessageID <= member.AvailableMinID {
		topMessageID = 0
	}
	if _, err := s.db.Exec(ctx, `
INSERT INTO channel_dialogs (
    user_id, channel_id, top_message_id, top_message_date,
    read_inbox_max_id, read_outbox_max_id,
    default_send_as_peer_type, default_send_as_peer_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    default_send_as_peer_type = EXCLUDED.default_send_as_peer_type,
    default_send_as_peer_id = EXCLUDED.default_send_as_peer_id,
    updated_at = now()`,
		req.UserID,
		req.ChannelID,
		topMessageID,
		channel.Date,
		member.ReadInboxMaxID,
		member.ReadOutboxMaxID,
		sendAsType,
		sendAsID,
	); err != nil {
		return domain.ChannelView{}, fmt.Errorf("save channel default send as: %w", err)
	}
	dialog, err := s.getChannelDialog(ctx, s.db, req.UserID, channel)
	if err != nil {
		return domain.ChannelView{}, err
	}
	return domain.ChannelView{Channel: channel, Self: member, Dialog: dialog}, nil
}

func (s *ChannelStore) GetChannelByID(ctx context.Context, channelID int64) (domain.Channel, error) {
	if channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return getChannelByID(ctx, s.db, channelID)
}

func (s *ChannelStore) GetParticipants(ctx context.Context, viewerUserID, channelID int64, filter domain.ChannelParticipantsFilter, offset, limit int) (domain.ChannelParticipantList, error) {
	channel, viewer, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID)
	if err != nil {
		return domain.ChannelParticipantList{}, err
	}
	if offset < 0 {
		offset = 0
	}
	if offset > domain.MaxChannelParticipantsOffset {
		offset = domain.MaxChannelParticipantsOffset
	}
	if limit <= 0 || limit > domain.MaxChannelParticipantsLimit {
		limit = domain.MaxChannelParticipantsLimit
	}
	count := channel.ParticipantsCount
	if channel.ParticipantsHidden && !isChannelAdmin(viewer) {
		switch filter.Kind {
		case domain.ChannelParticipantsAdmins:
		case domain.ChannelParticipantsBots:
			return domain.ChannelParticipantList{Channel: channel, Count: 0}, nil
		default:
			return domain.ChannelParticipantList{Channel: channel, Count: channel.ParticipantsCount}, nil
		}
	}
	where := []string{"m.channel_id = $1"}
	args := []any{channelID}
	joinUsers := false
	query := strings.TrimSpace(filter.Query)
	switch filter.Kind {
	case "", domain.ChannelParticipantsRecent, domain.ChannelParticipantsContacts, domain.ChannelParticipantsMentions:
		where = append(where, "m.status = 'active'")
	case domain.ChannelParticipantsAdmins:
		where = append(where, "m.status = 'active'", "m.role IN ('creator','admin')")
		count = channel.AdminsCount
	case domain.ChannelParticipantsKicked:
		count = channel.KickedCount
		if !isChannelAdmin(viewer) {
			return domain.ChannelParticipantList{Channel: channel, Count: channel.KickedCount}, nil
		}
		where = append(where, "(m.status = 'kicked' OR (m.banned_rights->>'ViewMessages')::boolean IS TRUE)")
	case domain.ChannelParticipantsBanned:
		count = channel.BannedCount
		if !isChannelAdmin(viewer) {
			return domain.ChannelParticipantList{Channel: channel, Count: channel.BannedCount}, nil
		}
		where = append(where, "m.status <> 'kicked'", `(
    m.status = 'banned' OR
    (m.banned_rights->>'SendMessages')::boolean IS TRUE OR
    (m.banned_rights->>'SendMedia')::boolean IS TRUE OR
    (m.banned_rights->>'SendStickers')::boolean IS TRUE OR
    (m.banned_rights->>'SendGifs')::boolean IS TRUE OR
    (m.banned_rights->>'SendGames')::boolean IS TRUE OR
    (m.banned_rights->>'SendInline')::boolean IS TRUE OR
    (m.banned_rights->>'EmbedLinks')::boolean IS TRUE OR
    (m.banned_rights->>'SendPolls')::boolean IS TRUE OR
    (m.banned_rights->>'ChangeInfo')::boolean IS TRUE OR
    (m.banned_rights->>'InviteUsers')::boolean IS TRUE OR
    (m.banned_rights->>'PinMessages')::boolean IS TRUE
)`)
	case domain.ChannelParticipantsSearch:
		where = append(where, "m.status = 'active'")
		count = 0
	case domain.ChannelParticipantsBots:
		return domain.ChannelParticipantList{Channel: channel}, nil
	default:
		where = append(where, "m.status = 'active'")
	}
	if query != "" {
		joinUsers = true
		count = 0
		args = append(args, "%"+strings.ToLower(query)+"%")
		placeholder := fmt.Sprintf("$%d", len(args))
		where = append(where, fmt.Sprintf(`(
    lower(COALESCE(u.first_name, '')) LIKE %s OR
    lower(COALESCE(u.last_name, '')) LIKE %s OR
    lower(COALESCE(u.username, '')) LIKE %s OR
    COALESCE(u.phone, '') LIKE %s OR
    m.user_id::text LIKE %s
)`, placeholder, placeholder, placeholder, placeholder, placeholder))
	}
	args = append(args, offset, limit)
	offsetArg := fmt.Sprintf("$%d", len(args)-1)
	limitArg := fmt.Sprintf("$%d", len(args))
	from := "FROM channel_members m"
	if joinUsers {
		from += " JOIN users u ON u.id = m.user_id"
	}
	rows, err := s.db.Query(ctx, `
SELECT channel_id, user_id, inviter_user_id, role, status, joined_at, left_at, admin_rights::text, banned_rights::text,
       rank, available_min_id, available_min_pts, read_inbox_max_id, read_outbox_max_id, unread_mark, slowmode_last_send_date
`+from+`
WHERE `+strings.Join(where, " AND ")+`
ORDER BY CASE role WHEN 'creator' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END, user_id
OFFSET `+offsetArg+` LIMIT `+limitArg, args...)
	if err != nil {
		return domain.ChannelParticipantList{}, fmt.Errorf("list channel participants: %w", err)
	}
	defer rows.Close()
	out := domain.ChannelParticipantList{Channel: channel, Count: count}
	for rows.Next() {
		member, err := scanChannelMember(rows)
		if err != nil {
			return domain.ChannelParticipantList{}, err
		}
		out.Participants = append(out.Participants, member)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelParticipantList{}, err
	}
	if out.Count == 0 {
		out.Count = len(out.Participants)
	}
	return out, nil
}

func (s *ChannelStore) GetParticipant(ctx context.Context, viewerUserID, channelID, participantUserID int64) (domain.ChannelMember, error) {
	if _, _, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID); err != nil {
		return domain.ChannelMember{}, err
	}
	return s.getChannelMember(ctx, s.db, channelID, participantUserID)
}

func (s *ChannelStore) InviteToChannel(ctx context.Context, channelID, inviterUserID int64, userIDs []int64, date int) (domain.CreateChannelResult, error) {
	if channelID == 0 || inviterUserID == 0 || len(userIDs) == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.CreateChannelResult{}, fmt.Errorf("invite channel: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("begin invite channel: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, date)
		}
	}()
	channel, inviter, err := s.getChannelForMember(ctx, tx, inviterUserID, channelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	if !canInviteToChannel(channel, inviter) {
		return domain.CreateChannelResult{}, domain.ErrChannelAdminRequired
	}
	if date == 0 {
		date = nowUnix()
	}
	requested := uniqueChannelUserIDs(userIDs, 0)
	inviteOne := len(requested) == 1
	canRestoreKicked := canBanChannelUsers(inviter)
	invitedIDs := make([]int64, 0, len(requested))
	members := make([]domain.ChannelMember, 0, len(requested))
	restoredKicked := 0
	for _, userID := range requested {
		if existing, err := s.getChannelMember(ctx, tx, channelID, userID); err == nil {
			if existing.Status == domain.ChannelMemberActive {
				if inviteOne {
					return domain.CreateChannelResult{}, domain.ErrUserAlreadyParticipant
				}
				continue
			}
			if existing.Status == domain.ChannelMemberBanned || existing.Status == domain.ChannelMemberKicked || existing.BannedRights.ViewMessages {
				if !canRestoreKicked {
					if inviteOne {
						return domain.CreateChannelResult{}, domain.ErrUserKicked
					}
					continue
				}
				if existing.Status == domain.ChannelMemberKicked {
					restoredKicked++
				}
			}
		} else if !errors.Is(err, domain.ErrChannelPrivate) {
			return domain.CreateChannelResult{}, err
		}
		member := domain.ChannelMember{
			ChannelID:       channelID,
			UserID:          userID,
			InviterUserID:   inviterUserID,
			Role:            domain.ChannelRoleMember,
			Status:          domain.ChannelMemberActive,
			JoinedAt:        date,
			AvailableMinID:  channelInitialAvailableMinID(channel),
			AvailableMinPts: channelInitialAvailableMinPts(channel),
			ReadInboxMaxID:  channel.TopMessageID,
		}
		if err := upsertChannelMemberTx(ctx, tx, channel, member); err != nil {
			return domain.CreateChannelResult{}, err
		}
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID:   channelID,
			UserID:      inviterUserID,
			Date:        date,
			Type:        domain.ChannelAdminLogParticipantInvite,
			Participant: &member,
		}); err != nil {
			return domain.CreateChannelResult{}, err
		}
		members = append(members, member)
		invitedIDs = append(invitedIDs, userID)
	}
	if len(members) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE channels SET participants_count = participants_count + $2, kicked_count = GREATEST(kicked_count - $3, 0), updated_at = now() WHERE id = $1`, channelID, len(members), restoredKicked); err != nil {
			return domain.CreateChannelResult{}, fmt.Errorf("update channel participants: %w", err)
		}
		channel.ParticipantsCount += len(members)
		channel.KickedCount = maxInt(channel.KickedCount-restoredKicked, 0)
	}
	var msg domain.ChannelMessage
	var event domain.ChannelUpdateEvent
	if len(members) > 0 && channel.Megagroup {
		msg, event, err = s.insertServiceMessage(ctx, tx, channel, inviterUserID, date, domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatAddUser,
			UserIDs: invitedIDs,
		}, &reserved)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		channel.TopMessageID = msg.ID
		channel.Pts = event.Pts
	}
	for _, member := range members {
		if err := upsertChannelDialogTx(ctx, tx, member.UserID, channel, msg, member.ReadInboxMaxID, member.ReadOutboxMaxID); err != nil {
			return domain.CreateChannelResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("commit invite channel: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, inviterUserID, channelID, 0)
	return domain.CreateChannelResult{Channel: channel, Members: members, Message: msg, Event: event, Recipients: recipients}, nil
}

func (s *ChannelStore) JoinChannel(ctx context.Context, channelID, userID int64, date int) (domain.CreateChannelResult, error) {
	if channelID == 0 || userID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.CreateChannelResult{}, fmt.Errorf("join channel: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("begin join channel: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, date)
		}
	}()
	channel, err := getChannelByID(ctx, tx, channelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	if existing, err := s.getChannelMember(ctx, tx, channelID, userID); err == nil {
		switch {
		case existing.Status == domain.ChannelMemberActive:
			return domain.CreateChannelResult{}, domain.ErrUserAlreadyParticipant
		case existing.Status == domain.ChannelMemberBanned || existing.Status == domain.ChannelMemberKicked || existing.BannedRights.ViewMessages:
			return domain.CreateChannelResult{}, domain.ErrChannelUserBanned
		}
	}
	if date == 0 {
		date = nowUnix()
	}
	if channel.JoinRequest {
		if err := s.recordPublicJoinRequestTx(ctx, tx, channel, userID, date); err != nil {
			return domain.CreateChannelResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.CreateChannelResult{}, fmt.Errorf("commit public channel join request: %w", err)
		}
		committed = true
		return domain.CreateChannelResult{Channel: channel}, domain.ErrInviteRequestSent
	}
	preJoinTopID := channel.TopMessageID
	minID := channelInitialAvailableMinID(channel)
	member := domain.ChannelMember{ChannelID: channelID, UserID: userID, Role: domain.ChannelRoleMember, Status: domain.ChannelMemberActive, JoinedAt: date, AvailableMinID: minID, AvailableMinPts: channelInitialAvailableMinPts(channel), ReadInboxMaxID: maxInt(minID, preJoinTopID)}
	if err := upsertChannelMemberTx(ctx, tx, channel, member); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID: channelID,
		UserID:    userID,
		Date:      date,
		Type:      domain.ChannelAdminLogParticipantJoin,
	}); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET participants_count = participants_count + 1, updated_at = now() WHERE id = $1`, channelID); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("update channel participants: %w", err)
	}
	channel.ParticipantsCount++
	var msg domain.ChannelMessage
	var event domain.ChannelUpdateEvent
	if channel.Megagroup {
		msg, event, err = s.insertServiceMessage(ctx, tx, channel, userID, date, domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatJoined,
			UserIDs: []int64{userID},
		}, &reserved)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		channel.TopMessageID = msg.ID
		channel.Pts = event.Pts
	}
	member.ReadInboxMaxID = maxInt(member.ReadInboxMaxID, channel.TopMessageID)
	if msg.ID != 0 && msg.SenderUserID == userID {
		member.ReadOutboxMaxID = maxInt(member.ReadOutboxMaxID, msg.ID)
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_members
SET read_inbox_max_id = GREATEST(read_inbox_max_id, $3),
    read_outbox_max_id = GREATEST(read_outbox_max_id, $4),
    unread_mark = false,
    updated_at = now()
WHERE channel_id = $1 AND user_id = $2`, channelID, userID, member.ReadInboxMaxID, member.ReadOutboxMaxID); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("update joined channel read watermarks: %w", err)
	}
	if err := upsertChannelDialogTx(ctx, tx, userID, channel, msg, member.ReadInboxMaxID, member.ReadOutboxMaxID); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("commit join channel: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, userID, channelID, 0)
	return domain.CreateChannelResult{Channel: channel, Members: []domain.ChannelMember{member}, Message: msg, Event: event, Recipients: recipients}, nil
}

func (s *ChannelStore) LeaveChannel(ctx context.Context, channelID, userID int64, date int) (domain.CreateChannelResult, error) {
	if channelID == 0 || userID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.CreateChannelResult{}, fmt.Errorf("leave channel: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("begin leave channel: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, date)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	if date == 0 {
		date = nowUnix()
	}
	if _, err := tx.Exec(ctx, `UPDATE channel_members SET status = 'left', left_at = $3, updated_at = now() WHERE channel_id = $1 AND user_id = $2`, channelID, userID, date); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("leave channel member: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET participants_count = GREATEST(participants_count - 1, 0), updated_at = now() WHERE id = $1`, channelID); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("update channel participants: %w", err)
	}
	member.Status = domain.ChannelMemberLeft
	member.LeftAt = date
	if err := upsertUserChannelMemberIndexTx(ctx, tx, channel, member); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID: channelID,
		UserID:    userID,
		Date:      date,
		Type:      domain.ChannelAdminLogParticipantLeave,
	}); err != nil {
		return domain.CreateChannelResult{}, err
	}
	channel.ParticipantsCount--
	if channel.ParticipantsCount < 0 {
		channel.ParticipantsCount = 0
	}
	var msg domain.ChannelMessage
	var event domain.ChannelUpdateEvent
	if channel.Megagroup {
		msg, event, err = s.insertServiceMessage(ctx, tx, channel, userID, date, domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatDelete,
			UserIDs: []int64{userID},
		}, &reserved)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		channel.TopMessageID = msg.ID
		channel.Pts = event.Pts
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("commit leave channel: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, userID, channelID, 0)
	recipients = append(recipients, userID)
	return domain.CreateChannelResult{Channel: channel, Members: []domain.ChannelMember{member}, Message: msg, Event: event, Recipients: recipients}, nil
}

func (s *ChannelStore) EditChannelTitle(ctx context.Context, req domain.EditChannelTitleRequest) (domain.EditChannelTitleResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || strings.TrimSpace(req.Title) == "" {
		return domain.EditChannelTitleResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.EditChannelTitleResult{}, fmt.Errorf("edit channel title: db does not support transactions")
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	title := strings.TrimSpace(req.Title)
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.EditChannelTitleResult{}, fmt.Errorf("begin edit channel title: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelTitleResult{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.EditChannelTitleResult{}, domain.ErrChannelAdminRequired
	}
	if channel.Title == title {
		return domain.EditChannelTitleResult{}, domain.ErrChannelNotModified
	}
	prevTitle := channel.Title
	if _, err := tx.Exec(ctx, `UPDATE channels SET title = $2, updated_at = now() WHERE id = $1`, req.ChannelID, title); err != nil {
		return domain.EditChannelTitleResult{}, fmt.Errorf("update channel title: %w", err)
	}
	channel.Title = title
	msg, event, err := s.insertServiceMessage(ctx, tx, channel, req.UserID, req.Date, domain.ChannelMessageAction{
		Type:  domain.ChannelActionEditTitle,
		Title: title,
	}, &reserved)
	if err != nil {
		return domain.EditChannelTitleResult{}, err
	}
	channel.TopMessageID = msg.ID
	channel.Pts = event.Pts
	if err := upsertChannelDialogTx(ctx, tx, req.UserID, channel, msg, msg.ID, msg.ID); err != nil {
		return domain.EditChannelTitleResult{}, err
	}
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID:  req.ChannelID,
		UserID:     req.UserID,
		Date:       req.Date,
		Type:       domain.ChannelAdminLogChangeTitle,
		PrevString: prevTitle,
		NewString:  title,
	}); err != nil {
		return domain.EditChannelTitleResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.EditChannelTitleResult{}, fmt.Errorf("commit edit channel title: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	return domain.EditChannelTitleResult{Channel: channel, Message: msg, Event: event, Recipients: recipients}, nil
}

func (s *ChannelStore) EditChannelAbout(ctx context.Context, req domain.EditChannelAboutRequest) (domain.Channel, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("edit channel about: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin edit channel about: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET about = $2, updated_at = now() WHERE id = $1`, req.ChannelID, req.About); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel about: %w", err)
	}
	channel.About = req.About
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit edit channel about: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) EditChannelAdmin(ctx context.Context, req domain.EditChannelAdminRequest) (domain.EditChannelAdminResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MemberID == 0 {
		return domain.EditChannelAdminResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.EditChannelAdminResult{}, fmt.Errorf("edit channel admin: db does not support transactions")
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.EditChannelAdminResult{}, fmt.Errorf("begin edit channel admin: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, actor, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelAdminResult{}, err
	}
	if !canAddChannelAdmins(actor) {
		return domain.EditChannelAdminResult{}, domain.ErrChannelAdminRequired
	}
	if actor.Role != domain.ChannelRoleCreator && !adminRightsSubset(req.AdminRights, actor.AdminRights) {
		return domain.EditChannelAdminResult{}, domain.ErrChannelRightForbidden
	}
	previous, err := s.getChannelMember(ctx, tx, req.ChannelID, req.MemberID)
	if err != nil {
		if !errors.Is(err, domain.ErrChannelPrivate) {
			return domain.EditChannelAdminResult{}, err
		}
		previous = domain.ChannelMember{
			ChannelID:       req.ChannelID,
			UserID:          req.MemberID,
			InviterUserID:   req.UserID,
			Role:            domain.ChannelRoleMember,
			Status:          domain.ChannelMemberActive,
			JoinedAt:        req.Date,
			AvailableMinID:  channelInitialAvailableMinID(channel),
			AvailableMinPts: channelInitialAvailableMinPts(channel),
			ReadInboxMaxID:  channel.TopMessageID,
		}
	}
	if previous.Role == domain.ChannelRoleCreator {
		return domain.EditChannelAdminResult{}, domain.ErrChannelUserCreator
	}
	member := previous
	member.InviterUserID = req.UserID
	member.Status = domain.ChannelMemberActive
	member.LeftAt = 0
	member.Rank = req.Rank
	if previous.Status != domain.ChannelMemberActive {
		if minPts := channelInitialAvailableMinPts(channel); minPts > member.AvailableMinPts {
			member.AvailableMinPts = minPts
		}
	}
	member.AdminRights = req.AdminRights
	if zeroChannelAdminRights(req.AdminRights) {
		member.Role = domain.ChannelRoleMember
		member.Rank = ""
	} else {
		member.Role = domain.ChannelRoleAdmin
	}
	if err := upsertChannelMemberTx(ctx, tx, channel, member); err != nil {
		return domain.EditChannelAdminResult{}, err
	}
	logType := domain.ChannelAdminLogParticipantPromote
	if member.Role != domain.ChannelRoleAdmin {
		logType = domain.ChannelAdminLogParticipantDemote
	}
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID:       req.ChannelID,
		UserID:          req.UserID,
		Date:            req.Date,
		Type:            logType,
		PrevParticipant: &previous,
		NewParticipant:  &member,
	}); err != nil {
		return domain.EditChannelAdminResult{}, err
	}
	channel, err = refreshChannelCountsTx(ctx, tx, channel)
	if err != nil {
		return domain.EditChannelAdminResult{}, err
	}
	event, channel, err := s.insertParticipantEventTx(ctx, tx, channel, req.UserID, previous, member, req.Date, &reserved)
	if err != nil {
		return domain.EditChannelAdminResult{}, err
	}
	msg, _ := s.getChannelMessage(ctx, tx, req.ChannelID, channel.TopMessageID)
	if err := upsertChannelDialogTx(ctx, tx, member.UserID, channel, msg, member.ReadInboxMaxID, member.ReadOutboxMaxID); err != nil {
		return domain.EditChannelAdminResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.EditChannelAdminResult{}, fmt.Errorf("commit edit channel admin: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	recipients = append(recipients, req.MemberID)
	return domain.EditChannelAdminResult{Channel: channel, Previous: previous, Participant: member, Event: event, Recipients: recipients, Date: req.Date}, nil
}

func (s *ChannelStore) EditChannelBanned(ctx context.Context, req domain.EditChannelBannedRequest) (domain.EditChannelBannedResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.Participant.Type != domain.PeerTypeUser || req.Participant.ID == 0 {
		return domain.EditChannelBannedResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.EditChannelBannedResult{}, fmt.Errorf("edit channel banned: db does not support transactions")
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.EditChannelBannedResult{}, fmt.Errorf("begin edit channel banned: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, actor, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelBannedResult{}, err
	}
	if !canBanChannelUsers(actor) {
		return domain.EditChannelBannedResult{}, domain.ErrChannelAdminRequired
	}
	previous, err := s.getChannelMember(ctx, tx, req.ChannelID, req.Participant.ID)
	if err != nil {
		if !errors.Is(err, domain.ErrChannelPrivate) {
			return domain.EditChannelBannedResult{}, err
		}
		previous = domain.ChannelMember{
			ChannelID:     req.ChannelID,
			UserID:        req.Participant.ID,
			InviterUserID: req.UserID,
			Role:          domain.ChannelRoleMember,
			Status:        domain.ChannelMemberLeft,
		}
	}
	if previous.Role == domain.ChannelRoleCreator {
		return domain.EditChannelBannedResult{}, domain.ErrChannelUserCreator
	}
	member := previous
	member.BannedRights = req.BannedRights
	member.Role = domain.ChannelRoleMember
	switch {
	case req.BannedRights.ViewMessages:
		member.InviterUserID = req.UserID
		member.Status = domain.ChannelMemberKicked
		member.LeftAt = req.Date
	case zeroChannelBannedRights(req.BannedRights):
		if previous.Status == domain.ChannelMemberActive {
			member.Status = domain.ChannelMemberActive
		} else {
			member.Status = domain.ChannelMemberLeft
		}
		member.LeftAt = 0
	default:
		member.InviterUserID = req.UserID
		if previous.Status == domain.ChannelMemberActive {
			member.Status = domain.ChannelMemberActive
		} else {
			member.Status = domain.ChannelMemberBanned
		}
	}
	if member.JoinedAt == 0 && member.Status == domain.ChannelMemberActive {
		member.JoinedAt = req.Date
	}
	if err := upsertChannelMemberTx(ctx, tx, channel, member); err != nil {
		return domain.EditChannelBannedResult{}, err
	}
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID:       req.ChannelID,
		UserID:          req.UserID,
		Date:            req.Date,
		Type:            adminLogBanType(previous, member),
		PrevParticipant: &previous,
		NewParticipant:  &member,
	}); err != nil {
		return domain.EditChannelBannedResult{}, err
	}
	channel, err = refreshChannelCountsTx(ctx, tx, channel)
	if err != nil {
		return domain.EditChannelBannedResult{}, err
	}
	event, channel, err := s.insertParticipantEventTx(ctx, tx, channel, req.UserID, previous, member, req.Date, &reserved)
	if err != nil {
		return domain.EditChannelBannedResult{}, err
	}
	if member.Status == domain.ChannelMemberActive {
		msg, _ := s.getChannelMessage(ctx, tx, req.ChannelID, channel.TopMessageID)
		if err := upsertChannelDialogTx(ctx, tx, member.UserID, channel, msg, member.ReadInboxMaxID, member.ReadOutboxMaxID); err != nil {
			return domain.EditChannelBannedResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.EditChannelBannedResult{}, fmt.Errorf("commit edit channel banned: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	recipients = append(recipients, req.Participant.ID)
	return domain.EditChannelBannedResult{Channel: channel, Previous: previous, Participant: member, Event: event, Recipients: recipients, Date: req.Date}, nil
}

func (s *ChannelStore) EditChannelDefaultBannedRights(ctx context.Context, req domain.EditChannelDefaultBannedRightsRequest) (domain.Channel, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	channel, actor, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canBanChannelUsers(actor) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if channel.DefaultBannedRights == req.BannedRights {
		return domain.Channel{}, domain.ErrChannelNotModified
	}
	rights, err := marshalJSON(req.BannedRights, "{}")
	if err != nil {
		return domain.Channel{}, err
	}
	if _, err := s.db.Exec(ctx, `
UPDATE channels
SET default_banned_rights = $2::jsonb, updated_at = now()
WHERE id = $1 AND NOT deleted`, req.ChannelID, rights); err != nil {
		return domain.Channel{}, fmt.Errorf("edit channel default banned rights: %w", err)
	}
	channel.DefaultBannedRights = req.BannedRights
	return channel, nil
}

func (s *ChannelStore) DeleteChannel(ctx context.Context, req domain.DeleteChannelRequest) (domain.DeleteChannelResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.DeleteChannelResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.DeleteChannelResult{}, fmt.Errorf("delete channel: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.DeleteChannelResult{}, fmt.Errorf("begin delete channel: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelResult{}, err
	}
	if member.Role != domain.ChannelRoleCreator {
		return domain.DeleteChannelResult{}, domain.ErrChannelAdminRequired
	}
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	if _, err := tx.Exec(ctx, `UPDATE channels SET deleted = true, updated_at = now() WHERE id = $1`, req.ChannelID); err != nil {
		return domain.DeleteChannelResult{}, fmt.Errorf("mark channel deleted: %w", err)
	}
	if err := markUserChannelMemberIndexDeletedTx(ctx, tx, req.ChannelID, true); err != nil {
		return domain.DeleteChannelResult{}, err
	}
	channel.Deleted = true
	if err := tx.Commit(ctx); err != nil {
		return domain.DeleteChannelResult{}, fmt.Errorf("commit delete channel: %w", err)
	}
	committed = true
	return domain.DeleteChannelResult{Channel: channel, Recipients: recipients}, nil
}

func (s *ChannelStore) CheckUsername(ctx context.Context, userID, channelID int64, username string) (bool, error) {
	if userID == 0 || channelID == 0 || strings.TrimSpace(username) == "" {
		return false, domain.ErrChannelInvalid
	}
	if _, _, err := s.getChannelForMember(ctx, s.db, userID, channelID); err != nil {
		return false, err
	}
	usernameLower := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(username, "@")))
	var existingChannelID int64
	err := s.db.QueryRow(ctx, `SELECT channel_id FROM channel_usernames WHERE username_lower = $1`, usernameLower).Scan(&existingChannelID)
	if err == nil {
		return existingChannelID == channelID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("check channel username: %w", err)
	}
	var userIDWithUsername int64
	err = s.db.QueryRow(ctx, `SELECT id FROM users WHERE lower(username) = $1 AND username <> '' LIMIT 1`, usernameLower).Scan(&userIDWithUsername)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("check channel username user collision: %w", err)
	}
	return true, nil
}

func (s *ChannelStore) UpdateUsername(ctx context.Context, req domain.UpdateChannelUsernameRequest) (domain.Channel, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("update channel username: db does not support transactions")
	}
	username := strings.TrimSpace(strings.TrimPrefix(req.Username, "@"))
	usernameLower := strings.ToLower(username)
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin update channel username: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if member.Role != domain.ChannelRoleCreator {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if strings.EqualFold(channel.Username, username) {
		return domain.Channel{}, domain.ErrChannelNotModified
	}
	if usernameLower != "" {
		var userIDWithUsername int64
		err := tx.QueryRow(ctx, `SELECT id FROM users WHERE lower(username) = $1 AND username <> '' LIMIT 1`, usernameLower).Scan(&userIDWithUsername)
		if err == nil {
			return domain.Channel{}, domain.ErrUsernameOccupied
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return domain.Channel{}, fmt.Errorf("check user username collision: %w", err)
		}
		var existingChannelID int64
		err = tx.QueryRow(ctx, `SELECT channel_id FROM channel_usernames WHERE username_lower = $1 FOR UPDATE`, usernameLower).Scan(&existingChannelID)
		if err == nil && existingChannelID != req.ChannelID {
			return domain.Channel{}, domain.ErrUsernameOccupied
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return domain.Channel{}, fmt.Errorf("lock channel username: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM channel_usernames WHERE channel_id = $1`, req.ChannelID); err != nil {
		return domain.Channel{}, fmt.Errorf("delete old channel username: %w", err)
	}
	if usernameLower != "" {
		if _, err := tx.Exec(ctx, `
INSERT INTO channel_usernames (username_lower, channel_id)
VALUES ($1,$2)
ON CONFLICT (username_lower) DO UPDATE SET channel_id = EXCLUDED.channel_id, updated_at = now()
WHERE channel_usernames.channel_id = EXCLUDED.channel_id`, usernameLower, req.ChannelID); err != nil {
			if isUniqueViolation(err) {
				return domain.Channel{}, domain.ErrUsernameOccupied
			}
			return domain.Channel{}, fmt.Errorf("insert channel username: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET username = NULLIF($2,''), updated_at = now() WHERE id = $1`, req.ChannelID, username); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel username: %w", err)
	}
	prevUsername := channel.Username
	channel.Username = username
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID:  req.ChannelID,
		UserID:     req.UserID,
		Date:       nowUnix(),
		Type:       domain.ChannelAdminLogChangeUsername,
		PrevString: prevUsername,
		NewString:  username,
	}); err != nil {
		return domain.Channel{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit update channel username: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) ListAdminedPublicChannels(ctx context.Context, userID int64) ([]domain.Channel, error) {
	if userID == 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
SELECT `+channelColumns+`
FROM channel_members m
JOIN channels c ON c.id = m.channel_id AND NOT c.deleted
WHERE m.user_id = $1
  AND m.status = 'active'
  AND m.role IN ('creator','admin')
  AND COALESCE(c.username, '') <> ''
ORDER BY c.id DESC
LIMIT $2`, userID, domain.MaxAdminedPublicChannels)
	if err != nil {
		return nil, fmt.Errorf("list admined public channels: %w", err)
	}
	defer rows.Close()
	out := make([]domain.Channel, 0)
	for rows.Next() {
		ch, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ch)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ChannelStore) ResolvePublicChannelUsername(ctx context.Context, viewerUserID int64, username string) (domain.Channel, bool, error) {
	if viewerUserID == 0 {
		return domain.Channel{}, false, domain.ErrChannelInvalid
	}
	usernameLower := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(username, "@")))
	if usernameLower == "" {
		return domain.Channel{}, false, nil
	}
	ch, err := scanChannel(s.db.QueryRow(ctx, `
SELECT `+channelColumns+`
FROM channel_usernames u
JOIN channels c ON c.id = u.channel_id
WHERE u.username_lower = $1
  AND NOT c.deleted
  AND (c.broadcast OR c.megagroup)
  AND COALESCE(c.username, '') <> ''
LIMIT 1`, usernameLower))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Channel{}, false, nil
		}
		return domain.Channel{}, false, fmt.Errorf("resolve public channel username: %w", err)
	}
	return ch, true, nil
}

func (s *ChannelStore) SearchPublicChannels(ctx context.Context, viewerUserID int64, query string, limit int) (domain.PublicChannelSearchResult, error) {
	if viewerUserID == 0 {
		return domain.PublicChannelSearchResult{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxPublicChannelSearchLimit {
		limit = domain.MaxPublicChannelSearchLimit
	}
	queryLower := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(query, "@")))
	if queryLower == "" {
		return domain.PublicChannelSearchResult{}, nil
	}
	queryPrefix := escapeLike(queryLower) + "%"
	queryLike := "%" + escapeLike(queryLower) + "%"
	rows, err := s.db.Query(ctx, `
SELECT `+channelColumns+`,
       EXISTS (
         SELECT 1
         FROM channel_members m
         WHERE m.channel_id = c.id
           AND m.user_id = $1
           AND m.status = 'active'
       ) AS viewer_member
FROM channels c
WHERE NOT c.deleted
  AND (c.broadcast OR c.megagroup)
  AND COALESCE(c.username, '') <> ''
  AND (
    lower(c.username) = $2
    OR lower(c.username) LIKE $3 ESCAPE '\'
    OR lower(c.title) LIKE $3 ESCAPE '\'
    OR lower(c.username) LIKE $4 ESCAPE '\'
    OR lower(c.title) LIKE $4 ESCAPE '\'
  )
ORDER BY CASE
    WHEN lower(c.username) = $2 THEN 0
    WHEN lower(c.username) LIKE $3 ESCAPE '\' THEN 1
    WHEN lower(c.username) LIKE $4 ESCAPE '\' THEN 2
    WHEN lower(c.title) LIKE $3 ESCAPE '\' THEN 3
    ELSE 4
  END,
  viewer_member DESC,
  c.participants_count DESC,
  c.date DESC,
  c.id DESC
LIMIT $5`, viewerUserID, queryLower, queryPrefix, queryLike, limit)
	if err != nil {
		return domain.PublicChannelSearchResult{}, fmt.Errorf("search public channels: %w", err)
	}
	defer rows.Close()
	out := domain.PublicChannelSearchResult{
		MyResults: make([]domain.Channel, 0),
		Results:   make([]domain.Channel, 0, limit),
	}
	for rows.Next() {
		ch, viewerMember, err := scanChannelWithViewerMember(rows)
		if err != nil {
			return domain.PublicChannelSearchResult{}, err
		}
		if viewerMember {
			out.MyResults = append(out.MyResults, ch)
		} else {
			out.Results = append(out.Results, ch)
		}
	}
	if err := rows.Err(); err != nil {
		return domain.PublicChannelSearchResult{}, err
	}
	return out, nil
}

func (s *ChannelStore) SetSignatures(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel signatures: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel signatures: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.Signatures
	if _, err := tx.Exec(ctx, `UPDATE channels SET signatures = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel signatures: %w", err)
	}
	channel.Signatures = enabled
	if prev != enabled {
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      nowUnix(),
			Type:      domain.ChannelAdminLogToggleSignatures,
			PrevBool:  prev,
			NewBool:   enabled,
		}); err != nil {
			return domain.Channel{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel signatures: %w", err)
	}
	committed = true
	return channel, nil
}

// SetChannelPhoto 设置/清除频道头像（反范式列）。photo==nil 表示清除。
func (s *ChannelStore) SetChannelPhoto(ctx context.Context, userID, channelID int64, photo *domain.Photo) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("set channel photo: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin set channel photo: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	var (
		photoID  int64
		dcID     int
		stripped []byte
	)
	if photo != nil && photo.ID != 0 {
		photoID = photo.ID
		dcID = photo.DCID
		stripped = domain.StrippedFromSizes(photo.Sizes)
	}
	if stripped == nil {
		stripped = []byte{}
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET photo_id = $2, photo_dc_id = $3, photo_stripped = $4, updated_at = now() WHERE id = $1`,
		channelID, photoID, dcID, stripped); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel photo: %w", err)
	}
	channel.PhotoID = photoID
	channel.PhotoDCID = dcID
	channel.PhotoStripped = stripped
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit set channel photo: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetPreHistoryHidden(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel prehistory: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel prehistory: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if member.Role != domain.ChannelRoleCreator {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.PreHistoryHidden
	if _, err := tx.Exec(ctx, `UPDATE channels SET pre_history_hidden = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel prehistory: %w", err)
	}
	channel.PreHistoryHidden = enabled
	if prev != enabled {
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      nowUnix(),
			Type:      domain.ChannelAdminLogTogglePreHistoryHidden,
			PrevBool:  prev,
			NewBool:   enabled,
		}); err != nil {
			return domain.Channel{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel prehistory: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetParticipantsHidden(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel participants hidden: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel participants hidden: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !channel.Megagroup || !canBanChannelUsers(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET participants_hidden = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel participants hidden: %w", err)
	}
	channel.ParticipantsHidden = enabled
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel participants hidden: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetForum(ctx context.Context, userID, channelID int64, enabled, tabs bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel forum: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel forum: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !channel.Megagroup || channel.Broadcast {
		return domain.Channel{}, domain.ErrChannelNotModified
	}
	if member.Role != domain.ChannelRoleCreator {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if enabled && channel.LinkedChatID != 0 {
		return domain.Channel{}, domain.ErrChatDiscussionUnallowed
	}
	prevForum := channel.Forum
	prevTabs := channel.ForumTabs
	nextTabs := enabled && tabs
	if _, err := tx.Exec(ctx, `
UPDATE channels
SET forum = $2,
    forum_tabs = $3,
    updated_at = now()
WHERE id = $1`, channelID, enabled, nextTabs); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel forum: %w", err)
	}
	channel.Forum = enabled
	channel.ForumTabs = nextTabs
	if prevForum != channel.Forum || prevTabs != channel.ForumTabs {
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      nowUnix(),
			Type:      domain.ChannelAdminLogToggleForum,
			PrevBool:  prevForum,
			NewBool:   enabled,
		}); err != nil {
			return domain.Channel{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel forum: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetAutotranslation(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel autotranslation: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel autotranslation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.Autotranslation
	if _, err := tx.Exec(ctx, `UPDATE channels SET autotranslation = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel autotranslation: %w", err)
	}
	channel.Autotranslation = enabled
	if prev != enabled {
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      nowUnix(),
			Type:      domain.ChannelAdminLogToggleAutotranslation,
			PrevBool:  prev,
			NewBool:   enabled,
		}); err != nil {
			return domain.Channel{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel autotranslation: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetRestrictedSponsored(ctx context.Context, userID, channelID int64, restricted bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel restricted sponsored: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel restricted sponsored: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET restricted_sponsored = $2, updated_at = now() WHERE id = $1`, channelID, restricted); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel restricted sponsored: %w", err)
	}
	channel.RestrictedSponsored = restricted
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel restricted sponsored: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetPaidMessagesPrice(ctx context.Context, userID, channelID int64, stars int64, broadcastMessagesAllowed bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 || stars < 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("update channel paid messages price: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin update channel paid messages price: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	broadcastAllowed := channel.Broadcast && broadcastMessagesAllowed
	if _, err := tx.Exec(ctx, `UPDATE channels SET send_paid_messages_stars = $2, broadcast_messages_allowed = $3, updated_at = now() WHERE id = $1`, channelID, stars, broadcastAllowed); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel paid messages price: %w", err)
	}
	channel.SendPaidMessagesStars = stars
	channel.BroadcastMessagesAllowed = broadcastAllowed
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit update channel paid messages price: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetAntiSpam(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel antispam: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel antispam: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !channel.Megagroup || !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.AntiSpam
	if _, err := tx.Exec(ctx, `UPDATE channels SET antispam = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel antispam: %w", err)
	}
	channel.AntiSpam = enabled
	if prev != enabled {
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      nowUnix(),
			Type:      domain.ChannelAdminLogToggleAntiSpam,
			PrevBool:  prev,
			NewBool:   enabled,
		}); err != nil {
			return domain.Channel{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel antispam: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetSlowMode(ctx context.Context, userID, channelID int64, seconds int) (domain.Channel, error) {
	if userID == 0 || channelID == 0 || !domain.ValidChannelSlowModeSeconds(seconds) {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel slowmode: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel slowmode: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !channel.Megagroup || !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.SlowmodeSeconds
	if _, err := tx.Exec(ctx, `UPDATE channels SET slowmode_seconds = $2, updated_at = now() WHERE id = $1`, channelID, seconds); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel slowmode: %w", err)
	}
	channel.SlowmodeSeconds = seconds
	if prev != seconds {
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      nowUnix(),
			Type:      domain.ChannelAdminLogToggleSlowMode,
			PrevInt:   prev,
			NewInt:    seconds,
		}); err != nil {
			return domain.Channel{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel slowmode: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetNoForwards(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel noforwards: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel noforwards: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET noforwards = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel noforwards: %w", err)
	}
	channel.NoForwards = enabled
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel noforwards: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetJoinToSend(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel join_to_send: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel join_to_send: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !channel.Megagroup || !canExportChannelInvite(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET join_to_send = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel join_to_send: %w", err)
	}
	channel.JoinToSend = enabled
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel join_to_send: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetJoinRequest(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("toggle channel join_request: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin toggle channel join_request: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !channel.Megagroup || !canExportChannelInvite(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if enabled && strings.TrimSpace(channel.Username) == "" {
		return domain.Channel{}, domain.ErrChatPublicRequired
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET join_request = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel join_request: %w", err)
	}
	channel.JoinRequest = enabled
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit toggle channel join_request: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetAvailableReactions(ctx context.Context, userID, channelID int64, policy domain.ChannelReactionPolicy) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("set channel available reactions: db does not support transactions")
	}
	policyJSON, err := marshalJSON(policy, "{}")
	if err != nil {
		return domain.Channel{}, err
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin set channel available reactions: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET available_reactions = $2, updated_at = now() WHERE id = $1`, channelID, policyJSON); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel available reactions: %w", err)
	}
	channel.ReactionPolicy = policy
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit set channel available reactions: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetColor(ctx context.Context, userID, channelID int64, forProfile bool, color domain.ChannelPeerColor) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("set channel color: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin set channel color: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if forProfile {
		if _, err := tx.Exec(ctx, `UPDATE channels SET profile_color_set = $2, profile_color = $3, profile_color_background_emoji_id = $4, updated_at = now() WHERE id = $1`,
			channelID, color.HasColor, color.Color, color.BackgroundEmojiID); err != nil {
			return domain.Channel{}, fmt.Errorf("update channel profile color: %w", err)
		}
		channel.ProfileColor = color
	} else {
		if _, err := tx.Exec(ctx, `UPDATE channels SET color_set = $2, color = $3, color_background_emoji_id = $4, updated_at = now() WHERE id = $1`,
			channelID, color.HasColor, color.Color, color.BackgroundEmojiID); err != nil {
			return domain.Channel{}, fmt.Errorf("update channel color: %w", err)
		}
		channel.Color = color
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit set channel color: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) SetEmojiStatus(ctx context.Context, userID, channelID int64, status domain.ChannelEmojiStatus) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	if status.DocumentID == 0 {
		status.Until = 0
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.Channel{}, fmt.Errorf("set channel emoji status: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("begin set channel emoji status: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET emoji_status_document_id = $2, emoji_status_until = $3, updated_at = now() WHERE id = $1`,
		channelID, status.DocumentID, status.Until); err != nil {
		return domain.Channel{}, fmt.Errorf("update channel emoji status: %w", err)
	}
	channel.EmojiStatus = status
	if err := tx.Commit(ctx); err != nil {
		return domain.Channel{}, fmt.Errorf("commit set channel emoji status: %w", err)
	}
	committed = true
	return channel, nil
}

func (s *ChannelStore) ListAdminLog(ctx context.Context, req domain.ChannelAdminLogRequest) (domain.ChannelAdminLogResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MaxID < 0 || req.MinID < 0 {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelAdminLogResult{}, err
	}
	if !isChannelAdmin(member) {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelAdminRequired
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelAdminLogLimit {
		limit = domain.MaxChannelAdminLogLimit
	}
	where := []string{"channel_id = $1"}
	args := []any{req.ChannelID}
	nextArg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if req.MaxID > 0 {
		where = append(where, "id < "+nextArg(req.MaxID))
	}
	if req.MinID > 0 {
		where = append(where, "id > "+nextArg(req.MinID))
	}
	if len(req.AdminUserIDs) > 0 {
		where = append(where, "actor_user_id = ANY("+nextArg(int64s(req.AdminUserIDs))+"::bigint[])")
	}
	if types := adminLogEventTypesForFilter(req.Filter); len(types) > 0 {
		where = append(where, "event_type = ANY("+nextArg(types)+"::text[])")
	} else if !req.Filter.Empty() {
		return domain.ChannelAdminLogResult{Channel: channel}, nil
	}
	query := strings.ToLower(strings.TrimSpace(req.Query))
	if query != "" {
		like := adminLogLikePattern(query)
		where = append(where, `(lower(prev_string) LIKE `+nextArg(like)+` ESCAPE '\' OR lower(new_string) LIKE `+nextArg(like)+` ESCAPE '\' OR lower(query) LIKE `+nextArg(like)+` ESCAPE '\')`)
	}
	args = append(args, limit)
	rows, err := s.db.Query(ctx, `
SELECT channel_id, id, actor_user_id, event_date, event_type, prev_string, new_string, prev_bool, new_bool, prev_int, new_int,
       prev_participant::text, new_participant::text, participant::text, message::text, prev_message::text, new_message::text, query
FROM channel_admin_log_events
WHERE `+strings.Join(where, " AND ")+`
ORDER BY id DESC
LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return domain.ChannelAdminLogResult{}, fmt.Errorf("list channel admin log: %w", err)
	}
	defer rows.Close()
	events := make([]domain.ChannelAdminLogEvent, 0, limit)
	for rows.Next() {
		event, err := scanChannelAdminLogEvent(rows)
		if err != nil {
			return domain.ChannelAdminLogResult{}, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelAdminLogResult{}, err
	}
	return domain.ChannelAdminLogResult{Channel: channel, Events: events}, nil
}

func (s *ChannelStore) SendChannelMessage(ctx context.Context, req domain.SendChannelMessageRequest) (domain.SendChannelMessageResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || (strings.TrimSpace(req.Message) == "" && req.Action == nil && req.Media.IsZero()) {
		return domain.SendChannelMessageResult{}, domain.ErrChannelInvalid
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	var lastErr error
	for attempt := 0; attempt < retryableChannelTxAttempts; attempt++ {
		res, err := s.sendChannelMessageOnce(ctx, req)
		if err == nil || !isRetryablePostgresTxError(err) || ctx.Err() != nil {
			return res, err
		}
		lastErr = err
	}
	return domain.SendChannelMessageResult{}, lastErr
}

func (s *ChannelStore) sendChannelMessageOnce(ctx context.Context, req domain.SendChannelMessageRequest) (domain.SendChannelMessageResult, error) {
	if req.RandomID != 0 {
		if dup, found, err := s.duplicateChannelMessage(ctx, req.ChannelID, req.UserID, req.RandomID); err != nil {
			return domain.SendChannelMessageResult{}, err
		} else if found {
			return dup, nil
		}
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.SendChannelMessageResult{}, fmt.Errorf("send channel message: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.SendChannelMessageResult{}, fmt.Errorf("begin send channel: %w", err)
	}
	var reserved []reservedChannelPts
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			if len(reserved) > 0 {
				s.recordChannelPtsGaps(ctx, reserved, req.Date)
			}
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	if !canSendChannelMessage(channel, member) {
		return domain.SendChannelMessageResult{}, domain.ErrChannelWriteForbidden
	}
	replyTo, err := s.resolveChannelReply(ctx, tx, req, member, channel)
	if err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	if _, err := messageMetadataParamsFrom(req.Silent, req.NoForwards, replyTo, req.Forward); err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	if wait := channelSlowModeWait(channel, member, req.Date); wait > 0 {
		return domain.SendChannelMessageResult{}, domain.NewSlowModeWaitError(wait)
	}
	var sendAs *domain.Peer
	if req.SendAs != nil {
		p := *req.SendAs
		sendAs = &p
	}
	msgID, err := s.msgIDs.NextChannelMessageID(ctx, req.ChannelID)
	if err != nil {
		return domain.SendChannelMessageResult{}, fmt.Errorf("allocate channel message id: %w", err)
	}
	pts, err := s.pts.NextChannelPts(ctx, req.ChannelID)
	if err != nil {
		return domain.SendChannelMessageResult{}, fmt.Errorf("allocate channel pts: %w", err)
	}
	reserved = []reservedChannelPts{{channelID: req.ChannelID, pts: pts, count: 1}}
	var discussion *domain.SendChannelDiscussionResult
	var discussionRef *domain.ChannelDiscussionRef
	if channel.Broadcast && channel.LinkedChatID != 0 {
		linked, err := getChannelByID(ctx, tx, channel.LinkedChatID)
		if err == nil && !linked.Deleted && linked.Megagroup {
			discussionMsgID, err := s.msgIDs.NextChannelMessageID(ctx, linked.ID)
			if err != nil {
				return domain.SendChannelMessageResult{}, fmt.Errorf("allocate discussion message id: %w", err)
			}
			discussionPts, err := s.pts.NextChannelPts(ctx, linked.ID)
			if err != nil {
				return domain.SendChannelMessageResult{}, fmt.Errorf("allocate discussion pts: %w", err)
			}
			reserved = append(reserved, reservedChannelPts{channelID: linked.ID, pts: discussionPts, count: 1})
			discussionRef = &domain.ChannelDiscussionRef{ChannelID: linked.ID, MessageID: discussionMsgID}
			discussionMsg := domain.ChannelMessage{
				ChannelID:    linked.ID,
				ID:           discussionMsgID,
				SenderUserID: req.UserID,
				From:         domain.Peer{Type: domain.PeerTypeChannel, ID: channel.ID},
				Date:         req.Date,
				Silent:       req.Silent,
				NoForwards:   req.NoForwards || channel.NoForwards || linked.NoForwards,
				Body:         req.Message,
				Entities:     append([]domain.MessageEntity(nil), req.Entities...),
				Media:        req.Media,
				Forward:      &domain.MessageForward{From: domain.Peer{Type: domain.PeerTypeChannel, ID: channel.ID}, Date: req.Date, ChannelPost: msgID, SavedFrom: domain.Peer{Type: domain.PeerTypeChannel, ID: channel.ID}, SavedFromMsgID: msgID},
				Pts:          discussionPts,
			}
			discussionEvent := domain.ChannelUpdateEvent{
				ChannelID: linked.ID,
				Type:      domain.ChannelUpdateNewMessage,
				Pts:       discussionPts,
				PtsCount:  1,
				Date:      req.Date,
				Message:   discussionMsg,
			}
			if err := insertChannelMessageTx(ctx, tx, discussionMsg); err != nil {
				return domain.SendChannelMessageResult{}, err
			}
			if err := insertChannelEventTx(ctx, tx, discussionEvent); err != nil {
				return domain.SendChannelMessageResult{}, err
			}
			if err := insertChannelUnreadMentionsTx(ctx, tx, linked.ID, discussionMsg, req.UserID, req.MentionUserIDs); err != nil {
				return domain.SendChannelMessageResult{}, err
			}
			if _, err := tx.Exec(ctx, `UPDATE channels SET top_message_id = $2, pts = $3, updated_at = now() WHERE id = $1`, linked.ID, discussionMsgID, discussionPts); err != nil {
				return domain.SendChannelMessageResult{}, fmt.Errorf("update discussion channel top: %w", err)
			}
			linked.TopMessageID = discussionMsgID
			linked.Pts = discussionPts
			if err := upsertChannelDialogsForMessageTx(ctx, tx, linked, discussionMsg, 0); err != nil {
				return domain.SendChannelMessageResult{}, err
			}
			discussion = &domain.SendChannelDiscussionResult{
				Channel: linked,
				Message: discussionMsg,
				Event:   discussionEvent,
			}
		} else if err != nil && !errors.Is(err, domain.ErrChannelInvalid) {
			return domain.SendChannelMessageResult{}, err
		}
	}
	msg := domain.ChannelMessage{
		ChannelID:    req.ChannelID,
		ID:           msgID,
		RandomID:     req.RandomID,
		SenderUserID: req.UserID,
		From:         domain.Peer{Type: domain.PeerTypeUser, ID: req.UserID},
		Date:         req.Date,
		Post:         channel.Broadcast,
		Silent:       req.Silent,
		NoForwards:   req.NoForwards || channel.NoForwards,
		Body:         req.Message,
		Entities:     append([]domain.MessageEntity(nil), req.Entities...),
		Media:        req.Media,
		ReplyTo:      replyTo,
		Forward:      cloneMessageForward(req.Forward),
		SendAs:       sendAs,
		Discussion:   discussionRef,
		Action:       cloneChannelMessageAction(req.Action),
		Pts:          pts,
	}
	if discussionRef != nil {
		msg.Replies = &domain.ChannelMessageReplies{Comments: true, ChannelID: discussionRef.ChannelID, RepliesPts: discussion.Event.Pts}
	}
	event := domain.ChannelUpdateEvent{
		ChannelID:    req.ChannelID,
		Type:         domain.ChannelUpdateNewMessage,
		Pts:          pts,
		PtsCount:     1,
		Date:         req.Date,
		Message:      msg,
		SenderUserID: req.UserID,
	}
	if err := insertChannelMessageTx(ctx, tx, msg); err != nil {
		if isUniqueViolation(err) {
			dup, found, dupErr := s.duplicateChannelMessage(ctx, req.ChannelID, req.UserID, req.RandomID)
			if dupErr != nil || !found {
				return domain.SendChannelMessageResult{}, dupErr
			}
			dup.Duplicate = true
			return dup, nil
		}
		return domain.SendChannelMessageResult{}, err
	}
	if err := insertChannelEventTx(ctx, tx, event); err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	if err := insertChannelUnreadMentionsTx(ctx, tx, req.ChannelID, msg, req.UserID, req.MentionUserIDs); err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	if err := updateForumTopicTopMessageTx(ctx, tx, req.ChannelID, msg); err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET top_message_id = $2, pts = $3, updated_at = now() WHERE id = $1`, req.ChannelID, msgID, pts); err != nil {
		return domain.SendChannelMessageResult{}, fmt.Errorf("update channel top: %w", err)
	}
	channel.TopMessageID = msgID
	channel.Pts = pts
	if _, err := tx.Exec(ctx, `
UPDATE channel_members
SET slowmode_last_send_date = $3,
    read_inbox_max_id = GREATEST(read_inbox_max_id, $4),
    unread_mark = false,
    updated_at = now()
WHERE channel_id = $1 AND user_id = $2`, req.ChannelID, req.UserID, req.Date, msgID); err != nil {
		return domain.SendChannelMessageResult{}, fmt.Errorf("update channel member slowmode send date: %w", err)
	}
	if err := upsertChannelDialogsForMessageTx(ctx, tx, channel, msg, req.UserID); err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	if channel.Broadcast {
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: req.ChannelID,
			UserID:    req.UserID,
			Date:      req.Date,
			Type:      domain.ChannelAdminLogSendMessage,
			Message:   &msg,
			Query:     msg.Body,
		}); err != nil {
			return domain.SendChannelMessageResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.SendChannelMessageResult{}, fmt.Errorf("commit send channel: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	if discussion != nil {
		discussion.Recipients, _ = s.ListActiveChannelMemberIDs(ctx, req.UserID, discussion.Channel.ID, 0)
	}
	return domain.SendChannelMessageResult{Channel: channel, Message: msg, Event: event, Recipients: recipients, Discussion: discussion}, nil
}

func (s *ChannelStore) EditChannelMessage(ctx context.Context, req domain.EditChannelMessageRequest) (domain.EditChannelMessageResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.ID <= 0 || strings.TrimSpace(req.Message) == "" {
		return domain.EditChannelMessageResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.EditChannelMessageResult{}, fmt.Errorf("edit channel message: db does not support transactions")
	}
	pts, err := s.pts.NextChannelPts(ctx, req.ChannelID)
	if err != nil {
		return domain.EditChannelMessageResult{}, fmt.Errorf("allocate channel edit pts: %w", err)
	}
	reserved := []reservedChannelPts{{channelID: req.ChannelID, pts: pts, count: 1}}
	if req.EditDate == 0 {
		req.EditDate = nowUnix()
	}
	entities, err := encodeMessageEntities(req.Entities)
	if err != nil {
		s.recordChannelPtsGaps(ctx, reserved, req.EditDate)
		return domain.EditChannelMessageResult{}, err
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		s.recordChannelPtsGaps(ctx, reserved, req.EditDate)
		return domain.EditChannelMessageResult{}, fmt.Errorf("begin edit channel message: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.EditDate)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelMessageResult{}, err
	}
	msg, err := s.getChannelMessage(ctx, tx, req.ChannelID, req.ID)
	if err != nil {
		return domain.EditChannelMessageResult{}, err
	}
	if msg.Deleted || msg.Action != nil {
		return domain.EditChannelMessageResult{}, domain.ErrMessageIDInvalid
	}
	if msg.SenderUserID != req.UserID && !canEditChannelMessage(member) {
		return domain.EditChannelMessageResult{}, domain.ErrMessageAuthorRequired
	}
	if msg.Body == req.Message && sameMessageEntities(msg.Entities, req.Entities) {
		return domain.EditChannelMessageResult{}, domain.ErrMessageNotModified
	}
	prevMsg := msg
	if _, err := tx.Exec(ctx, `
UPDATE channel_messages
SET body = $4, entities = $5, edit_date = $6, pts = $7, updated_at = now()
WHERE channel_id = $1 AND id = $2 AND NOT deleted AND sender_user_id = $3 OR (
    channel_id = $1 AND id = $2 AND NOT deleted AND $8
)`,
		req.ChannelID, req.ID, req.UserID, req.Message, entities, req.EditDate, pts, canEditChannelMessage(member)); err != nil {
		return domain.EditChannelMessageResult{}, fmt.Errorf("update channel edit: %w", err)
	}
	msg.Body = req.Message
	msg.Entities = append([]domain.MessageEntity(nil), req.Entities...)
	msg.EditDate = req.EditDate
	msg.Pts = pts
	event := domain.ChannelUpdateEvent{
		ChannelID:    req.ChannelID,
		Type:         domain.ChannelUpdateEditMessage,
		Pts:          pts,
		PtsCount:     1,
		Date:         req.EditDate,
		Message:      msg,
		SenderUserID: req.UserID,
	}
	if err := insertChannelEventTx(ctx, tx, event); err != nil {
		return domain.EditChannelMessageResult{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET pts = $2, updated_at = now() WHERE id = $1`, req.ChannelID, pts); err != nil {
		return domain.EditChannelMessageResult{}, fmt.Errorf("update channel edit pts: %w", err)
	}
	channel.Pts = pts
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID:   req.ChannelID,
		UserID:      req.UserID,
		Date:        req.EditDate,
		Type:        domain.ChannelAdminLogEditMessage,
		PrevMessage: &prevMsg,
		NewMessage:  &msg,
		Query:       msg.Body,
	}); err != nil {
		return domain.EditChannelMessageResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.EditChannelMessageResult{}, fmt.Errorf("commit edit channel message: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	return domain.EditChannelMessageResult{Channel: channel, Message: msg, Event: event, Recipients: recipients}, nil
}

func (s *ChannelStore) DeleteChannelMessages(ctx context.Context, req domain.DeleteChannelMessagesRequest) (domain.DeleteChannelMessagesResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || len(req.IDs) == 0 {
		return domain.DeleteChannelMessagesResult{}, domain.ErrChannelInvalid
	}
	if len(req.IDs) > domain.MaxDeleteMessageIDs {
		return domain.DeleteChannelMessagesResult{}, domain.ErrChannelInvalid
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.DeleteChannelMessagesResult{}, fmt.Errorf("delete channel messages: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.DeleteChannelMessagesResult{}, fmt.Errorf("begin delete channel messages: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelMessagesResult{}, err
	}
	deleted, event, channel, err := s.deleteChannelMessagesTx(ctx, tx, channel, member, req.IDs, req.UserID, req.Date, &reserved)
	if err != nil {
		return domain.DeleteChannelMessagesResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DeleteChannelMessagesResult{}, fmt.Errorf("commit delete channel messages: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	return domain.DeleteChannelMessagesResult{Channel: channel, Event: event, DeletedIDs: deleted, Recipients: recipients}, nil
}

func (s *ChannelStore) DeleteChannelHistory(ctx context.Context, req domain.DeleteChannelHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("delete channel history: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("begin delete channel history: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	maxID := req.MaxID
	if maxID <= 0 || maxID > channel.TopMessageID {
		maxID = channel.TopMessageID
	}
	if !req.ForEveryone {
		appliedMinID := maxInt(member.AvailableMinID, maxID)
		topID, topDate, err := visibleChannelTopAfter(ctx, tx, req.ChannelID, appliedMinID, channel.Date)
		if err != nil {
			return domain.DeleteChannelHistoryResult{}, err
		}
		if _, err := tx.Exec(ctx, `
UPDATE channel_members
SET available_min_id = GREATEST(available_min_id, $3),
    read_inbox_max_id = GREATEST(read_inbox_max_id, $3),
    unread_mark = false,
    updated_at = now()
WHERE channel_id = $1 AND user_id = $2`, req.ChannelID, req.UserID, appliedMinID); err != nil {
			return domain.DeleteChannelHistoryResult{}, fmt.Errorf("update channel local clear member: %w", err)
		}
		if err := deleteChannelUnreadMentionsUpToTx(ctx, tx, req.UserID, req.ChannelID, appliedMinID); err != nil {
			return domain.DeleteChannelHistoryResult{}, err
		}
		if err := refreshChannelUnreadReactionsCountTx(ctx, tx, req.UserID, req.ChannelID); err != nil {
			return domain.DeleteChannelHistoryResult{}, err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO channel_dialogs (
    user_id, channel_id, top_message_id, top_message_date, read_inbox_max_id, read_outbox_max_id, unread_count, unread_mark
) VALUES ($1,$2,$3,$4,$5,0,0,false)
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    top_message_id = EXCLUDED.top_message_id,
    top_message_date = EXCLUDED.top_message_date,
    read_inbox_max_id = GREATEST(channel_dialogs.read_inbox_max_id, EXCLUDED.read_inbox_max_id),
    unread_count = 0,
    unread_mark = false,
    updated_at = now()`, req.UserID, req.ChannelID, topID, topDate, appliedMinID); err != nil {
			return domain.DeleteChannelHistoryResult{}, fmt.Errorf("upsert channel local clear dialog: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.DeleteChannelHistoryResult{}, fmt.Errorf("commit local clear channel history: %w", err)
		}
		committed = true
		return domain.DeleteChannelHistoryResult{Channel: channel, AvailableMinID: appliedMinID}, nil
	}
	if !canDeleteAnyChannelMessage(member) {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelAdminRequired
	}
	rows, err := tx.Query(ctx, `
SELECT id
FROM channel_messages
WHERE channel_id = $1 AND id <= $2 AND NOT deleted
ORDER BY id DESC
LIMIT $3`, req.ChannelID, maxID, domain.MaxDeleteHistoryBatch)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("list channel history delete ids: %w", err)
	}
	ids := make([]int, 0, domain.MaxDeleteHistoryBatch)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return domain.DeleteChannelHistoryResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.DeleteChannelHistoryResult{}, err
	}
	rows.Close()
	deleted, event, channel, err := s.deleteChannelMessagesTx(ctx, tx, channel, member, ids, req.UserID, req.Date, &reserved)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("commit delete channel history: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	offset := 0
	if len(deleted) == domain.MaxDeleteHistoryBatch {
		offset = 1
	}
	return domain.DeleteChannelHistoryResult{Channel: channel, Event: event, DeletedIDs: deleted, Recipients: recipients, Offset: offset}, nil
}

func (s *ChannelStore) DeleteChannelParticipantHistory(ctx context.Context, req domain.DeleteChannelParticipantHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.ParticipantUserID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("delete participant channel history: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("begin delete participant channel history: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	if !canDeleteAnyChannelMessage(member) {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelAdminRequired
	}
	rows, err := tx.Query(ctx, `
SELECT id
FROM channel_messages
WHERE channel_id = $1 AND sender_user_id = $2 AND NOT deleted
ORDER BY id DESC
LIMIT $3`, req.ChannelID, req.ParticipantUserID, domain.MaxDeleteHistoryBatch)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("list participant channel history delete ids: %w", err)
	}
	ids := make([]int, 0, domain.MaxDeleteHistoryBatch)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return domain.DeleteChannelHistoryResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.DeleteChannelHistoryResult{}, err
	}
	rows.Close()
	deleted, event, channel, err := s.deleteChannelMessagesTx(ctx, tx, channel, member, ids, req.UserID, req.Date, &reserved)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("commit delete participant channel history: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	offset := 0
	if len(deleted) == domain.MaxDeleteHistoryBatch {
		offset = 1
	}
	return domain.DeleteChannelHistoryResult{Channel: channel, Event: event, DeletedIDs: deleted, Recipients: recipients, Offset: offset}, nil
}

func (s *ChannelStore) UpdatePinnedMessage(ctx context.Context, req domain.UpdateChannelPinnedMessageRequest) (domain.UpdateChannelPinnedMessageResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MessageID <= 0 {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.UpdateChannelPinnedMessageResult{}, fmt.Errorf("pin channel message: db does not support transactions")
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	pts, err := s.pts.NextChannelPts(ctx, req.ChannelID)
	if err != nil {
		return domain.UpdateChannelPinnedMessageResult{}, fmt.Errorf("allocate channel pin pts: %w", err)
	}
	reserved := []reservedChannelPts{{channelID: req.ChannelID, pts: pts, count: 1}}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		s.recordChannelPtsGaps(ctx, reserved, req.Date)
		return domain.UpdateChannelPinnedMessageResult{}, fmt.Errorf("begin pin channel message: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.UpdateChannelPinnedMessageResult{}, err
	}
	if !canPinChannelMessages(channel, member) {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrChannelAdminRequired
	}
	msg, err := s.getChannelMessage(ctx, tx, req.ChannelID, req.MessageID)
	if err != nil || msg.Deleted {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrMessageIDInvalid
	}
	pinnedID := 0
	if req.Pinned {
		pinnedID = req.MessageID
	}
	if channel.PinnedMessageID == pinnedID {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrChannelNotModified
	}
	if _, err := tx.Exec(ctx, `
UPDATE channels
SET pinned_message_id = $2, pts = $3, updated_at = now()
WHERE id = $1`, req.ChannelID, pinnedID, pts); err != nil {
		return domain.UpdateChannelPinnedMessageResult{}, fmt.Errorf("update channel pinned message: %w", err)
	}
	channel.PinnedMessageID = pinnedID
	channel.Pts = pts
	event := domain.ChannelUpdateEvent{
		ChannelID:    req.ChannelID,
		Type:         domain.ChannelUpdatePinnedMessages,
		Pts:          pts,
		PtsCount:     1,
		Date:         req.Date,
		MessageIDs:   []int{req.MessageID},
		SenderUserID: req.UserID,
		Pinned:       req.Pinned,
	}
	if err := insertChannelEventTx(ctx, tx, event); err != nil {
		return domain.UpdateChannelPinnedMessageResult{}, err
	}
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID: req.ChannelID,
		UserID:    req.UserID,
		Date:      req.Date,
		Type:      domain.ChannelAdminLogUpdatePinned,
		Message:   &msg,
		Query:     msg.Body,
	}); err != nil {
		return domain.UpdateChannelPinnedMessageResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.UpdateChannelPinnedMessageResult{}, fmt.Errorf("commit pin channel message: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	return domain.UpdateChannelPinnedMessageResult{Channel: channel, Event: event, Recipients: recipients}, nil
}

func (s *ChannelStore) ExportInvite(ctx context.Context, req domain.ExportChannelInviteRequest) (domain.ExportChannelInviteResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ExportChannelInviteResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ExportChannelInviteResult{}, fmt.Errorf("export channel invite: db does not support transactions")
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ExportChannelInviteResult{}, fmt.Errorf("begin export channel invite: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ExportChannelInviteResult{}, err
	}
	if !canExportChannelInvite(member) {
		return domain.ExportChannelInviteResult{}, domain.ErrChannelAdminRequired
	}
	if req.LegacyRevokePermanent {
		if _, err := tx.Exec(ctx, `
UPDATE channel_invites
SET revoked = true, updated_at = now()
WHERE channel_id = $1 AND admin_user_id = $2 AND permanent AND NOT revoked`, req.ChannelID, req.UserID); err != nil {
			return domain.ExportChannelInviteResult{}, fmt.Errorf("revoke permanent channel invite: %w", err)
		}
	}
	inviteID, err := randomPositiveInt64()
	if err != nil {
		return domain.ExportChannelInviteResult{}, err
	}
	hash, err := randomInviteHash()
	if err != nil {
		return domain.ExportChannelInviteResult{}, err
	}
	invite := domain.ChannelInvite{
		ChannelID:     req.ChannelID,
		InviteID:      inviteID,
		Hash:          hash,
		AdminUserID:   req.UserID,
		Title:         req.Title,
		Permanent:     req.ExpireDate == 0 && req.UsageLimit == 0 && !req.RequestNeeded && req.Title == "",
		RequestNeeded: req.RequestNeeded,
		ExpireDate:    req.ExpireDate,
		UsageLimit:    req.UsageLimit,
		Date:          req.Date,
	}
	if err := insertChannelInviteTx(ctx, tx, invite); err != nil {
		return domain.ExportChannelInviteResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ExportChannelInviteResult{}, fmt.Errorf("commit export channel invite: %w", err)
	}
	committed = true
	return domain.ExportChannelInviteResult{Channel: channel, Invite: invite}, nil
}

func (s *ChannelStore) CheckInvite(ctx context.Context, userID int64, hash string, date int) (domain.CheckChannelInviteResult, error) {
	if userID == 0 || strings.TrimSpace(hash) == "" {
		return domain.CheckChannelInviteResult{}, domain.ErrInviteHashEmpty
	}
	if date == 0 {
		date = nowUnix()
	}
	channel, invite, err := s.getInviteByHash(ctx, s.db, strings.TrimSpace(hash))
	if err != nil {
		return domain.CheckChannelInviteResult{}, err
	}
	if invite.ExpireDate > 0 && invite.ExpireDate < date {
		return domain.CheckChannelInviteResult{}, domain.ErrInviteHashExpired
	}
	member, err := s.getChannelMember(ctx, s.db, channel.ID, userID)
	already := false
	if err == nil {
		if member.Status == domain.ChannelMemberKicked || member.Status == domain.ChannelMemberBanned || member.BannedRights.ViewMessages {
			return domain.CheckChannelInviteResult{}, domain.ErrInviteHashInvalid
		}
		already = member.Status == domain.ChannelMemberActive
	} else if !errors.Is(err, domain.ErrChannelPrivate) {
		return domain.CheckChannelInviteResult{}, err
	}
	return domain.CheckChannelInviteResult{Channel: channel, Invite: invite, Already: already, Self: member}, nil
}

func (s *ChannelStore) ImportInvite(ctx context.Context, req domain.ImportChannelInviteRequest) (domain.CreateChannelResult, error) {
	if req.UserID == 0 || strings.TrimSpace(req.Hash) == "" {
		return domain.CreateChannelResult{}, domain.ErrInviteHashEmpty
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.CreateChannelResult{}, fmt.Errorf("import channel invite: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("begin import channel invite: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, invite, err := s.getInviteByHashForUpdate(ctx, tx, strings.TrimSpace(req.Hash))
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	if invite.ExpireDate > 0 && invite.ExpireDate < req.Date {
		return domain.CreateChannelResult{}, domain.ErrInviteHashExpired
	}
	if invite.RequestNeeded {
		if err := s.recordPendingInviteImporterTx(ctx, tx, invite, req.UserID, req.Date); err != nil {
			return domain.CreateChannelResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.CreateChannelResult{}, fmt.Errorf("commit pending channel invite request: %w", err)
		}
		committed = true
		return domain.CreateChannelResult{Channel: channel}, domain.ErrInviteRequestSent
	}
	result, err := s.approveInviteImporterTx(ctx, tx, channel, invite, req.UserID, 0, req.Date, &reserved)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("commit import channel invite: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, result.Channel.ID, 0)
	result.Recipients = recipients
	return result, nil
}

func (s *ChannelStore) approveInviteImporterTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, invite domain.ChannelInvite, userID, approvedBy int64, date int, reserved *[]reservedChannelPts) (domain.CreateChannelResult, error) {
	if invite.InviteID != 0 && invite.UsageLimit > 0 && invite.UsageCount >= invite.UsageLimit {
		return domain.CreateChannelResult{}, domain.ErrUsersTooMuch
	}
	channelID := channel.ID
	if channelID == 0 {
		channelID = invite.ChannelID
	}
	if existing, err := s.getChannelMember(ctx, tx, channelID, userID); err == nil {
		if existing.Status == domain.ChannelMemberActive {
			return domain.CreateChannelResult{}, domain.ErrUserAlreadyParticipant
		}
		if existing.Status == domain.ChannelMemberKicked || existing.Status == domain.ChannelMemberBanned || existing.BannedRights.ViewMessages {
			return domain.CreateChannelResult{}, domain.ErrInviteHashInvalid
		}
	} else if !errors.Is(err, domain.ErrChannelPrivate) {
		return domain.CreateChannelResult{}, err
	}
	preJoinTopID := channel.TopMessageID
	minID := channelInitialAvailableMinID(channel)
	inviterID := invite.AdminUserID
	if inviterID == 0 {
		inviterID = approvedBy
	}
	member := domain.ChannelMember{
		ChannelID:       channelID,
		UserID:          userID,
		InviterUserID:   inviterID,
		Role:            domain.ChannelRoleMember,
		Status:          domain.ChannelMemberActive,
		JoinedAt:        date,
		AvailableMinID:  minID,
		AvailableMinPts: channelInitialAvailableMinPts(channel),
		ReadInboxMaxID:  maxInt(minID, preJoinTopID),
	}
	if err := upsertChannelMemberTx(ctx, tx, channel, member); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
		ChannelID: channelID,
		UserID:    userID,
		Date:      date,
		Type:      domain.ChannelAdminLogParticipantJoin,
	}); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if invite.InviteID != 0 {
		if _, err := tx.Exec(ctx, `
UPDATE channel_invites
SET requested_count = GREATEST(requested_count - 1, 0),
    updated_at = now()
WHERE channel_id = $1
  AND invite_id = (
      SELECT invite_id
      FROM channel_invite_importers
      WHERE channel_id = $1 AND user_id = $2 AND requested
  )`, channelID, userID); err != nil {
			return domain.CreateChannelResult{}, fmt.Errorf("clear pending channel invite request: %w", err)
		}
		if _, err := tx.Exec(ctx, `
UPDATE channel_invites
SET usage_count = usage_count + 1,
    updated_at = now()
WHERE channel_id = $1 AND invite_id = $2`, channelID, invite.InviteID); err != nil {
			return domain.CreateChannelResult{}, fmt.Errorf("increment channel invite usage: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_invite_importers (channel_id, invite_id, user_id, date, requested, approved_by)
VALUES ($1, $2, $3, $4, false, $5)
ON CONFLICT (channel_id, user_id) DO UPDATE
SET invite_id = EXCLUDED.invite_id,
    date = EXCLUDED.date,
    requested = false,
    approved_by = EXCLUDED.approved_by,
    updated_at = now()`, channelID, invite.InviteID, userID, date, approvedBy); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("upsert channel invite importer: %w", err)
	}
	channel, err := refreshChannelCountsTx(ctx, tx, channel)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	var msg domain.ChannelMessage
	var event domain.ChannelUpdateEvent
	if channel.Megagroup {
		msg, event, err = s.insertServiceMessage(ctx, tx, channel, userID, date, domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatJoined,
			UserIDs: []int64{userID},
		}, reserved)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		channel.TopMessageID = msg.ID
		channel.Pts = event.Pts
	}
	member.ReadInboxMaxID = maxInt(member.ReadInboxMaxID, channel.TopMessageID)
	if msg.ID != 0 {
		member.ReadOutboxMaxID = maxInt(member.ReadOutboxMaxID, msg.ID)
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_members
SET read_inbox_max_id = $3, read_outbox_max_id = $4, updated_at = now()
WHERE channel_id = $1 AND user_id = $2`, channel.ID, userID, member.ReadInboxMaxID, member.ReadOutboxMaxID); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("update imported member read state: %w", err)
	}
	if err := upsertChannelDialogTx(ctx, tx, userID, channel, msg, member.ReadInboxMaxID, member.ReadOutboxMaxID); err != nil {
		return domain.CreateChannelResult{}, err
	}
	return domain.CreateChannelResult{Channel: channel, Members: []domain.ChannelMember{member}, Message: msg, Event: event}, nil
}

func (s *ChannelStore) recordPendingInviteImporterTx(ctx context.Context, tx pgx.Tx, invite domain.ChannelInvite, userID int64, date int) error {
	if existing, err := s.getChannelMember(ctx, tx, invite.ChannelID, userID); err == nil {
		if existing.Status == domain.ChannelMemberActive {
			return domain.ErrUserAlreadyParticipant
		}
		if existing.Status == domain.ChannelMemberKicked || existing.Status == domain.ChannelMemberBanned || existing.BannedRights.ViewMessages {
			return domain.ErrInviteHashInvalid
		}
	} else if !errors.Is(err, domain.ErrChannelPrivate) {
		return err
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO channel_invite_importers (channel_id, invite_id, user_id, date, requested)
VALUES ($1, $2, $3, $4, true)
ON CONFLICT (channel_id, user_id) DO UPDATE
SET invite_id = EXCLUDED.invite_id,
    date = EXCLUDED.date,
    requested = true,
    approved_by = 0,
    updated_at = now()
WHERE NOT channel_invite_importers.requested`,
		invite.ChannelID, invite.InviteID, userID, date)
	if err != nil {
		return fmt.Errorf("record pending channel invite importer: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrInviteRequestSent
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_invites
SET requested_count = requested_count + 1, updated_at = now()
WHERE channel_id = $1 AND invite_id = $2`, invite.ChannelID, invite.InviteID); err != nil {
		return fmt.Errorf("increment channel invite requested count: %w", err)
	}
	return nil
}

func (s *ChannelStore) recordPublicJoinRequestTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, userID int64, date int) error {
	if existing, err := s.getChannelMember(ctx, tx, channel.ID, userID); err == nil {
		if existing.Status == domain.ChannelMemberActive {
			return domain.ErrUserAlreadyParticipant
		}
		if existing.Status == domain.ChannelMemberKicked || existing.Status == domain.ChannelMemberBanned || existing.BannedRights.ViewMessages {
			return domain.ErrInviteHashInvalid
		}
	} else if !errors.Is(err, domain.ErrChannelPrivate) {
		return err
	}
	if existing, err := s.getPendingInviteImporterTx(ctx, tx, channel.ID, userID, true); err == nil && existing.Requested {
		return domain.ErrInviteRequestSent
	} else if err != nil && !errors.Is(err, domain.ErrHideRequesterMissing) {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_invite_importers (channel_id, invite_id, user_id, date, requested)
VALUES ($1, 0, $2, $3, true)
ON CONFLICT (channel_id, user_id) DO UPDATE
SET invite_id = 0,
    date = EXCLUDED.date,
    requested = true,
    approved_by = 0,
    updated_at = now()`, channel.ID, userID, date); err != nil {
		return fmt.Errorf("insert public channel join request: %w", err)
	}
	return nil
}

func (s *ChannelStore) ListExportedInvites(ctx context.Context, req domain.ChannelInviteListRequest) (domain.ChannelInviteList, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.AdminUserID == 0 {
		return domain.ChannelInviteList{}, domain.ErrChannelInvalid
	}
	_, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelInviteList{}, err
	}
	if !canExportChannelInvite(member) {
		return domain.ChannelInviteList{}, domain.ErrChannelAdminRequired
	}
	var total int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_invites
WHERE channel_id = $1 AND admin_user_id = $2 AND revoked = $3`, req.ChannelID, req.AdminUserID, req.Revoked).Scan(&total); err != nil {
		return domain.ChannelInviteList{}, err
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelInviteListLimit {
		limit = domain.MaxChannelInviteListLimit
	}
	rows, err := s.db.Query(ctx, `
SELECT channel_id, invite_id, hash, admin_user_id, title, permanent, revoked, request_needed,
       COALESCE(expire_date, 0), COALESCE(usage_limit, 0), usage_count, requested_count,
       EXTRACT(EPOCH FROM created_at)::int
FROM channel_invites
WHERE channel_id = $1
  AND admin_user_id = $2
  AND revoked = $3
  AND (($4::int = 0 AND $5::text = '') OR (EXTRACT(EPOCH FROM created_at)::int, hash) < ($4, $5))
ORDER BY EXTRACT(EPOCH FROM created_at)::int DESC, hash DESC
LIMIT $6`, req.ChannelID, req.AdminUserID, req.Revoked, req.OffsetDate, req.OffsetHash, limit)
	if err != nil {
		return domain.ChannelInviteList{}, err
	}
	defer rows.Close()
	invites := make([]domain.ChannelInvite, 0, limit)
	for rows.Next() {
		invite, err := scanChannelInvite(rows)
		if err != nil {
			return domain.ChannelInviteList{}, err
		}
		invites = append(invites, invite)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelInviteList{}, err
	}
	return domain.ChannelInviteList{Count: total, Invites: invites}, nil
}

func (s *ChannelStore) GetExportedInvite(ctx context.Context, req domain.GetChannelInviteRequest) (domain.ChannelInvite, error) {
	if req.UserID == 0 || req.ChannelID == 0 || strings.TrimSpace(req.Hash) == "" {
		return domain.ChannelInvite{}, domain.ErrInviteHashEmpty
	}
	_, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelInvite{}, err
	}
	if !canExportChannelInvite(member) {
		return domain.ChannelInvite{}, domain.ErrChannelAdminRequired
	}
	return s.getInviteByChannelHash(ctx, s.db, req.ChannelID, req.Hash, false)
}

func (s *ChannelStore) EditExportedInvite(ctx context.Context, req domain.EditChannelInviteRequest) (domain.EditChannelInviteResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || strings.TrimSpace(req.Hash) == "" {
		return domain.EditChannelInviteResult{}, domain.ErrInviteHashEmpty
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.EditChannelInviteResult{}, fmt.Errorf("edit channel invite: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.EditChannelInviteResult{}, fmt.Errorf("begin edit channel invite: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if _, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID); err != nil {
		return domain.EditChannelInviteResult{}, err
	} else if !canExportChannelInvite(member) {
		return domain.EditChannelInviteResult{}, domain.ErrChannelAdminRequired
	}
	invite, err := s.getInviteByChannelHash(ctx, tx, req.ChannelID, req.Hash, true)
	if err != nil {
		return domain.EditChannelInviteResult{}, err
	}
	if req.Revoked {
		if invite.Revoked {
			return domain.EditChannelInviteResult{}, domain.ErrInviteRevokedMissing
		}
		if _, err := tx.Exec(ctx, `UPDATE channel_invites SET revoked = true, updated_at = now() WHERE channel_id = $1 AND invite_id = $2`, invite.ChannelID, invite.InviteID); err != nil {
			return domain.EditChannelInviteResult{}, fmt.Errorf("revoke channel invite: %w", err)
		}
		invite.Revoked = true
		result := domain.EditChannelInviteResult{Invite: invite}
		if invite.Permanent {
			newInvite, err := s.newPostgresReplacementInvite(invite, req.Date)
			if err != nil {
				return domain.EditChannelInviteResult{}, err
			}
			if err := insertChannelInviteTx(ctx, tx, newInvite); err != nil {
				return domain.EditChannelInviteResult{}, err
			}
			result.NewInvite = &newInvite
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.EditChannelInviteResult{}, fmt.Errorf("commit edit channel invite: %w", err)
		}
		committed = true
		return result, nil
	}
	if invite.Revoked {
		return domain.EditChannelInviteResult{}, domain.ErrInviteRevokedMissing
	}
	if invite.Permanent && ((req.HasExpireDate && req.ExpireDate > 0) || (req.HasUsageLimit && req.UsageLimit > 0) || (req.HasRequestNeeded && req.RequestNeeded)) {
		return domain.EditChannelInviteResult{}, domain.ErrInvitePermanent
	}
	if req.HasExpireDate {
		invite.ExpireDate = req.ExpireDate
	}
	if req.HasUsageLimit {
		invite.UsageLimit = req.UsageLimit
	}
	if req.HasRequestNeeded {
		invite.RequestNeeded = req.RequestNeeded
	}
	if req.HasTitle {
		invite.Title = req.Title
	}
	invite.Permanent = invite.ExpireDate == 0 && invite.UsageLimit == 0 && !invite.RequestNeeded && invite.Title == ""
	if _, err := tx.Exec(ctx, `
UPDATE channel_invites
SET title = $3,
    expire_date = NULLIF($4, 0),
    usage_limit = NULLIF($5, 0),
    request_needed = $6,
    permanent = $7,
    updated_at = now()
WHERE channel_id = $1 AND invite_id = $2`,
		invite.ChannelID, invite.InviteID, invite.Title, invite.ExpireDate, invite.UsageLimit, invite.RequestNeeded, invite.Permanent); err != nil {
		return domain.EditChannelInviteResult{}, fmt.Errorf("update channel invite: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.EditChannelInviteResult{}, fmt.Errorf("commit edit channel invite: %w", err)
	}
	committed = true
	return domain.EditChannelInviteResult{Invite: invite}, nil
}

func (s *ChannelStore) DeleteExportedInvite(ctx context.Context, req domain.DeleteChannelInviteRequest) error {
	if req.UserID == 0 || req.ChannelID == 0 || strings.TrimSpace(req.Hash) == "" {
		return domain.ErrInviteHashEmpty
	}
	if _, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID); err != nil {
		return err
	} else if !canExportChannelInvite(member) {
		return domain.ErrChannelAdminRequired
	}
	tag, err := s.db.Exec(ctx, `
WITH deleted AS (
    DELETE FROM channel_invites
    WHERE channel_id = $1 AND hash = $2
    RETURNING hash
)
DELETE FROM channel_invite_hashes h USING deleted d WHERE h.hash = d.hash`, req.ChannelID, strings.TrimSpace(req.Hash))
	if err != nil {
		return fmt.Errorf("delete channel invite: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrInviteRevokedMissing
	}
	return nil
}

func (s *ChannelStore) DeleteRevokedExportedInvites(ctx context.Context, req domain.DeleteRevokedChannelInvitesRequest) error {
	if req.UserID == 0 || req.ChannelID == 0 || req.AdminUserID == 0 {
		return domain.ErrChannelInvalid
	}
	if _, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID); err != nil {
		return err
	} else if !canExportChannelInvite(member) {
		return domain.ErrChannelAdminRequired
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelHideJoinRequests {
		limit = domain.MaxChannelHideJoinRequests
	}
	if _, err := s.db.Exec(ctx, `
WITH deleted AS (
    DELETE FROM channel_invites
    WHERE ctid IN (
        SELECT ctid FROM channel_invites
        WHERE channel_id = $1 AND admin_user_id = $2 AND revoked
        ORDER BY updated_at ASC
        LIMIT $3
    )
    RETURNING hash
)
DELETE FROM channel_invite_hashes h USING deleted d WHERE h.hash = d.hash`, req.ChannelID, req.AdminUserID, limit); err != nil {
		return fmt.Errorf("delete revoked channel invites: %w", err)
	}
	return nil
}

func (s *ChannelStore) ListAdminsWithInvites(ctx context.Context, userID, channelID int64) ([]domain.ChannelAdminInviteCount, error) {
	if userID == 0 || channelID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if _, member, err := s.getChannelForMember(ctx, s.db, userID, channelID); err != nil {
		return nil, err
	} else if !canExportChannelInvite(member) {
		return nil, domain.ErrChannelAdminRequired
	}
	rows, err := s.db.Query(ctx, `
SELECT admin_user_id,
       COUNT(*) FILTER (WHERE NOT revoked)::int,
       COUNT(*) FILTER (WHERE revoked)::int
FROM channel_invites
WHERE channel_id = $1
GROUP BY admin_user_id
ORDER BY admin_user_id ASC`, channelID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.ChannelAdminInviteCount, 0)
	for rows.Next() {
		var count domain.ChannelAdminInviteCount
		if err := rows.Scan(&count.AdminUserID, &count.InvitesCount, &count.RevokedInvitesCount); err != nil {
			return nil, err
		}
		out = append(out, count)
	}
	return out, rows.Err()
}

func (s *ChannelStore) ListInviteImporters(ctx context.Context, req domain.ChannelInviteImportersRequest) (domain.ChannelInviteImporterList, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelInvalid
	}
	if _, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID); err != nil {
		return domain.ChannelInviteImporterList{}, err
	} else if !canExportChannelInvite(member) {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelAdminRequired
	}
	var inviteID int64
	if req.Hash != "" {
		invite, err := s.getInviteByChannelHash(ctx, s.db, req.ChannelID, req.Hash, false)
		if err != nil {
			return domain.ChannelInviteImporterList{}, err
		}
		inviteID = invite.InviteID
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelInviteListLimit {
		limit = domain.MaxChannelInviteListLimit
	}
	args := []any{req.ChannelID, req.Requested, inviteID, req.Query, req.OffsetDate, req.OffsetUserID, limit}
	where := []string{
		"i.channel_id = $1",
		"i.requested = $2",
		"($3::bigint = 0 OR i.invite_id = $3)",
		"($4::text = '' OR lower(trim(u.username || ' ' || u.first_name || ' ' || u.last_name)) LIKE '%' || lower($4) || '%')",
		"(($5::int = 0 AND $6::bigint = 0) OR (i.date, i.user_id) < ($5, $6))",
	}
	whereSQL := strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_invite_importers i
JOIN users u ON u.id = i.user_id
WHERE `+whereSQL, args[:6]...).Scan(&total); err != nil {
		return domain.ChannelInviteImporterList{}, err
	}
	rows, err := s.db.Query(ctx, `
SELECT i.channel_id, i.invite_id, i.user_id, i.date, i.requested, i.approved_by, i.via_chatlist, i.about
FROM channel_invite_importers i
JOIN users u ON u.id = i.user_id
WHERE `+whereSQL+`
ORDER BY i.date DESC, i.user_id DESC
LIMIT $7`, args...)
	if err != nil {
		return domain.ChannelInviteImporterList{}, err
	}
	defer rows.Close()
	importers := make([]domain.ChannelInviteImporter, 0, limit)
	for rows.Next() {
		var importer domain.ChannelInviteImporter
		if err := rows.Scan(&importer.ChannelID, &importer.InviteID, &importer.UserID, &importer.Date, &importer.Requested, &importer.ApprovedBy, &importer.ViaChatlist, &importer.About); err != nil {
			return domain.ChannelInviteImporterList{}, err
		}
		importers = append(importers, importer)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelInviteImporterList{}, err
	}
	return domain.ChannelInviteImporterList{Count: total, Importers: importers}, nil
}

func (s *ChannelStore) PendingJoinRequests(ctx context.Context, channelID int64, limit int) (domain.ChannelPendingJoinRequests, error) {
	if channelID == 0 {
		return domain.ChannelPendingJoinRequests{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxChannelPendingJoinRecentRequesters {
		limit = domain.MaxChannelPendingJoinRecentRequesters
	}
	rows, err := s.db.Query(ctx, `
SELECT user_id, COUNT(*) OVER()::int
FROM channel_invite_importers
WHERE channel_id = $1 AND requested
ORDER BY date DESC, user_id DESC
LIMIT $2`, channelID, limit)
	if err != nil {
		return domain.ChannelPendingJoinRequests{}, fmt.Errorf("list pending channel join requests: %w", err)
	}
	defer rows.Close()
	out := domain.ChannelPendingJoinRequests{
		ChannelID:        channelID,
		RecentRequesters: make([]int64, 0, limit),
	}
	for rows.Next() {
		var userID int64
		var count int
		if err := rows.Scan(&userID, &count); err != nil {
			return domain.ChannelPendingJoinRequests{}, err
		}
		out.Count = count
		out.RecentRequesters = append(out.RecentRequesters, userID)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelPendingJoinRequests{}, err
	}
	return out, nil
}

func (s *ChannelStore) HideChatJoinRequest(ctx context.Context, req domain.HideChannelJoinRequestRequest) (domain.CreateChannelResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TargetUserID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.CreateChannelResult{}, fmt.Errorf("hide channel join request: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("begin hide channel join request: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	if !canExportChannelInvite(member) {
		return domain.CreateChannelResult{}, domain.ErrChannelAdminRequired
	}
	importer, err := s.getPendingInviteImporterTx(ctx, tx, req.ChannelID, req.TargetUserID, true)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	invite := domain.ChannelInvite{ChannelID: req.ChannelID, AdminUserID: req.UserID}
	if importer.InviteID != 0 {
		invite, err = s.getInviteByID(ctx, tx, req.ChannelID, importer.InviteID, true)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
	}
	var result domain.CreateChannelResult
	if req.Approved {
		result, err = s.approveInviteImporterTx(ctx, tx, channel, invite, req.TargetUserID, req.UserID, req.Date, &reserved)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
	} else if err := deletePendingInviteImporterTx(ctx, tx, invite, req.TargetUserID); err != nil {
		return domain.CreateChannelResult{}, err
	} else {
		result = domain.CreateChannelResult{Channel: channel}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("commit hide channel join request: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	result.Recipients = recipients
	return result, nil
}

func (s *ChannelStore) HideAllChatJoinRequests(ctx context.Context, req domain.HideChannelJoinRequestsRequest) (domain.CreateChannelResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	var inviteID int64
	if req.Hash != "" {
		invite, err := s.GetExportedInvite(ctx, domain.GetChannelInviteRequest{UserID: req.UserID, ChannelID: req.ChannelID, Hash: req.Hash})
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		inviteID = invite.InviteID
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelHideJoinRequests {
		limit = domain.MaxChannelHideJoinRequests
	}
	rows, err := s.db.Query(ctx, `
SELECT user_id
FROM channel_invite_importers
WHERE channel_id = $1 AND requested AND ($2::bigint = 0 OR invite_id = $2)
ORDER BY date ASC, user_id ASC
LIMIT $3`, req.ChannelID, inviteID, limit)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	targets := make([]int64, 0, limit)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			rows.Close()
			return domain.CreateChannelResult{}, err
		}
		targets = append(targets, userID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.CreateChannelResult{}, err
	}
	rows.Close()
	var result domain.CreateChannelResult
	for _, target := range targets {
		next, err := s.HideChatJoinRequest(ctx, domain.HideChannelJoinRequestRequest{
			UserID:       req.UserID,
			ChannelID:    req.ChannelID,
			TargetUserID: target,
			Approved:     req.Approved,
			Date:         req.Date,
		})
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		result = next
	}
	if result.Channel.ID == 0 {
		ch, _, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		result.Channel = ch
		recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
		result.Recipients = recipients
	}
	return result, nil
}

func channelDialogDynamicUnreadCountSQL(readInboxExpr, topIDExpr string) string {
	return fmt.Sprintf(`(
           SELECT COUNT(*)::int
           FROM channel_messages cm_unread
           WHERE cm_unread.channel_id = c.id
             AND cm_unread.id > GREATEST(%s, m.available_min_id)
             AND cm_unread.id <= %s
             AND NOT cm_unread.deleted
             AND cm_unread.sender_user_id <> m.user_id
       )`, readInboxExpr, topIDExpr)
}

func channelDialogDynamicUnreadExistsSQL(readInboxExpr, topIDExpr string) string {
	return fmt.Sprintf(`EXISTS (
           SELECT 1
           FROM channel_messages cm_unread
           WHERE cm_unread.channel_id = c.id
             AND cm_unread.id > GREATEST(%s, m.available_min_id)
             AND cm_unread.id <= %s
             AND NOT cm_unread.deleted
             AND cm_unread.sender_user_id <> m.user_id
       )`, readInboxExpr, topIDExpr)
}

func channelDialogVisibleUnreadCountSQL(readInboxExpr, topIDExpr string) string {
	dynamicCount := channelDialogDynamicUnreadCountSQL(readInboxExpr, topIDExpr)
	return fmt.Sprintf(`CASE
           WHEN c.broadcast OR c.participants_count > %d THEN %s
           ELSE COALESCE(d.unread_count, %s)
       END`, domain.MaxSynchronousChannelDialogFanout, dynamicCount, dynamicCount)
}

func channelDialogHasUnreadSQL(readInboxExpr, topIDExpr string) string {
	dynamicUnread := channelDialogDynamicUnreadExistsSQL(readInboxExpr, topIDExpr)
	return fmt.Sprintf(`CASE
           WHEN c.broadcast OR c.participants_count > %d THEN %s
           ELSE COALESCE(d.unread_count > 0, %s)
       END`, domain.MaxSynchronousChannelDialogFanout, dynamicUnread, dynamicUnread)
}

func (s *ChannelStore) ListChannelDialogs(ctx context.Context, viewerUserID int64, filter domain.DialogFilter) (domain.ChannelDialogList, error) {
	if viewerUserID == 0 {
		return domain.ChannelDialogList{}, nil
	}
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	visibleTopID := "CASE WHEN c.top_message_id > m.available_min_id THEN c.top_message_id ELSE 0 END"
	visibleTopDate := "CASE WHEN c.top_message_id > m.available_min_id THEN COALESCE(top_msg.message_date, d.top_message_date, c.date) ELSE 0 END"
	visibleReadInbox := "COALESCE(d.read_inbox_max_id, m.read_inbox_max_id)"
	visibleUnreadCount := channelDialogVisibleUnreadCountSQL(visibleReadInbox, visibleTopID)
	args := []any{viewerUserID}
	where := []string{"m.user_id = $1", "m.status = 'active'"}
	if filter.HasFolderID && filter.FolderID < domain.DialogCustomFolderMinID {
		args = append(args, filter.FolderID)
		where = append(where, fmt.Sprintf("COALESCE(d.folder_id, 0) = $%d", len(args)))
	}
	if filter.PinnedOnly {
		where = append(where, "COALESCE(d.pinned, false)")
	}
	if filter.ExcludePinned {
		where = append(where, "NOT COALESCE(d.pinned, false)")
	}
	switch {
	case filter.OffsetDate > 0:
		args = append(args, filter.OffsetDate, filter.OffsetID)
		dateArg := fmt.Sprintf("$%d", len(args)-1)
		idArg := fmt.Sprintf("$%d", len(args))
		if filter.HasOffsetPeer && filter.OffsetPeer.Type == domain.PeerTypeChannel && filter.OffsetPeer.ID > 0 {
			args = append(args, filter.OffsetPeer.ID)
			peerArg := fmt.Sprintf("$%d", len(args))
			where = append(where, fmt.Sprintf("(%s < %s OR (%s = %s AND %s < %s) OR (%s = %s AND %s = %s AND c.id < %s))",
				visibleTopDate, dateArg,
				visibleTopDate, dateArg, visibleTopID, idArg,
				visibleTopDate, dateArg, visibleTopID, idArg, peerArg))
		} else {
			where = append(where, fmt.Sprintf("(%s < %s OR (%s = %s AND %s < %s))",
				visibleTopDate, dateArg,
				visibleTopDate, dateArg, visibleTopID, idArg))
		}
	case filter.OffsetID > 0:
		args = append(args, filter.OffsetID)
		idArg := fmt.Sprintf("$%d", len(args))
		if filter.HasOffsetPeer && filter.OffsetPeer.Type == domain.PeerTypeChannel && filter.OffsetPeer.ID > 0 {
			args = append(args, filter.OffsetPeer.ID)
			peerArg := fmt.Sprintf("$%d", len(args))
			where = append(where, fmt.Sprintf("(%s < %s OR (%s = %s AND c.id < %s))",
				visibleTopID, idArg, visibleTopID, idArg, peerArg))
		} else {
			where = append(where, fmt.Sprintf("%s < %s", visibleTopID, idArg))
		}
	case filter.HasOffsetPeer && filter.OffsetPeer.Type == domain.PeerTypeChannel && filter.OffsetPeer.ID > 0:
		args = append(args, filter.OffsetPeer.ID)
		where = append(where, fmt.Sprintf("c.id <> $%d", len(args)))
	}
	if filter.Folder != nil {
		folder := filter.Folder
		if folder.ExcludeArchived {
			where = append(where, fmt.Sprintf("COALESCE(d.folder_id, 0) <> %d", domain.DialogArchiveFolderID))
		}
		if folder.ExcludeRead {
			where = append(where, fmt.Sprintf(`(COALESCE(d.unread_mark, m.unread_mark) OR %s)`,
				channelDialogHasUnreadSQL(visibleReadInbox, visibleTopID)))
		}
		if excludeIDs := channelFolderPeerIDs(folder.ExcludePeers); len(excludeIDs) > 0 {
			args = append(args, excludeIDs)
			where = append(where, fmt.Sprintf("NOT (c.id = ANY($%d::bigint[]))", len(args)))
		}
		includeIDs := channelFolderPeerIDs(folder.IncludePeers, folder.PinnedPeers)
		include := make([]string, 0, 3)
		if len(includeIDs) > 0 {
			args = append(args, includeIDs)
			include = append(include, fmt.Sprintf("c.id = ANY($%d::bigint[])", len(args)))
		}
		if folder.Groups {
			include = append(include, "c.megagroup")
		}
		if folder.Broadcasts {
			include = append(include, "c.broadcast")
		}
		if len(include) > 0 {
			where = append(where, "("+strings.Join(include, " OR ")+")")
		}
	}
	args = append(args, channelDialogQueryLimit)
	limitArg := fmt.Sprintf("$%d", len(args))
	rows, err := s.db.Query(ctx, `
SELECT `+channelColumns+`,
       `+visibleTopID+`,
       `+visibleTopDate+`,
       COALESCE(d.folder_id, 0), `+visibleReadInbox+`,
       COALESCE(d.read_outbox_max_id, m.read_outbox_max_id), `+visibleUnreadCount+`,
       COALESCE(d.pinned, false),
       COALESCE(d.pinned_order, 0), COALESCE(d.unread_mark, m.unread_mark),
       COALESCE(d.unread_mentions_count, 0), COALESCE(d.unread_reactions_count, 0),
       COALESCE(d.view_forum_as_messages, false)
FROM channel_members m
JOIN channels c ON c.id = m.channel_id AND NOT c.deleted
LEFT JOIN channel_messages top_msg ON top_msg.channel_id = c.id AND top_msg.id = c.top_message_id AND NOT top_msg.deleted
LEFT JOIN channel_dialogs d ON d.user_id = m.user_id AND d.channel_id = m.channel_id
WHERE `+strings.Join(where, " AND ")+`
ORDER BY COALESCE(d.pinned, false) DESC,
         COALESCE(d.pinned_order, 0) DESC,
         `+visibleTopDate+` DESC,
         `+visibleTopID+` DESC,
         c.id DESC
LIMIT `+limitArg, args...)
	if err != nil {
		return domain.ChannelDialogList{}, fmt.Errorf("list channel dialogs: %w", err)
	}
	defer rows.Close()
	type item struct {
		channel domain.Channel
		dialog  domain.Dialog
	}
	items := make([]item, 0, limit)
	for rows.Next() {
		ch, dialog, err := scanChannelDialogRow(rows, viewerUserID)
		if err != nil {
			return domain.ChannelDialogList{}, err
		}
		if !channelDialogMatchesFilter(dialog, ch, filter) {
			continue
		}
		items = append(items, item{channel: ch, dialog: dialog})
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelDialogList{}, err
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].dialog.Pinned != items[j].dialog.Pinned {
			return items[i].dialog.Pinned
		}
		if items[i].dialog.PinnedOrder != items[j].dialog.PinnedOrder {
			return items[i].dialog.PinnedOrder > items[j].dialog.PinnedOrder
		}
		if items[i].dialog.TopMessageDate != items[j].dialog.TopMessageDate {
			return items[i].dialog.TopMessageDate > items[j].dialog.TopMessageDate
		}
		if items[i].dialog.TopMessage != items[j].dialog.TopMessage {
			return items[i].dialog.TopMessage > items[j].dialog.TopMessage
		}
		return items[i].dialog.Peer.ID > items[j].dialog.Peer.ID
	})
	out := domain.ChannelDialogList{Count: len(items)}
	if len(items) > limit {
		items = items[:limit]
	}
	for _, item := range items {
		msg, _ := s.getChannelMessage(ctx, s.db, item.channel.ID, item.dialog.TopMessage)
		if msg.ID != 0 {
			item.dialog.TopMessageDate = msg.Date
			out.Messages = append(out.Messages, msg)
		}
		out.Dialogs = append(out.Dialogs, item.dialog)
		out.Channels = append(out.Channels, item.channel)
	}
	return out, nil
}

func (s *ChannelStore) GetChannelDialogs(ctx context.Context, viewerUserID int64, channelIDs []int64) (domain.ChannelDialogList, error) {
	out := domain.ChannelDialogList{}
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID == 0 {
			continue
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		channel, _, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID)
		if err != nil {
			if errors.Is(err, domain.ErrChannelInvalid) || errors.Is(err, domain.ErrChannelPrivate) {
				continue
			}
			return domain.ChannelDialogList{}, err
		}
		dialog, err := s.getChannelDialog(ctx, s.db, viewerUserID, channel)
		if err != nil {
			return domain.ChannelDialogList{}, err
		}
		msg, _ := s.getChannelMessage(ctx, s.db, channelID, dialog.TopMessageID)
		if msg.ID != 0 {
			dialog.TopMessageDate = msg.Date
			out.Messages = append(out.Messages, msg)
		}
		out.Dialogs = append(out.Dialogs, channelDialogToDialog(dialog))
		out.Channels = append(out.Channels, channel)
	}
	out.Count = len(out.Dialogs)
	return out, nil
}

func (s *ChannelStore) ListCommonChannels(ctx context.Context, req domain.CommonChannelsRequest) (domain.CommonChannelsResult, error) {
	if req.UserID == 0 || req.TargetUserID == 0 || req.UserID == req.TargetUserID || req.MaxID < 0 {
		return domain.CommonChannelsResult{}, domain.ErrChannelInvalid
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxCommonChannelsLimit {
		limit = domain.MaxCommonChannelsLimit
	}
	var count int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM user_channel_member_index selfm
JOIN user_channel_member_index targetm ON targetm.channel_id = selfm.channel_id
WHERE selfm.user_id = $1
  AND targetm.user_id = $2
  AND selfm.status = 'active'
  AND targetm.status = 'active'
  AND selfm.megagroup
  AND NOT selfm.broadcast
  AND NOT selfm.deleted
  AND targetm.megagroup
  AND NOT targetm.broadcast
  AND NOT targetm.deleted`, req.UserID, req.TargetUserID).Scan(&count); err != nil {
		return domain.CommonChannelsResult{}, fmt.Errorf("count common channels: %w", err)
	}
	out := domain.CommonChannelsResult{Count: count}
	if req.CountOnly {
		return out, nil
	}
	rows, err := s.db.Query(ctx, `
SELECT selfm.channel_id
FROM user_channel_member_index selfm
JOIN user_channel_member_index targetm ON targetm.channel_id = selfm.channel_id
WHERE selfm.user_id = $1
  AND targetm.user_id = $2
  AND selfm.status = 'active'
  AND targetm.status = 'active'
  AND selfm.megagroup
  AND NOT selfm.broadcast
  AND NOT selfm.deleted
  AND targetm.megagroup
  AND NOT targetm.broadcast
  AND NOT targetm.deleted
  AND ($3::bigint = 0 OR selfm.channel_id > $3)
ORDER BY selfm.channel_id ASC
LIMIT $4`, req.UserID, req.TargetUserID, req.MaxID, limit)
	if err != nil {
		return domain.CommonChannelsResult{}, fmt.Errorf("list common channels: %w", err)
	}
	defer rows.Close()
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return domain.CommonChannelsResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return domain.CommonChannelsResult{}, err
	}
	channels, err := listChannelsByIDs(ctx, s.db, ids)
	if err != nil {
		return domain.CommonChannelsResult{}, err
	}
	out.Channels = channels
	return out, nil
}

func (s *ChannelStore) ListLeftChannels(ctx context.Context, userID int64, offset, limit int) (domain.LeftChannelsResult, error) {
	if userID == 0 || offset < 0 || offset > domain.MaxLeftChannelsOffset {
		return domain.LeftChannelsResult{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxLeftChannelsLimit {
		limit = domain.MaxLeftChannelsLimit
	}
	var count int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_members m
JOIN channels c ON c.id = m.channel_id
WHERE m.user_id = $1
  AND m.status = 'left'
  AND (c.broadcast OR c.megagroup)
  AND NOT c.deleted`, userID).Scan(&count); err != nil {
		return domain.LeftChannelsResult{}, fmt.Errorf("count left channels: %w", err)
	}
	rows, err := s.db.Query(ctx, `
SELECT `+channelColumns+`,
       m.channel_id, m.user_id, m.inviter_user_id, m.role, m.status, m.joined_at, m.left_at,
       m.admin_rights::text, m.banned_rights::text, m.rank, m.available_min_id, m.available_min_pts,
       m.read_inbox_max_id, m.read_outbox_max_id, m.unread_mark, m.slowmode_last_send_date
FROM channel_members m
JOIN channels c ON c.id = m.channel_id
WHERE m.user_id = $1
  AND m.status = 'left'
  AND (c.broadcast OR c.megagroup)
  AND NOT c.deleted
ORDER BY m.left_at DESC, c.id DESC
OFFSET $2
LIMIT $3`, userID, offset, limit)
	if err != nil {
		return domain.LeftChannelsResult{}, fmt.Errorf("list left channels: %w", err)
	}
	defer rows.Close()
	out := domain.LeftChannelsResult{Count: count, Channels: make([]domain.LeftChannel, 0, limit)}
	for rows.Next() {
		ch, member, err := scanChannelWithMember(rows)
		if err != nil {
			return domain.LeftChannelsResult{}, err
		}
		out.Channels = append(out.Channels, domain.LeftChannel{Channel: ch, Self: member})
	}
	return out, rows.Err()
}

func (s *ChannelStore) ListInactiveChannels(ctx context.Context, userID int64, limit int) (domain.ChannelDialogList, error) {
	if userID == 0 {
		return domain.ChannelDialogList{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxInactiveChannelsLimit {
		limit = domain.MaxInactiveChannelsLimit
	}
	visibleTopID := "CASE WHEN c.top_message_id > m.available_min_id THEN c.top_message_id ELSE 0 END"
	visibleTopDate := "CASE WHEN c.top_message_id > m.available_min_id THEN COALESCE(top_msg.message_date, d.top_message_date, c.date) ELSE GREATEST(c.date, m.joined_at) END"
	visibleReadInbox := "COALESCE(d.read_inbox_max_id, m.read_inbox_max_id)"
	visibleUnreadCount := channelDialogVisibleUnreadCountSQL(visibleReadInbox, visibleTopID)
	rows, err := s.db.Query(ctx, `
SELECT `+channelColumns+`,
       `+visibleTopID+`,
       `+visibleTopDate+`,
       COALESCE(d.folder_id, 0), `+visibleReadInbox+`,
       COALESCE(d.read_outbox_max_id, m.read_outbox_max_id), `+visibleUnreadCount+`,
       COALESCE(d.pinned, false),
       COALESCE(d.pinned_order, 0), COALESCE(d.unread_mark, m.unread_mark),
       COALESCE(d.unread_mentions_count, 0), COALESCE(d.unread_reactions_count, 0),
       COALESCE(d.view_forum_as_messages, false)
FROM channel_members m
JOIN channels c ON c.id = m.channel_id AND NOT c.deleted
LEFT JOIN channel_messages top_msg ON top_msg.channel_id = c.id AND top_msg.id = c.top_message_id AND NOT top_msg.deleted
LEFT JOIN channel_dialogs d ON d.user_id = m.user_id AND d.channel_id = m.channel_id
WHERE m.user_id = $1
  AND m.status = 'active'
  AND (c.broadcast OR c.megagroup)
ORDER BY `+visibleTopDate+` ASC,
         `+visibleTopID+` ASC,
         c.id ASC
LIMIT $2`, userID, limit)
	if err != nil {
		return domain.ChannelDialogList{}, fmt.Errorf("list inactive channels: %w", err)
	}
	defer rows.Close()
	out := domain.ChannelDialogList{Dialogs: make([]domain.Dialog, 0, limit), Channels: make([]domain.Channel, 0, limit)}
	for rows.Next() {
		ch, dialog, err := scanChannelDialogRow(rows, userID)
		if err != nil {
			return domain.ChannelDialogList{}, err
		}
		out.Dialogs = append(out.Dialogs, dialog)
		out.Channels = append(out.Channels, ch)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelDialogList{}, err
	}
	out.Count = len(out.Dialogs)
	return out, nil
}

func (s *ChannelStore) ListChannelRecommendations(ctx context.Context, req domain.ChannelRecommendationsRequest) (domain.ChannelRecommendationsResult, error) {
	if req.UserID == 0 || req.SourceChannelID < 0 {
		return domain.ChannelRecommendationsResult{}, domain.ErrChannelInvalid
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelRecommendationsLimit {
		limit = domain.DefaultChannelRecommendationsLimit
	}
	args := []any{req.UserID, req.SourceChannelID}
	where := []string{
		"($1::bigint <> 0)",
		"c.broadcast",
		"NOT c.megagroup",
		"NOT c.deleted",
		"COALESCE(c.username, '') <> ''",
		"($2::bigint = 0 OR c.id <> $2)",
	}
	if req.SourceChannelID == 0 {
		where = append(where, `NOT EXISTS (
  SELECT 1
  FROM channel_members m
  WHERE m.channel_id = c.id
    AND m.user_id = $1
    AND m.status = 'active'
)`)
	}
	whereSQL := strings.Join(where, " AND ")
	var count int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channels c
WHERE `+whereSQL, args...).Scan(&count); err != nil {
		return domain.ChannelRecommendationsResult{}, fmt.Errorf("count channel recommendations: %w", err)
	}
	args = append(args, limit)
	rows, err := s.db.Query(ctx, `
SELECT `+channelColumns+`
FROM channels c
WHERE `+whereSQL+`
ORDER BY c.participants_count DESC, c.date DESC, c.id DESC
LIMIT $3`, args...)
	if err != nil {
		return domain.ChannelRecommendationsResult{}, fmt.Errorf("list channel recommendations: %w", err)
	}
	defer rows.Close()
	out := domain.ChannelRecommendationsResult{Count: count, Channels: make([]domain.Channel, 0, limit)}
	for rows.Next() {
		ch, err := scanChannel(rows)
		if err != nil {
			return domain.ChannelRecommendationsResult{}, err
		}
		out.Channels = append(out.Channels, ch)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelRecommendationsResult{}, err
	}
	return out, nil
}

func (s *ChannelStore) ListDiscussionGroups(ctx context.Context, userID int64, limit int) ([]domain.Channel, error) {
	if userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxDiscussionGroupsLimit {
		limit = domain.MaxDiscussionGroupsLimit
	}
	rows, err := s.db.Query(ctx, `
SELECT `+channelColumns+`
FROM channel_members m
JOIN channels c ON c.id = m.channel_id
WHERE m.user_id = $1
  AND m.status = 'active'
  AND c.megagroup
  AND NOT c.broadcast
  AND NOT c.forum
  AND NOT c.deleted
  AND (
      m.role = 'creator'
      OR (m.role = 'admin' AND COALESCE((m.admin_rights->>'PinMessages')::boolean, false))
  )
ORDER BY c.id DESC
LIMIT $2`, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("list discussion groups: %w", err)
	}
	defer rows.Close()
	out := make([]domain.Channel, 0, limit)
	for rows.Next() {
		ch, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ch)
	}
	return out, rows.Err()
}

func (s *ChannelStore) SetDiscussionGroup(ctx context.Context, userID, broadcastID, groupID int64) (domain.DiscussionGroupUpdateResult, error) {
	if userID == 0 {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrChannelInvalid
	}
	if broadcastID == 0 && groupID == 0 {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrLinkNotModified
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.DiscussionGroupUpdateResult{}, fmt.Errorf("set discussion group: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.DiscussionGroupUpdateResult{}, fmt.Errorf("begin set discussion group: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	changed := make(map[int64]domain.Channel)
	markChanged := func(channel domain.Channel) {
		if channel.ID != 0 {
			changed[channel.ID] = channel
		}
	}
	setLinked := func(channel domain.Channel, linkedID int64) (domain.Channel, error) {
		if channel.LinkedChatID == linkedID {
			return channel, nil
		}
		if _, err := tx.Exec(ctx, `UPDATE channels SET linked_chat_id = $2, updated_at = now() WHERE id = $1`, channel.ID, linkedID); err != nil {
			return domain.Channel{}, fmt.Errorf("update linked chat: %w", err)
		}
		channel.LinkedChatID = linkedID
		markChanged(channel)
		return channel, nil
	}
	logLinkChange := func(channelID, prev, next int64) error {
		if prev == next || channelID == 0 {
			return nil
		}
		return s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      nowUnix(),
			Type:      domain.ChannelAdminLogChangeLinkedChat,
			PrevInt:   int(prev),
			NewInt:    int(next),
		})
	}

	if broadcastID == 0 {
		group, groupMember, err := s.getChannelForMember(ctx, tx, userID, groupID)
		if err != nil || !validDiscussionGroup(group) {
			return domain.DiscussionGroupUpdateResult{}, domain.ErrMegagroupIDInvalid
		}
		if !canManageDiscussionGroup(groupMember) {
			return domain.DiscussionGroupUpdateResult{}, domain.ErrChannelAdminRequired
		}
		oldBroadcastID := group.LinkedChatID
		if oldBroadcastID == 0 {
			return domain.DiscussionGroupUpdateResult{}, domain.ErrLinkNotModified
		}
		oldBroadcast, err := getChannelByID(ctx, tx, oldBroadcastID)
		if err == nil && oldBroadcast.LinkedChatID == groupID {
			updated, err := setLinked(oldBroadcast, 0)
			if err != nil {
				return domain.DiscussionGroupUpdateResult{}, err
			}
			if err := logLinkChange(updated.ID, groupID, 0); err != nil {
				return domain.DiscussionGroupUpdateResult{}, err
			}
		} else if err != nil && !errors.Is(err, domain.ErrChannelInvalid) {
			return domain.DiscussionGroupUpdateResult{}, err
		}
		if _, err := setLinked(group, 0); err != nil {
			return domain.DiscussionGroupUpdateResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.DiscussionGroupUpdateResult{}, fmt.Errorf("commit set discussion group: %w", err)
		}
		committed = true
		return discussionGroupUpdateResult(changed), nil
	}

	broadcast, broadcastMember, err := s.getChannelForMember(ctx, tx, userID, broadcastID)
	if err != nil || !broadcast.Broadcast || broadcast.Megagroup {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrBroadcastIDInvalid
	}
	if !canManageDiscussionBroadcast(broadcastMember) {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrChannelAdminRequired
	}
	oldGroupID := broadcast.LinkedChatID
	if groupID == 0 {
		if oldGroupID == 0 {
			return domain.DiscussionGroupUpdateResult{}, domain.ErrLinkNotModified
		}
		updated, err := setLinked(broadcast, 0)
		if err != nil {
			return domain.DiscussionGroupUpdateResult{}, err
		}
		if err := logLinkChange(updated.ID, oldGroupID, 0); err != nil {
			return domain.DiscussionGroupUpdateResult{}, err
		}
		oldGroup, err := getChannelByID(ctx, tx, oldGroupID)
		if err == nil && oldGroup.LinkedChatID == broadcastID {
			if _, err := setLinked(oldGroup, 0); err != nil {
				return domain.DiscussionGroupUpdateResult{}, err
			}
		} else if err != nil && !errors.Is(err, domain.ErrChannelInvalid) {
			return domain.DiscussionGroupUpdateResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.DiscussionGroupUpdateResult{}, fmt.Errorf("commit set discussion group: %w", err)
		}
		committed = true
		return discussionGroupUpdateResult(changed), nil
	}

	group, groupMember, err := s.getChannelForMember(ctx, tx, userID, groupID)
	if err != nil || !validDiscussionGroup(group) {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrMegagroupIDInvalid
	}
	if group.PreHistoryHidden {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrMegagroupPrehistoryHidden
	}
	if !canManageDiscussionGroup(groupMember) {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrChannelAdminRequired
	}
	if oldGroupID == groupID && group.LinkedChatID == broadcastID {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrLinkNotModified
	}
	oldBroadcastID := group.LinkedChatID
	if oldGroupID != 0 && oldGroupID != groupID {
		oldGroup, err := getChannelByID(ctx, tx, oldGroupID)
		if err == nil && oldGroup.LinkedChatID == broadcastID {
			if _, err := setLinked(oldGroup, 0); err != nil {
				return domain.DiscussionGroupUpdateResult{}, err
			}
		} else if err != nil && !errors.Is(err, domain.ErrChannelInvalid) {
			return domain.DiscussionGroupUpdateResult{}, err
		}
	}
	if oldBroadcastID != 0 && oldBroadcastID != broadcastID {
		oldBroadcast, err := getChannelByID(ctx, tx, oldBroadcastID)
		if err == nil && oldBroadcast.LinkedChatID == groupID {
			updated, err := setLinked(oldBroadcast, 0)
			if err != nil {
				return domain.DiscussionGroupUpdateResult{}, err
			}
			if err := logLinkChange(updated.ID, groupID, 0); err != nil {
				return domain.DiscussionGroupUpdateResult{}, err
			}
		} else if err != nil && !errors.Is(err, domain.ErrChannelInvalid) {
			return domain.DiscussionGroupUpdateResult{}, err
		}
	}
	updatedBroadcast, err := setLinked(broadcast, groupID)
	if err != nil {
		return domain.DiscussionGroupUpdateResult{}, err
	}
	if _, err := setLinked(group, broadcastID); err != nil {
		return domain.DiscussionGroupUpdateResult{}, err
	}
	if err := logLinkChange(updatedBroadcast.ID, oldGroupID, groupID); err != nil {
		return domain.DiscussionGroupUpdateResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DiscussionGroupUpdateResult{}, fmt.Errorf("commit set discussion group: %w", err)
	}
	committed = true
	return discussionGroupUpdateResult(changed), nil
}

func (s *ChannelStore) SetChannelDialogPinned(ctx context.Context, userID, channelID int64, pinned bool) (bool, error) {
	if userID == 0 || channelID == 0 {
		return false, nil
	}
	var changed bool
	if err := s.db.QueryRow(ctx, `
WITH target AS (
    SELECT c.id AS channel_id, c.top_message_id, c.date AS top_message_date
    FROM channels c
    JOIN channel_members m ON m.channel_id = c.id
    WHERE c.id = $2 AND m.user_id = $1 AND m.status = 'active' AND NOT c.deleted
),
ensured AS (
    INSERT INTO channel_dialogs (user_id, channel_id, top_message_id, top_message_date)
    SELECT $1, channel_id, top_message_id, top_message_date FROM target
    ON CONFLICT (user_id, channel_id) DO NOTHING
),
next_order AS (
    SELECT COALESCE(MAX(pinned_order), 0)::int + 1 AS value
    FROM channel_dialogs
    WHERE user_id = $1 AND pinned
),
updated AS (
    UPDATE channel_dialogs d
    SET pinned = $3,
        pinned_order = CASE
            WHEN $3::boolean THEN CASE WHEN d.pinned_order > 0 THEN d.pinned_order ELSE next_order.value END
            ELSE 0
        END,
        updated_at = now()
    FROM next_order
    WHERE d.user_id = $1 AND d.channel_id = $2
      AND EXISTS (SELECT 1 FROM target)
      AND (d.pinned IS DISTINCT FROM $3::boolean OR ($3::boolean AND d.pinned_order = 0))
    RETURNING d.user_id
)
SELECT EXISTS (SELECT 1 FROM updated)::boolean`, userID, channelID, pinned).Scan(&changed); err != nil {
		return false, fmt.Errorf("set channel dialog pinned: %w", err)
	}
	return changed, nil
}

func (s *ChannelStore) ReorderChannelPinnedDialogs(ctx context.Context, userID int64, order []domain.Peer, force bool) error {
	if userID == 0 {
		return nil
	}
	peerTypes, peerIDs := peerArrays(order)
	if force {
		if _, err := s.db.Exec(ctx, `
WITH requested AS (
    SELECT ($2::text[])[i] AS peer_type, ($3::bigint[])[i] AS peer_id
    FROM generate_subscripts($3::bigint[], 1) AS g(i)
    WHERE i <= cardinality($2::text[])
)
UPDATE channel_dialogs d
SET pinned = false, pinned_order = 0, updated_at = now()
WHERE d.user_id = $1
  AND d.pinned
  AND NOT EXISTS (
      SELECT 1 FROM requested r
      WHERE r.peer_type = 'channel' AND r.peer_id = d.channel_id
  )`, userID, peerTypes, peerIDs); err != nil {
			return fmt.Errorf("clear channel pinned dialogs not in order: %w", err)
		}
	}
	if len(peerIDs) == 0 {
		return nil
	}
	if _, err := s.db.Exec(ctx, `
WITH requested AS (
    SELECT ($2::text[])[i] AS peer_type, ($3::bigint[])[i] AS peer_id, i::int AS pos
    FROM generate_subscripts($3::bigint[], 1) AS g(i)
    WHERE i <= cardinality($2::text[])
),
deduped AS (
    SELECT DISTINCT ON (peer_id) peer_id, (cardinality($3::bigint[]) - pos + 1)::int AS ord
    FROM requested
    WHERE peer_type = 'channel'
    ORDER BY peer_id, pos
)
UPDATE channel_dialogs d
SET pinned = true, pinned_order = deduped.ord, updated_at = now()
FROM deduped
WHERE d.user_id = $1 AND d.channel_id = deduped.peer_id`, userID, peerTypes, peerIDs); err != nil {
		return fmt.Errorf("reorder channel pinned dialogs: %w", err)
	}
	return nil
}

func (s *ChannelStore) SetChannelDialogUnreadMark(ctx context.Context, userID, channelID int64, unread bool) (bool, error) {
	if userID == 0 || channelID == 0 {
		return false, nil
	}
	var changed bool
	if err := s.db.QueryRow(ctx, `
WITH target AS (
    SELECT c.id AS channel_id, c.top_message_id, c.date AS top_message_date
    FROM channels c
    JOIN channel_members m ON m.channel_id = c.id
    WHERE c.id = $2 AND m.user_id = $1 AND m.status = 'active' AND NOT c.deleted
),
ensured AS (
    INSERT INTO channel_dialogs (user_id, channel_id, top_message_id, top_message_date)
    SELECT $1, channel_id, top_message_id, top_message_date FROM target
    ON CONFLICT (user_id, channel_id) DO NOTHING
),
updated_dialog AS (
    UPDATE channel_dialogs d
    SET unread_mark = $3, updated_at = now()
    WHERE d.user_id = $1 AND d.channel_id = $2
      AND EXISTS (SELECT 1 FROM target)
      AND d.unread_mark IS DISTINCT FROM $3::boolean
    RETURNING d.user_id
),
updated_member AS (
    UPDATE channel_members m
    SET unread_mark = $3
    WHERE m.user_id = $1 AND m.channel_id = $2 AND m.status = 'active'
    RETURNING m.user_id
)
SELECT EXISTS (SELECT 1 FROM updated_dialog)::boolean`, userID, channelID, unread).Scan(&changed); err != nil {
		return false, fmt.Errorf("set channel dialog unread mark: %w", err)
	}
	return changed, nil
}

func (s *ChannelStore) SetChannelViewForumAsMessages(ctx context.Context, userID, channelID int64, enabled bool) (bool, error) {
	if userID == 0 || channelID == 0 {
		return false, nil
	}
	var changed bool
	if err := s.db.QueryRow(ctx, `
WITH target AS (
    SELECT c.id AS channel_id, c.top_message_id, c.date AS top_message_date
    FROM channels c
    JOIN channel_members m ON m.channel_id = c.id
    WHERE c.id = $2 AND m.user_id = $1 AND m.status = 'active' AND NOT c.deleted
),
ensured AS (
    INSERT INTO channel_dialogs (user_id, channel_id, top_message_id, top_message_date)
    SELECT $1, channel_id, top_message_id, top_message_date FROM target
    ON CONFLICT (user_id, channel_id) DO NOTHING
),
updated_dialog AS (
    UPDATE channel_dialogs d
    SET view_forum_as_messages = $3, updated_at = now()
    WHERE d.user_id = $1 AND d.channel_id = $2
      AND EXISTS (SELECT 1 FROM target)
      AND d.view_forum_as_messages IS DISTINCT FROM $3::boolean
    RETURNING d.user_id
)
SELECT EXISTS (SELECT 1 FROM updated_dialog)::boolean`, userID, channelID, enabled).Scan(&changed); err != nil {
		return false, fmt.Errorf("set channel view forum as messages: %w", err)
	}
	return changed, nil
}

func (s *ChannelStore) ListChannelUnreadMarked(ctx context.Context, userID int64) ([]domain.Peer, error) {
	if userID == 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
SELECT d.channel_id
FROM channel_dialogs d
JOIN channel_members m ON m.channel_id = d.channel_id AND m.user_id = d.user_id AND m.status = 'active'
JOIN channels c ON c.id = d.channel_id AND NOT c.deleted
WHERE d.user_id = $1 AND d.unread_mark
ORDER BY d.top_message_date DESC, d.top_message_id DESC, d.channel_id DESC
LIMIT 500`, userID)
	if err != nil {
		return nil, fmt.Errorf("list channel unread marks: %w", err)
	}
	defer rows.Close()
	out := make([]domain.Peer, 0)
	for rows.Next() {
		var channelID int64
		if err := rows.Scan(&channelID); err != nil {
			return nil, err
		}
		out = append(out, domain.Peer{Type: domain.PeerTypeChannel, ID: channelID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ChannelStore) EditChannelPeerFolders(ctx context.Context, userID int64, peers []domain.FolderPeerUpdate) error {
	if userID == 0 || len(peers) == 0 {
		return nil
	}
	peerTypes := make([]string, 0, len(peers))
	peerIDs := make([]int64, 0, len(peers))
	folderIDs := make([]int32, 0, len(peers))
	seen := make(map[int64]struct{}, len(peers))
	for _, item := range peers {
		if item.Peer.Type != domain.PeerTypeChannel || item.Peer.ID == 0 {
			continue
		}
		if item.FolderID != domain.DialogMainFolderID && item.FolderID != domain.DialogArchiveFolderID {
			continue
		}
		if _, ok := seen[item.Peer.ID]; ok {
			continue
		}
		seen[item.Peer.ID] = struct{}{}
		peerTypes = append(peerTypes, string(item.Peer.Type))
		peerIDs = append(peerIDs, item.Peer.ID)
		folderIDs = append(folderIDs, int32(item.FolderID))
	}
	if len(peerIDs) == 0 {
		return nil
	}
	if _, err := s.db.Exec(ctx, `
WITH requested AS (
    SELECT ($2::text[])[i] AS peer_type, ($3::bigint[])[i] AS channel_id, ($4::int[])[i] AS folder_id
    FROM generate_subscripts($3::bigint[], 1) AS g(i)
    WHERE i <= cardinality($2::text[]) AND i <= cardinality($4::int[])
),
deduped AS (
    SELECT DISTINCT ON (channel_id) channel_id, folder_id
    FROM requested
    WHERE peer_type = 'channel' AND folder_id IN (0, 1)
    ORDER BY channel_id
)
UPDATE channel_dialogs d
SET folder_id = deduped.folder_id, updated_at = now()
FROM deduped
WHERE d.user_id = $1
  AND d.channel_id = deduped.channel_id
  AND EXISTS (
      SELECT 1 FROM channel_members m
      WHERE m.user_id = d.user_id AND m.channel_id = d.channel_id AND m.status = 'active'
  )`, userID, peerTypes, peerIDs, folderIDs); err != nil {
		return fmt.Errorf("edit channel peer folders: %w", err)
	}
	return nil
}

func (s *ChannelStore) ListChannelHistory(ctx context.Context, viewerUserID int64, filter domain.ChannelHistoryFilter) (domain.ChannelHistory, error) {
	channel, member, _, err := s.getChannelForViewer(ctx, s.db, viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	// 公共过滤条件（不含 offset 锚点的方向条件，供 add_offset 各模式复用）
	baseArgs := []any{filter.ChannelID}
	base := "channel_id = $1 AND NOT deleted"
	if member.AvailableMinID > 0 {
		baseArgs = append(baseArgs, member.AvailableMinID)
		base += fmt.Sprintf(" AND id > $%d", len(baseArgs))
	}
	if filter.Query != "" {
		baseArgs = append(baseArgs, filter.Query)
		base += fmt.Sprintf(" AND body ILIKE '%%' || $%d || '%%'", len(baseArgs))
	}
	if filter.SenderUserID != 0 {
		baseArgs = append(baseArgs, filter.SenderUserID)
		base += fmt.Sprintf(" AND sender_user_id = $%d", len(baseArgs))
	}
	if filter.MinDate > 0 {
		baseArgs = append(baseArgs, filter.MinDate)
		base += fmt.Sprintf(" AND message_date > $%d", len(baseArgs))
	}
	if filter.MaxDate > 0 {
		baseArgs = append(baseArgs, filter.MaxDate)
		base += fmt.Sprintf(" AND message_date < $%d", len(baseArgs))
	}
	if filter.MaxID > 0 {
		baseArgs = append(baseArgs, filter.MaxID)
		base += fmt.Sprintf(" AND id <= $%d", len(baseArgs))
	}
	if filter.MinID > 0 {
		baseArgs = append(baseArgs, filter.MinID)
		base += fmt.Sprintf(" AND id > $%d", len(baseArgs))
	}
	scanList := func(sql string, queryArgs []any) ([]domain.ChannelMessage, error) {
		rows, err := s.db.Query(ctx, sql, queryArgs...)
		if err != nil {
			return nil, fmt.Errorf("list channel history: %w", err)
		}
		defer rows.Close()
		var list []domain.ChannelMessage
		for rows.Next() {
			msg, scanErr := scanChannelMessage(rows)
			if scanErr != nil {
				return nil, scanErr
			}
			list = append(list, msg)
		}
		return list, rows.Err()
	}
	// add_offset 决定加载方向（对齐私聊 ListMessagesByUser）：
	//   >= 0           backward：锚点更旧方向，先跳过 add_offset 条
	//   < 0 且 +limit>0 around：以锚点为中心，向更新取 -add_offset 条 + 向更旧取 limit+add_offset 条
	//   否则           forward：仅锚点更新方向（拉未读消息）
	addOffset := filter.AddOffset
	out := domain.ChannelHistory{Channel: channel, Self: member}
	hasMoreOlder := false
	// 锚点条件：offset_date 优先按日期、否则按消息 id（对齐私聊/orange）；
	// 二者皆空时向更新方向退化为空、向更旧方向退化为全部（取最新）。
	forwardCond := func(args *[]any) string {
		if filter.OffsetDate > 0 {
			*args = append(*args, filter.OffsetDate)
			return fmt.Sprintf("message_date >= $%d", len(*args))
		}
		if filter.OffsetID > 0 {
			*args = append(*args, filter.OffsetID)
			return fmt.Sprintf("id > $%d", len(*args))
		}
		return "false"
	}
	aroundOlderCond := func(args *[]any) string {
		if filter.OffsetDate > 0 {
			*args = append(*args, filter.OffsetDate)
			return fmt.Sprintf("message_date < $%d", len(*args))
		}
		if filter.OffsetID > 0 {
			*args = append(*args, filter.OffsetID)
			return fmt.Sprintf("id <= $%d", len(*args))
		}
		return "true"
	}
	switch {
	case addOffset < 0 && addOffset+limit > 0:
		// around：以锚点为中心，向更新取 -add_offset 条 + 向更旧（含锚点）取 limit+add_offset 条
		fwdLimit := minInt(-addOffset, limit)
		bwdLimit := maxInt(limit+addOffset, 0)
		fwdArgs := append([]any{}, baseArgs...)
		fwdWhere := forwardCond(&fwdArgs)
		fwdArgs = append(fwdArgs, fwdLimit)
		newer, err := scanList(fmt.Sprintf("SELECT "+channelMessageColumns+" FROM channel_messages WHERE %s AND %s ORDER BY id ASC LIMIT $%d", base, fwdWhere, len(fwdArgs)), fwdArgs)
		if err != nil {
			return domain.ChannelHistory{}, err
		}
		bwdArgs := append([]any{}, baseArgs...)
		bwdWhere := aroundOlderCond(&bwdArgs)
		bwdArgs = append(bwdArgs, bwdLimit+1)
		older, err := scanList(fmt.Sprintf("SELECT "+channelMessageColumns+" FROM channel_messages WHERE %s AND %s ORDER BY id DESC LIMIT $%d", base, bwdWhere, len(bwdArgs)), bwdArgs)
		if err != nil {
			return domain.ChannelHistory{}, err
		}
		if len(older) > bwdLimit {
			older = older[:bwdLimit]
			hasMoreOlder = true
		}
		for i := len(newer) - 1; i >= 0; i-- {
			out.Messages = append(out.Messages, newer[i])
		}
		out.Messages = append(out.Messages, older...)
	case addOffset < 0:
		// forward：仅锚点更新方向（拉未读/更新消息）
		fwdArgs := append([]any{}, baseArgs...)
		fwdWhere := forwardCond(&fwdArgs)
		fwdArgs = append(fwdArgs, limit+1)
		newer, err := scanList(fmt.Sprintf("SELECT "+channelMessageColumns+" FROM channel_messages WHERE %s AND %s ORDER BY id ASC LIMIT $%d", base, fwdWhere, len(fwdArgs)), fwdArgs)
		if err != nil {
			return domain.ChannelHistory{}, err
		}
		if len(newer) > limit {
			newer = newer[:limit]
		}
		for i := len(newer) - 1; i >= 0; i-- {
			out.Messages = append(out.Messages, newer[i])
		}
	default:
		// backward：锚点更旧方向（不含锚点），先跳过 add_offset 条
		where := base
		args := append([]any{}, baseArgs...)
		if filter.OffsetDate > 0 {
			args = append(args, filter.OffsetDate)
			where += fmt.Sprintf(" AND message_date < $%d", len(args))
		} else if filter.OffsetID > 0 {
			args = append(args, filter.OffsetID)
			where += fmt.Sprintf(" AND id < $%d", len(args))
		}
		args = append(args, limit+1)
		limIdx := len(args)
		sql := "SELECT " + channelMessageColumns + " FROM channel_messages WHERE " + where + " ORDER BY id DESC"
		if addOffset > 0 {
			args = append(args, addOffset)
			sql += fmt.Sprintf(" OFFSET $%d", len(args))
		}
		sql += fmt.Sprintf(" LIMIT $%d", limIdx)
		older, err := scanList(sql, args)
		if err != nil {
			return domain.ChannelHistory{}, err
		}
		if len(older) > limit {
			older = older[:limit]
			hasMoreOlder = true
		}
		out.Messages = older
	}
	out.Count = len(out.Messages)
	if hasMoreOlder {
		out.Count = len(out.Messages) + 1
	}
	if err := s.populateChannelMessageReplies(ctx, s.db, viewerUserID, channel, out.Messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, []domain.Channel{channel}, out.Messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	return out, nil
}

func (s *ChannelStore) SearchPublicPosts(ctx context.Context, viewerUserID int64, req domain.ChannelSearchPostsRequest) (domain.ChannelHistory, error) {
	query := strings.TrimSpace(req.Query)
	hashtag := strings.TrimSpace(req.Hashtag)
	if (query == "") == (hashtag == "") {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelSearchPostsLimit {
		limit = domain.MaxChannelSearchPostsLimit
	}
	args := []any{}
	where := `NOT deleted
AND body <> ''
AND EXISTS (
  SELECT 1
  FROM channels c
  WHERE c.id = channel_messages.channel_id
    AND NOT c.deleted
    AND COALESCE(c.username, '') <> ''
	)`
	if query != "" {
		args = append(args, "%"+escapeLike(query)+"%")
		where += fmt.Sprintf(" AND body ILIKE $%d ESCAPE '\\'", len(args))
	}
	if hashtag != "" {
		args = append(args, "%#"+escapeLike(hashtag)+"%")
		where += fmt.Sprintf(" AND body ILIKE $%d ESCAPE '\\'", len(args))
	}
	switch {
	case req.OffsetRate > 0 && req.OffsetChannelID > 0 && req.OffsetID > 0:
		args = append(args, req.OffsetRate, req.OffsetChannelID, req.OffsetID)
		n := len(args)
		where += fmt.Sprintf(" AND (message_date < $%d OR (message_date = $%d AND (channel_id < $%d OR (channel_id = $%d AND id < $%d))))", n-2, n-2, n-1, n-1, n)
	case req.OffsetRate > 0:
		args = append(args, req.OffsetRate)
		where += fmt.Sprintf(" AND message_date < $%d", len(args))
	case req.OffsetChannelID > 0 && req.OffsetID > 0:
		args = append(args, req.OffsetChannelID, req.OffsetID)
		n := len(args)
		where += fmt.Sprintf(" AND (channel_id < $%d OR (channel_id = $%d AND id < $%d))", n-1, n-1, n)
	case req.OffsetID > 0:
		args = append(args, req.OffsetID)
		where += fmt.Sprintf(" AND id < $%d", len(args))
	}
	queryLimit := limit + 1
	args = append(args, queryLimit)
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE `+where+`
ORDER BY message_date DESC, channel_id DESC, id DESC
LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return domain.ChannelHistory{}, fmt.Errorf("search public channel posts: %w", err)
	}
	defer rows.Close()
	out := domain.ChannelHistory{}
	channelRefs := make(map[int64]struct{})
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return domain.ChannelHistory{}, err
		}
		out.Messages = append(out.Messages, msg)
		channelRefs[msg.ChannelID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelHistory{}, err
	}
	if len(out.Messages) > limit {
		out.Messages = out.Messages[:limit]
		out.Count = limit + 1
		channelRefs = make(map[int64]struct{}, len(out.Messages))
		for _, msg := range out.Messages {
			channelRefs[msg.ChannelID] = struct{}{}
		}
	} else {
		out.Count = len(out.Messages)
	}
	channels, err := listChannelsByIDs(ctx, s.db, mapKeysInt64(channelRefs))
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	out.Channels = channels
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, out.Channels, out.Messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	return out, nil
}

func (s *ChannelStore) SearchJoinedMessages(ctx context.Context, viewerUserID int64, req domain.ChannelGlobalSearchRequest) (domain.ChannelHistory, error) {
	query := strings.TrimSpace(req.Query)
	if viewerUserID == 0 || query == "" {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelGlobalSearchLimit {
		limit = domain.MaxChannelGlobalSearchLimit
	}
	args := []any{viewerUserID, "%" + escapeLike(query) + "%"}
	where := `NOT deleted
AND body <> ''
AND body ILIKE $2 ESCAPE '\'
AND EXISTS (
  SELECT 1
  FROM channels c
  JOIN channel_members cm ON cm.channel_id = c.id
    AND cm.user_id = $1
    AND cm.status = 'active'
    AND NOT COALESCE((cm.banned_rights->>'ViewMessages')::boolean, false)
  LEFT JOIN channel_dialogs d ON d.channel_id = c.id AND d.user_id = $1
  WHERE c.id = channel_messages.channel_id
    AND NOT c.deleted
    AND (cm.available_min_id <= 0 OR channel_messages.id > cm.available_min_id)`
	if req.BroadcastsOnly {
		where += `
    AND c.broadcast AND NOT c.megagroup`
	}
	if req.GroupsOnly {
		where += `
    AND c.megagroup`
	}
	if req.HasFolderID {
		args = append(args, req.FolderID)
		where += fmt.Sprintf(`
    AND d.folder_id = $%d`, len(args))
	}
	where += `
)`
	if req.MinDate > 0 {
		args = append(args, req.MinDate)
		where += fmt.Sprintf(" AND message_date > $%d", len(args))
	}
	if req.MaxDate > 0 {
		args = append(args, req.MaxDate)
		where += fmt.Sprintf(" AND message_date < $%d", len(args))
	}
	switch {
	case req.OffsetRate > 0 && req.OffsetChannelID > 0 && req.OffsetID > 0:
		args = append(args, req.OffsetRate, req.OffsetChannelID, req.OffsetID)
		n := len(args)
		where += fmt.Sprintf(" AND (message_date < $%d OR (message_date = $%d AND (channel_id < $%d OR (channel_id = $%d AND id < $%d))))", n-2, n-2, n-1, n-1, n)
	case req.OffsetRate > 0:
		args = append(args, req.OffsetRate)
		where += fmt.Sprintf(" AND message_date < $%d", len(args))
	case req.OffsetChannelID > 0 && req.OffsetID > 0:
		args = append(args, req.OffsetChannelID, req.OffsetID)
		n := len(args)
		where += fmt.Sprintf(" AND (channel_id < $%d OR (channel_id = $%d AND id < $%d))", n-1, n-1, n)
	case req.OffsetID > 0:
		args = append(args, req.OffsetID)
		where += fmt.Sprintf(" AND id < $%d", len(args))
	}
	queryLimit := limit + 1
	args = append(args, queryLimit)
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE `+where+`
ORDER BY message_date DESC, channel_id DESC, id DESC
LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return domain.ChannelHistory{}, fmt.Errorf("search joined channel messages: %w", err)
	}
	defer rows.Close()
	out := domain.ChannelHistory{}
	channelRefs := make(map[int64]struct{})
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return domain.ChannelHistory{}, err
		}
		out.Messages = append(out.Messages, msg)
		channelRefs[msg.ChannelID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelHistory{}, err
	}
	if len(out.Messages) > limit {
		out.Messages = out.Messages[:limit]
		out.Count = limit + 1
		channelRefs = make(map[int64]struct{}, len(out.Messages))
		for _, msg := range out.Messages {
			channelRefs[msg.ChannelID] = struct{}{}
		}
	} else {
		out.Count = len(out.Messages)
	}
	channels, err := listChannelsByIDs(ctx, s.db, mapKeysInt64(channelRefs))
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	out.Channels = channels
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, out.Channels, out.Messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	return out, nil
}

func (s *ChannelStore) GetChannelMessages(ctx context.Context, viewerUserID, channelID int64, ids []int) (domain.ChannelHistory, error) {
	channel, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	if len(ids) == 0 {
		return domain.ChannelHistory{Channel: channel, Self: member}, nil
	}
	if len(ids) > domain.MaxGetMessageIDs {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	id32, _, err := validUniqueChannelMessageIDs(ids)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	args := []any{channelID, id32}
	where := "channel_id = $1 AND id = ANY($2::int[]) AND NOT deleted"
	if member.AvailableMinID > 0 {
		args = append(args, member.AvailableMinID)
		where += fmt.Sprintf(" AND id > $%d", len(args))
	}
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE `+where+`
ORDER BY id DESC`, args...)
	if err != nil {
		return domain.ChannelHistory{}, fmt.Errorf("get channel messages by ids: %w", err)
	}
	defer rows.Close()
	out := domain.ChannelHistory{Channel: channel, Self: member}
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return domain.ChannelHistory{}, err
		}
		out.Messages = append(out.Messages, msg)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelHistory{}, err
	}
	out.Count = len(out.Messages)
	if err := s.populateChannelMessageReplies(ctx, s.db, viewerUserID, channel, out.Messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, []domain.Channel{channel}, out.Messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	return out, nil
}

func (s *ChannelStore) ReadChannelMessageContents(ctx context.Context, req domain.ReadChannelMessageContentsRequest) (domain.ReadChannelMessageContentsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ReadChannelMessageContentsResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ReadChannelMessageContentsResult{}, fmt.Errorf("read channel message contents: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ReadChannelMessageContentsResult{}, fmt.Errorf("begin read channel message contents: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelMessageContentsResult{}, err
	}
	if len(req.IDs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return domain.ReadChannelMessageContentsResult{}, fmt.Errorf("commit read channel message contents: %w", err)
		}
		committed = true
		return domain.ReadChannelMessageContentsResult{Channel: channel}, nil
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ReadChannelMessageContentsResult{}, domain.ErrChannelInvalid
	}
	id32, _, err := validUniqueChannelMessageIDs(req.IDs)
	if err != nil {
		return domain.ReadChannelMessageContentsResult{}, err
	}
	args := []any{req.ChannelID, id32}
	where := "channel_id = $1 AND id = ANY($2::int[]) AND NOT deleted"
	if member.AvailableMinID > 0 {
		args = append(args, member.AvailableMinID)
		where += fmt.Sprintf(" AND id > $%d", len(args))
	}
	rows, err := tx.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE `+where+`
ORDER BY id DESC`, args...)
	if err != nil {
		return domain.ReadChannelMessageContentsResult{}, fmt.Errorf("read channel messages by ids: %w", err)
	}
	messages := make([]domain.ChannelMessage, 0, len(id32))
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			rows.Close()
			return domain.ReadChannelMessageContentsResult{}, err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.ReadChannelMessageContentsResult{}, err
	}
	rows.Close()
	visibleIDs := make([]int32, 0, len(messages))
	for _, msg := range messages {
		visibleIDs = append(visibleIDs, int32(msg.ID))
	}
	cleared, err := clearChannelUnreadReactionsForMessageIDsTx(ctx, tx, req.UserID, req.ChannelID, visibleIDs)
	if err != nil {
		return domain.ReadChannelMessageContentsResult{}, err
	}
	if err := s.populateChannelMessageReplies(ctx, tx, req.UserID, channel, messages); err != nil {
		return domain.ReadChannelMessageContentsResult{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, tx, req.UserID, []domain.Channel{channel}, messages); err != nil {
		return domain.ReadChannelMessageContentsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ReadChannelMessageContentsResult{}, fmt.Errorf("commit read channel message contents: %w", err)
	}
	committed = true
	return domain.ReadChannelMessageContentsResult{
		Channel:                         channel,
		Messages:                        messages,
		ClearedUnreadReactionMessageIDs: cleared,
	}, nil
}

func (s *ChannelStore) GetChannelMessageViews(ctx context.Context, req domain.ChannelMessageViewsRequest) (domain.ChannelMessageViewsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelMessageViewsResult{}, domain.ErrChannelInvalid
	}
	_, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageViewsResult{}, err
	}
	if len(req.IDs) == 0 {
		return domain.ChannelMessageViewsResult{Views: map[int]int{}}, nil
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ChannelMessageViewsResult{}, domain.ErrChannelInvalid
	}
	id32, _, err := validUniqueChannelMessageIDs(req.IDs)
	if err != nil {
		return domain.ChannelMessageViewsResult{}, err
	}
	if req.Increment {
		date := req.Date
		if date <= 0 {
			date = nowUnix()
		}
		rows, err := s.db.Query(ctx, `
WITH inserted AS (
    INSERT INTO channel_message_viewers (channel_id, message_id, viewer_user_id, viewed_at)
    SELECT m.channel_id, m.id, $3, $4
    FROM channel_messages m
    WHERE m.channel_id = $1
      AND m.id = ANY($2::int[])
      AND NOT m.deleted
      AND m.id > $5
    ON CONFLICT DO NOTHING
    RETURNING message_id
), updated AS (
    UPDATE channel_messages m
    SET views_count = views_count + 1,
        updated_at = now()
    FROM inserted i
    WHERE m.channel_id = $1
      AND m.id = i.message_id
    RETURNING m.id
)
SELECT i.message_id
FROM inserted i
LEFT JOIN updated u ON u.id = i.message_id`, req.ChannelID, id32, req.UserID, date, member.AvailableMinID)
		if err != nil {
			return domain.ChannelMessageViewsResult{}, fmt.Errorf("increment channel message views: %w", err)
		}
		for rows.Next() {
			var ignored int
			if err := rows.Scan(&ignored); err != nil {
				rows.Close()
				return domain.ChannelMessageViewsResult{}, err
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return domain.ChannelMessageViewsResult{}, err
		}
		rows.Close()
	}
	args := []any{req.ChannelID, id32}
	where := "channel_id = $1 AND id = ANY($2::int[]) AND NOT deleted"
	if member.AvailableMinID > 0 {
		args = append(args, member.AvailableMinID)
		where += fmt.Sprintf(" AND id > $%d", len(args))
	}
	rows, err := s.db.Query(ctx, `
SELECT id, views_count
FROM channel_messages
WHERE `+where, args...)
	if err != nil {
		return domain.ChannelMessageViewsResult{}, fmt.Errorf("get channel message views: %w", err)
	}
	defer rows.Close()
	out := make(map[int]int, len(req.IDs))
	for rows.Next() {
		var id int
		var views int
		if err := rows.Scan(&id, &views); err != nil {
			return domain.ChannelMessageViewsResult{}, err
		}
		out[id] = views
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelMessageViewsResult{}, err
	}
	return domain.ChannelMessageViewsResult{Views: out}, nil
}

func (s *ChannelStore) SetChannelMessageReactions(ctx context.Context, req domain.SetChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	if len(req.Reactions) > domain.MaxChannelMessageReactionsPerUser {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	for _, reaction := range req.Reactions {
		if reaction.Type != domain.MessageReactionEmoji || strings.TrimSpace(reaction.Emoticon) == "" || len(reaction.Emoticon) > domain.MaxChannelReactionEmoticonLength {
			return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
		}
	}
	if req.Date <= 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("set channel message reactions: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("begin set channel message reactions: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	msg, err := s.getChannelMessage(ctx, tx, req.ChannelID, req.MessageID)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	if msg.Deleted || msg.Action != nil || msg.ID <= member.AvailableMinID {
		return domain.ChannelMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM channel_message_reactions
WHERE channel_id = $1 AND message_id = $2 AND reacted_user_id = $3`, req.ChannelID, req.MessageID, req.UserID); err != nil {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("delete channel message reactions: %w", err)
	}
	for i, reaction := range req.Reactions {
		if _, err := tx.Exec(ctx, `
INSERT INTO channel_message_reactions (
    channel_id, message_id, reacted_user_id, sender_user_id, reaction_type, reaction_value,
    big, unread, chosen_order, reaction_date
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			req.ChannelID, req.MessageID, req.UserID, msg.SenderUserID, string(reaction.Type), reaction.Emoticon, req.Big, msg.SenderUserID != 0 && msg.SenderUserID != req.UserID, i+1, req.Date); err != nil {
			return domain.ChannelMessageReactionsResult{}, fmt.Errorf("insert channel message reaction: %w", err)
		}
		if req.AddToRecent {
			if _, err := tx.Exec(ctx, `
INSERT INTO user_recent_reactions (user_id, reaction_type, reaction_value, reaction_date)
VALUES ($1,$2,$3,$4)
ON CONFLICT (user_id, reaction_type, reaction_value)
DO UPDATE SET reaction_date = EXCLUDED.reaction_date, updated_at = now()`,
				req.UserID, string(reaction.Type), reaction.Emoticon, req.Date); err != nil {
				return domain.ChannelMessageReactionsResult{}, fmt.Errorf("upsert recent message reaction: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO user_top_reactions (user_id, reaction_type, reaction_value, reaction_count, reaction_date)
VALUES ($1,$2,$3,1,$4)
ON CONFLICT (user_id, reaction_type, reaction_value)
DO UPDATE SET reaction_count = user_top_reactions.reaction_count + 1, reaction_date = EXCLUDED.reaction_date, updated_at = now()`,
			req.UserID, string(reaction.Type), reaction.Emoticon, req.Date); err != nil {
			return domain.ChannelMessageReactionsResult{}, fmt.Errorf("upsert top message reaction: %w", err)
		}
	}
	if err := refreshChannelUnreadReactionsCountTx(ctx, tx, msg.SenderUserID, req.ChannelID); err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("commit set channel message reactions: %w", err)
	}
	committed = true
	messages := []domain.ChannelMessage{msg}
	if err := s.populateChannelMessagesReactions(ctx, s.db, req.UserID, []domain.Channel{channel}, messages); err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	msg = messages[0]
	reactions := emptyChannelMessageReactions(channel)
	if msg.Reactions != nil {
		reactions = *msg.Reactions
	} else {
		msg.Reactions = &reactions
	}
	recipients, err := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, domain.MaxChannelRealtimeFanout)
	if err != nil {
		recipients = []int64{req.UserID}
	}
	return domain.ChannelMessageReactionsResult{
		Channel:    channel,
		Message:    msg,
		Messages:   []domain.ChannelMessage{msg},
		Reactions:  reactions,
		Recipients: recipients,
	}, nil
}

func (s *ChannelStore) DeleteChannelParticipantReaction(ctx context.Context, req domain.DeleteChannelParticipantReactionRequest) (domain.ChannelMessageReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID || req.ParticipantUserID == 0 {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("delete channel participant reaction: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("begin delete channel participant reaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	if !canDeleteAnyChannelMessage(member) {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelAdminRequired
	}
	msg, err := s.getChannelMessage(ctx, tx, req.ChannelID, req.MessageID)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	if msg.Deleted || msg.ID <= member.AvailableMinID {
		return domain.ChannelMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM channel_message_reactions
WHERE channel_id = $1 AND message_id = $2 AND reacted_user_id = $3`,
		req.ChannelID, req.MessageID, req.ParticipantUserID); err != nil {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("delete participant reaction: %w", err)
	}
	if err := refreshChannelUnreadReactionsCountTx(ctx, tx, msg.SenderUserID, req.ChannelID); err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("commit delete participant reaction: %w", err)
	}
	committed = true
	messages := []domain.ChannelMessage{msg}
	if err := s.populateChannelMessagesReactions(ctx, s.db, req.UserID, []domain.Channel{channel}, messages); err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	msg = messages[0]
	reactions := emptyChannelMessageReactions(channel)
	if msg.Reactions != nil {
		reactions = *msg.Reactions
	} else {
		msg.Reactions = &reactions
	}
	recipients, err := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, domain.MaxChannelRealtimeFanout)
	if err != nil {
		recipients = []int64{req.UserID}
	}
	return domain.ChannelMessageReactionsResult{
		Channel:    channel,
		Message:    msg,
		Messages:   []domain.ChannelMessage{msg},
		Reactions:  reactions,
		Recipients: recipients,
	}, nil
}

func (s *ChannelStore) DeleteChannelParticipantReactions(ctx context.Context, req domain.DeleteChannelParticipantReactionsRequest) (domain.DeleteChannelParticipantReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.ParticipantUserID == 0 {
		return domain.DeleteChannelParticipantReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.Limit <= 0 || req.Limit > domain.MaxDeleteParticipantReactionsBatch {
		req.Limit = domain.MaxDeleteParticipantReactionsBatch
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.DeleteChannelParticipantReactionsResult{}, fmt.Errorf("delete channel participant reactions: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.DeleteChannelParticipantReactionsResult{}, fmt.Errorf("begin delete channel participant reactions: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelParticipantReactionsResult{}, err
	}
	if !canDeleteAnyChannelMessage(member) {
		return domain.DeleteChannelParticipantReactionsResult{}, domain.ErrChannelAdminRequired
	}
	rows, err := tx.Query(ctx, `
SELECT message_id, MAX(sender_user_id)
FROM channel_message_reactions
WHERE channel_id = $1 AND reacted_user_id = $2
GROUP BY message_id
ORDER BY MAX(reaction_date) DESC, message_id DESC
LIMIT $3`, req.ChannelID, req.ParticipantUserID, req.Limit)
	if err != nil {
		return domain.DeleteChannelParticipantReactionsResult{}, fmt.Errorf("list participant reaction messages: %w", err)
	}
	ids := make([]int, 0, req.Limit)
	owners := make(map[int64]struct{})
	for rows.Next() {
		var msgID int
		var senderUserID int64
		if err := rows.Scan(&msgID, &senderUserID); err != nil {
			rows.Close()
			return domain.DeleteChannelParticipantReactionsResult{}, err
		}
		ids = append(ids, msgID)
		if senderUserID != 0 {
			owners[senderUserID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.DeleteChannelParticipantReactionsResult{}, err
	}
	rows.Close()
	if len(ids) > 0 {
		if _, err := tx.Exec(ctx, `
DELETE FROM channel_message_reactions
WHERE channel_id = $1 AND reacted_user_id = $2 AND message_id = ANY($3::int[])`,
			req.ChannelID, req.ParticipantUserID, int32s(ids)); err != nil {
			return domain.DeleteChannelParticipantReactionsResult{}, fmt.Errorf("delete participant reactions: %w", err)
		}
		for ownerID := range owners {
			if err := refreshChannelUnreadReactionsCountTx(ctx, tx, ownerID, req.ChannelID); err != nil {
				return domain.DeleteChannelParticipantReactionsResult{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DeleteChannelParticipantReactionsResult{}, fmt.Errorf("commit delete participant reactions: %w", err)
	}
	committed = true
	messages := []domain.ChannelMessage{}
	if len(ids) > 0 {
		res, err := s.GetChannelMessageReactions(ctx, domain.ChannelMessageReactionsRequest{
			UserID:    req.UserID,
			ChannelID: req.ChannelID,
			IDs:       ids,
		})
		if err != nil {
			return domain.DeleteChannelParticipantReactionsResult{}, err
		}
		messages = res.Messages
	}
	recipients, err := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, domain.MaxChannelRealtimeFanout)
	if err != nil {
		recipients = []int64{req.UserID}
	}
	return domain.DeleteChannelParticipantReactionsResult{
		Channel:    channel,
		Messages:   messages,
		Recipients: recipients,
		Deleted:    len(ids),
	}, nil
}

func (s *ChannelStore) GetChannelMessageReactions(ctx context.Context, req domain.ChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	if len(req.IDs) == 0 {
		return domain.ChannelMessageReactionsResult{Channel: channel}, nil
	}
	id32, _, err := validUniqueChannelMessageIDs(req.IDs)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	args := []any{req.ChannelID, id32}
	where := "channel_id = $1 AND id = ANY($2::int[]) AND NOT deleted"
	if member.AvailableMinID > 0 {
		args = append(args, member.AvailableMinID)
		where += fmt.Sprintf(" AND id > $%d", len(args))
	}
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE `+where+`
ORDER BY id DESC`, args...)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, fmt.Errorf("get channel message reactions messages: %w", err)
	}
	defer rows.Close()
	messages := make([]domain.ChannelMessage, 0, len(req.IDs))
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return domain.ChannelMessageReactionsResult{}, err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, s.db, req.UserID, []domain.Channel{channel}, messages); err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	res := domain.ChannelMessageReactionsResult{Channel: channel, Messages: messages}
	if len(messages) == 1 {
		res.Message = messages[0]
		res.Reactions = emptyChannelMessageReactions(channel)
		if messages[0].Reactions != nil {
			res.Reactions = *messages[0].Reactions
		}
	}
	return res, nil
}

func (s *ChannelStore) ListChannelMessageReactions(ctx context.Context, req domain.ChannelMessageReactionsListRequest) (domain.ChannelMessageReactionsList, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.ChannelMessageReactionsList{}, domain.ErrChannelInvalid
	}
	if req.Limit <= 0 || req.Limit > domain.MaxChannelMessageReactionListLimit {
		req.Limit = domain.MaxChannelMessageReactionListLimit
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageReactionsList{}, err
	}
	if channel.Broadcast && !channel.Megagroup {
		return domain.ChannelMessageReactionsList{}, domain.ErrChannelRightForbidden
	}
	msg, err := s.getChannelMessage(ctx, s.db, req.ChannelID, req.MessageID)
	if err != nil {
		return domain.ChannelMessageReactionsList{}, err
	}
	if msg.Deleted || msg.ID <= member.AvailableMinID {
		return domain.ChannelMessageReactionsList{}, domain.ErrMessageIDInvalid
	}
	baseWhere := []string{"channel_id = $1", "message_id = $2"}
	baseArgs := []any{req.ChannelID, req.MessageID}
	if req.Reaction != nil {
		if req.Reaction.Type != domain.MessageReactionEmoji || strings.TrimSpace(req.Reaction.Emoticon) == "" {
			return domain.ChannelMessageReactionsList{}, domain.ErrChannelInvalid
		}
		baseArgs = append(baseArgs, string(req.Reaction.Type), req.Reaction.Emoticon)
		baseWhere = append(baseWhere, fmt.Sprintf("reaction_type = $%d AND reaction_value = $%d", len(baseArgs)-1, len(baseArgs)))
	}
	var count int
	if err := s.db.QueryRow(ctx, `SELECT COUNT(*)::int FROM channel_message_reactions WHERE `+strings.Join(baseWhere, " AND "), baseArgs...).Scan(&count); err != nil {
		return domain.ChannelMessageReactionsList{}, fmt.Errorf("count channel message reactions: %w", err)
	}
	where := append([]string(nil), baseWhere...)
	args := append([]any(nil), baseArgs...)
	if req.Offset != "" {
		cursor, ok := parseChannelReactionOffset(req.Offset)
		if !ok {
			return domain.ChannelMessageReactionsList{}, domain.ErrChannelInvalid
		}
		args = append(args, cursor.date, cursor.userID, cursor.emoticon)
		n := len(args)
		where = append(where, fmt.Sprintf("(reaction_date < $%d OR (reaction_date = $%d AND (reacted_user_id < $%d OR (reacted_user_id = $%d AND reaction_value > $%d))))", n-2, n-2, n-1, n-1, n))
	}
	args = append(args, req.Limit+1)
	rows, err := s.db.Query(ctx, `
SELECT channel_id, message_id, reacted_user_id, sender_user_id, reaction_type, reaction_value,
       big, unread, chosen_order, reaction_date
FROM channel_message_reactions
WHERE `+strings.Join(where, " AND ")+`
ORDER BY reaction_date DESC, reacted_user_id DESC, reaction_value ASC
LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return domain.ChannelMessageReactionsList{}, fmt.Errorf("list channel message reactions: %w", err)
	}
	defer rows.Close()
	reactions := make([]domain.ChannelMessagePeerReaction, 0, req.Limit+1)
	for rows.Next() {
		row, err := scanChannelMessagePeerReaction(rows, req.UserID)
		if err != nil {
			return domain.ChannelMessageReactionsList{}, err
		}
		reactions = append(reactions, row)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelMessageReactionsList{}, err
	}
	next := ""
	if len(reactions) > req.Limit {
		reactions = reactions[:req.Limit]
		next = channelReactionOffset(reactions[len(reactions)-1])
	}
	return domain.ChannelMessageReactionsList{
		Channel:    channel,
		Message:    msg,
		Count:      count,
		Reactions:  reactions,
		NextOffset: next,
	}, nil
}

func (s *ChannelStore) ListTopMessageReactions(ctx context.Context, userID int64, limit int) ([]domain.MessageReaction, error) {
	if userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.MessageReaction{}, nil
	}
	if limit > domain.MaxTopMessageReactions {
		limit = domain.MaxTopMessageReactions
	}
	rows, err := s.db.Query(ctx, `
SELECT reaction_type, reaction_value
FROM user_top_reactions
WHERE user_id = $1
ORDER BY reaction_count DESC, reaction_date DESC, updated_at DESC, reaction_value ASC
LIMIT $2`, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("list top message reactions: %w", err)
	}
	defer rows.Close()
	out := make([]domain.MessageReaction, 0, limit)
	for rows.Next() {
		var reactionType, reactionValue string
		if err := rows.Scan(&reactionType, &reactionValue); err != nil {
			return nil, err
		}
		out = append(out, domain.MessageReaction{
			Type:     domain.MessageReactionType(reactionType),
			Emoticon: reactionValue,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ChannelStore) ListRecentMessageReactions(ctx context.Context, userID int64, limit int) ([]domain.MessageReaction, error) {
	if userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.MessageReaction{}, nil
	}
	if limit > domain.MaxRecentMessageReactions {
		limit = domain.MaxRecentMessageReactions
	}
	rows, err := s.db.Query(ctx, `
SELECT reaction_type, reaction_value
FROM user_recent_reactions
WHERE user_id = $1
ORDER BY reaction_date DESC, updated_at DESC, reaction_value ASC
LIMIT $2`, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent message reactions: %w", err)
	}
	defer rows.Close()
	out := make([]domain.MessageReaction, 0, limit)
	for rows.Next() {
		var reactionType, reactionValue string
		if err := rows.Scan(&reactionType, &reactionValue); err != nil {
			return nil, err
		}
		out = append(out, domain.MessageReaction{
			Type:     domain.MessageReactionType(reactionType),
			Emoticon: reactionValue,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ChannelStore) RecordMessageReactionUse(ctx context.Context, userID int64, reactions []domain.MessageReaction, addToRecent bool, date int) error {
	if userID == 0 || len(reactions) == 0 {
		return nil
	}
	if date <= 0 {
		date = nowUnix()
	}
	for _, reaction := range reactions {
		if reaction.Type != domain.MessageReactionEmoji || strings.TrimSpace(reaction.Emoticon) == "" || len(reaction.Emoticon) > domain.MaxChannelReactionEmoticonLength {
			continue
		}
		if addToRecent {
			if _, err := s.db.Exec(ctx, `
INSERT INTO user_recent_reactions (user_id, reaction_type, reaction_value, reaction_date)
VALUES ($1,$2,$3,$4)
ON CONFLICT (user_id, reaction_type, reaction_value)
DO UPDATE SET reaction_date = EXCLUDED.reaction_date, updated_at = now()`,
				userID, string(reaction.Type), reaction.Emoticon, date); err != nil {
				return fmt.Errorf("record recent message reaction: %w", err)
			}
		}
		if _, err := s.db.Exec(ctx, `
INSERT INTO user_top_reactions (user_id, reaction_type, reaction_value, reaction_count, reaction_date)
VALUES ($1,$2,$3,1,$4)
ON CONFLICT (user_id, reaction_type, reaction_value)
DO UPDATE SET reaction_count = user_top_reactions.reaction_count + 1, reaction_date = EXCLUDED.reaction_date, updated_at = now()`,
			userID, string(reaction.Type), reaction.Emoticon, date); err != nil {
			return fmt.Errorf("record top message reaction: %w", err)
		}
	}
	return nil
}

func (s *ChannelStore) ClearRecentMessageReactions(ctx context.Context, userID int64) error {
	if userID == 0 {
		return domain.ErrChannelInvalid
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM user_recent_reactions WHERE user_id = $1`, userID); err != nil {
		return fmt.Errorf("clear recent message reactions: %w", err)
	}
	return nil
}

func (s *ChannelStore) ListSavedReactionTags(ctx context.Context, userID int64, limit int) ([]domain.SavedReactionTag, error) {
	if userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.SavedReactionTag{}, nil
	}
	if limit > domain.MaxSavedReactionTags {
		limit = domain.MaxSavedReactionTags
	}
	rows, err := s.db.Query(ctx, `
SELECT reaction_type, reaction_value, title, reaction_count
FROM user_saved_reaction_tags
WHERE user_id = $1
ORDER BY reaction_count DESC, updated_at DESC, reaction_value ASC
LIMIT $2`, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("list saved reaction tags: %w", err)
	}
	defer rows.Close()
	out := make([]domain.SavedReactionTag, 0, limit)
	for rows.Next() {
		var reactionType, reactionValue, title string
		var count int
		if err := rows.Scan(&reactionType, &reactionValue, &title, &count); err != nil {
			return nil, err
		}
		out = append(out, domain.SavedReactionTag{
			UserID: userID,
			Reaction: domain.MessageReaction{
				Type:     domain.MessageReactionType(reactionType),
				Emoticon: reactionValue,
			},
			Title: title,
			Count: count,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ChannelStore) UpsertSavedReactionTag(ctx context.Context, tag domain.SavedReactionTag) error {
	if tag.UserID == 0 || tag.Reaction.Type != domain.MessageReactionEmoji {
		return domain.ErrChannelInvalid
	}
	reactionValue := strings.TrimSpace(tag.Reaction.Emoticon)
	if reactionValue == "" {
		return domain.ErrChannelInvalid
	}
	if tag.Count < 0 {
		tag.Count = 0
	}
	if _, err := s.db.Exec(ctx, `
INSERT INTO user_saved_reaction_tags (user_id, reaction_type, reaction_value, title, reaction_count)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (user_id, reaction_type, reaction_value) DO UPDATE SET
    title = EXCLUDED.title,
    reaction_count = GREATEST(user_saved_reaction_tags.reaction_count, EXCLUDED.reaction_count),
    updated_at = now()`, tag.UserID, string(tag.Reaction.Type), reactionValue, tag.Title, tag.Count); err != nil {
		return fmt.Errorf("upsert saved reaction tag: %w", err)
	}
	return nil
}

func (s *ChannelStore) CreateForumTopic(ctx context.Context, req domain.CreateChannelForumTopicRequest) (domain.CreateChannelForumTopicResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.RandomID == 0 {
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	title := strings.TrimSpace(req.Title)
	if title == "" && !req.TitleMissing {
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.CreateChannelForumTopicResult{}, err
	}
	if !channel.Forum || channel.Broadcast || !channel.Megagroup {
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelForumMissing
	}
	if !canSendChannelMessage(channel, member) {
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelWriteForbidden
	}
	if req.IconColor == 0 {
		req.IconColor = domain.DefaultForumTopicIconColor
	}
	res, err := s.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    req.UserID,
		ChannelID: req.ChannelID,
		RandomID:  req.RandomID,
		SendAs:    req.SendAs,
		Action: &domain.ChannelMessageAction{
			Type:         domain.ChannelActionTopicCreate,
			Title:        title,
			IconColor:    req.IconColor,
			IconEmojiID:  req.IconEmojiID,
			TitleMissing: req.TitleMissing,
		},
		Date: req.Date,
	})
	if err != nil {
		return domain.CreateChannelForumTopicResult{}, err
	}
	if res.Message.Action == nil || res.Message.Action.Type != domain.ChannelActionTopicCreate {
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	if _, err := s.db.Exec(ctx, `
INSERT INTO channel_forum_topics (
    channel_id, topic_id, creator_user_id, title, icon_color, icon_emoji_id,
    title_missing, date, top_message_id, read_inbox_max_id, read_outbox_max_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $2, $2, $2)
ON CONFLICT (channel_id, topic_id) DO NOTHING`,
		req.ChannelID, res.Message.ID, req.UserID, title, req.IconColor, req.IconEmojiID, req.TitleMissing, res.Message.Date); err != nil {
		return domain.CreateChannelForumTopicResult{}, fmt.Errorf("insert forum topic: %w", err)
	}
	topic, err := s.getForumTopic(ctx, s.db, req.ChannelID, res.Message.ID)
	if err != nil {
		return domain.CreateChannelForumTopicResult{}, err
	}
	return domain.CreateChannelForumTopicResult{
		Channel:    res.Channel,
		Topic:      topic,
		Message:    res.Message,
		Event:      res.Event,
		Recipients: res.Recipients,
		Duplicate:  res.Duplicate,
	}, nil
}

func (s *ChannelStore) EditForumTopic(ctx context.Context, req domain.EditChannelForumTopicRequest) (domain.EditChannelForumTopicResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TopicID <= 0 {
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelForumTopicResult{}, err
	}
	if !channel.Forum {
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelForumMissing
	}
	topic, err := s.getForumTopic(ctx, s.db, req.ChannelID, req.TopicID)
	if err != nil {
		return domain.EditChannelForumTopicResult{}, err
	}
	if !canManageForumTopic(channel, member, topic, req.UserID) {
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelAdminRequired
	}
	next := topic
	action := domain.ChannelMessageAction{Type: domain.ChannelActionTopicEdit}
	changed := false
	if req.Title != nil {
		title := strings.TrimSpace(*req.Title)
		if title == "" {
			return domain.EditChannelForumTopicResult{}, domain.ErrChannelInvalid
		}
		if next.Title != title {
			next.Title = title
			action.Title = title
			changed = true
		}
	}
	if req.IconEmojiID != nil && next.IconEmojiID != *req.IconEmojiID {
		next.IconEmojiID = *req.IconEmojiID
		action.IconEmojiID = *req.IconEmojiID
		action.IconEmojiIDSet = true
		changed = true
	}
	if req.Closed != nil && next.Closed != *req.Closed {
		next.Closed = *req.Closed
		action.Closed = boolPtr(*req.Closed)
		changed = true
	}
	if req.Hidden != nil && next.Hidden != *req.Hidden {
		next.Hidden = *req.Hidden
		action.Hidden = boolPtr(*req.Hidden)
		changed = true
	}
	if !changed {
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelNotModified
	}
	res, err := s.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    req.UserID,
		ChannelID: req.ChannelID,
		ReplyTo: &domain.MessageReply{
			Peer:         domain.Peer{Type: domain.PeerTypeChannel, ID: req.ChannelID},
			MessageID:    req.TopicID,
			TopMessageID: req.TopicID,
		},
		Action: &action,
		Date:   req.Date,
	})
	if err != nil {
		return domain.EditChannelForumTopicResult{}, err
	}
	if _, err := s.db.Exec(ctx, `
UPDATE channel_forum_topics
SET title = $3,
    icon_emoji_id = $4,
    closed = $5,
    hidden = $6,
    top_message_id = GREATEST(top_message_id, $7),
    updated_at = now()
WHERE channel_id = $1 AND topic_id = $2 AND NOT deleted`,
		req.ChannelID, req.TopicID, next.Title, next.IconEmojiID, next.Closed, next.Hidden, res.Message.ID); err != nil {
		return domain.EditChannelForumTopicResult{}, fmt.Errorf("update forum topic: %w", err)
	}
	topic, err = s.getForumTopic(ctx, s.db, req.ChannelID, req.TopicID)
	if err != nil {
		return domain.EditChannelForumTopicResult{}, err
	}
	return domain.EditChannelForumTopicResult{
		Channel:    res.Channel,
		Topic:      topic,
		Message:    res.Message,
		Event:      res.Event,
		Recipients: res.Recipients,
	}, nil
}

func (s *ChannelStore) UpdatePinnedForumTopic(ctx context.Context, req domain.UpdateChannelForumTopicPinnedRequest) (domain.UpdateChannelForumTopicPinnedResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TopicID <= 0 {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.UpdateChannelForumTopicPinnedResult{}, err
	}
	if !channel.Forum {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelForumMissing
	}
	topic, err := s.getForumTopic(ctx, s.db, req.ChannelID, req.TopicID)
	if err != nil {
		return domain.UpdateChannelForumTopicPinnedResult{}, err
	}
	if !canPinChannelMessages(channel, member) {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelAdminRequired
	}
	if topic.Pinned == req.Pinned {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelNotModified
	}
	pinnedOrder := 0
	if req.Pinned {
		pinnedOrder = topic.PinnedOrder
		if pinnedOrder == 0 {
			pinnedOrder, err = s.nextForumTopicPinnedOrder(ctx, req.ChannelID)
			if err != nil {
				return domain.UpdateChannelForumTopicPinnedResult{}, err
			}
		}
	}
	if _, err := s.db.Exec(ctx, `
UPDATE channel_forum_topics
SET pinned = $3, pinned_order = $4, updated_at = now()
WHERE channel_id = $1 AND topic_id = $2 AND NOT deleted`,
		req.ChannelID, req.TopicID, req.Pinned, pinnedOrder); err != nil {
		return domain.UpdateChannelForumTopicPinnedResult{}, fmt.Errorf("update pinned forum topic: %w", err)
	}
	topic, err = s.getForumTopic(ctx, s.db, req.ChannelID, req.TopicID)
	if err != nil {
		return domain.UpdateChannelForumTopicPinnedResult{}, err
	}
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	return domain.UpdateChannelForumTopicPinnedResult{Channel: channel, Topic: topic, Recipients: recipients}, nil
}

func (s *ChannelStore) ReorderPinnedForumTopics(ctx context.Context, req domain.ReorderChannelPinnedForumTopicsRequest) (domain.ReorderChannelPinnedForumTopicsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || len(req.Order) > domain.MaxChannelForumTopicIDs {
		return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReorderChannelPinnedForumTopicsResult{}, err
	}
	if !channel.Forum {
		return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrChannelForumMissing
	}
	if !canPinChannelMessages(channel, member) {
		return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrChannelAdminRequired
	}
	seen := make(map[int]struct{}, len(req.Order))
	order := make([]int, 0, len(req.Order))
	for _, id := range req.Order {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrMessageIDInvalid
		}
		if _, ok := seen[id]; ok {
			continue
		}
		topic, err := s.getForumTopic(ctx, s.db, req.ChannelID, id)
		if err != nil || !topic.Pinned {
			if req.Force {
				continue
			}
			return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrMessageIDInvalid
		}
		seen[id] = struct{}{}
		order = append(order, id)
	}
	for i, id := range order {
		if _, err := s.db.Exec(ctx, `
UPDATE channel_forum_topics
SET pinned_order = $3, updated_at = now()
WHERE channel_id = $1 AND topic_id = $2 AND pinned AND NOT deleted`, req.ChannelID, id, len(order)-i); err != nil {
			return domain.ReorderChannelPinnedForumTopicsResult{}, fmt.Errorf("reorder pinned forum topics: %w", err)
		}
	}
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	return domain.ReorderChannelPinnedForumTopicsResult{Channel: channel, Order: order, Recipients: recipients}, nil
}

func (s *ChannelStore) DeleteForumTopicHistory(ctx context.Context, req domain.DeleteChannelForumTopicHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TopicID <= 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("delete forum topic history: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("begin delete forum topic history: %w", err)
	}
	committed := false
	var reserved []reservedChannelPts
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			s.recordChannelPtsGaps(ctx, reserved, req.Date)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	if !channel.Forum {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelForumMissing
	}
	topic, err := s.getForumTopic(ctx, tx, req.ChannelID, req.TopicID)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	if !canManageForumTopic(channel, member, topic, req.UserID) && !canDeleteAnyChannelMessage(member) {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelAdminRequired
	}
	rows, err := tx.Query(ctx, `
SELECT id
FROM channel_messages
WHERE channel_id = $1 AND NOT deleted AND (id = $2 OR reply_to_top_id = $2)
ORDER BY id DESC
LIMIT $3`, req.ChannelID, req.TopicID, domain.MaxDeleteHistoryBatch)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("list forum topic delete ids: %w", err)
	}
	ids := make([]int, 0, domain.MaxDeleteHistoryBatch)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return domain.DeleteChannelHistoryResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.DeleteChannelHistoryResult{}, err
	}
	rows.Close()
	deleted, event, channel, err := s.deleteChannelMessagesTx(ctx, tx, channel, member, ids, req.UserID, req.Date, &reserved)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	remaining := 0
	if err := tx.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_messages
WHERE channel_id = $1 AND NOT deleted AND (id = $2 OR reply_to_top_id = $2)`, req.ChannelID, req.TopicID).Scan(&remaining); err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("count remaining forum topic messages: %w", err)
	}
	offset := 0
	if remaining > 0 {
		offset = 1
	} else if _, err := tx.Exec(ctx, `
UPDATE channel_forum_topics
SET deleted = true, updated_at = now()
WHERE channel_id = $1 AND topic_id = $2`, req.ChannelID, req.TopicID); err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("mark forum topic deleted: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DeleteChannelHistoryResult{}, fmt.Errorf("commit delete forum topic history: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	return domain.DeleteChannelHistoryResult{Channel: channel, Event: event, DeletedIDs: deleted, Recipients: recipients, Offset: offset}, nil
}

func (s *ChannelStore) ListForumTopics(ctx context.Context, viewerUserID int64, filter domain.ChannelForumTopicFilter) (domain.ChannelForumTopicList, error) {
	channel, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelForumTopicList{}, err
	}
	if !channel.Forum {
		return domain.ChannelForumTopicList{}, domain.ErrChannelForumMissing
	}
	limit := filter.Limit
	if limit <= 0 || limit > domain.MaxChannelForumTopicsLimit {
		limit = domain.MaxChannelForumTopicsLimit
	}
	query := strings.TrimSpace(strings.ToLower(filter.Query))
	countArgs := []any{filter.ChannelID, member.AvailableMinID, query}
	countSQL := `
SELECT COUNT(*)::int
FROM channel_forum_topics
WHERE channel_id = $1 AND NOT deleted AND topic_id > $2
  AND ($3 = '' OR POSITION($3 IN LOWER(title)) > 0)`
	var total int
	if err := s.db.QueryRow(ctx, countSQL, countArgs...).Scan(&total); err != nil {
		return domain.ChannelForumTopicList{}, fmt.Errorf("count forum topics: %w", err)
	}
	args := []any{filter.ChannelID, member.AvailableMinID, query}
	where := `channel_id = $1 AND NOT deleted AND topic_id > $2 AND ($3 = '' OR POSITION($3 IN LOWER(title)) > 0)`
	offsetID := filter.OffsetTopic
	if offsetID == 0 {
		offsetID = filter.OffsetID
	}
	if filter.OffsetDate != 0 {
		args = append(args, filter.OffsetDate, offsetID)
		where += fmt.Sprintf(" AND (date, topic_id) < ($%d, $%d)", len(args)-1, len(args))
	} else if offsetID != 0 {
		args = append(args, offsetID)
		where += fmt.Sprintf(" AND topic_id < $%d", len(args))
	}
	args = append(args, limit)
	rows, err := s.db.Query(ctx, `
SELECT `+channelForumTopicColumns+`
FROM channel_forum_topics
WHERE `+where+`
ORDER BY pinned DESC, pinned_order DESC, date DESC, topic_id DESC
LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return domain.ChannelForumTopicList{}, fmt.Errorf("list forum topics: %w", err)
	}
	defer rows.Close()
	topics := make([]domain.ChannelForumTopic, 0, limit)
	for rows.Next() {
		topic, err := scanChannelForumTopic(rows)
		if err != nil {
			return domain.ChannelForumTopicList{}, err
		}
		topics = append(topics, s.topicWithViewerCounters(ctx, viewerUserID, filter.ChannelID, topic, member.ReadInboxMaxID, member.AvailableMinID))
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelForumTopicList{}, err
	}
	messages, err := s.forumTopicRootMessages(ctx, filter.ChannelID, topics, member.AvailableMinID)
	if err != nil {
		return domain.ChannelForumTopicList{}, err
	}
	dialog, _ := s.getChannelDialog(ctx, s.db, viewerUserID, channel)
	return domain.ChannelForumTopicList{Channel: channel, Dialog: dialog, Topics: topics, Messages: messages, Count: total}, nil
}

func (s *ChannelStore) GetForumTopicsByID(ctx context.Context, viewerUserID, channelID int64, ids []int) (domain.ChannelForumTopicList, error) {
	channel, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID)
	if err != nil {
		return domain.ChannelForumTopicList{}, err
	}
	if !channel.Forum {
		return domain.ChannelForumTopicList{}, domain.ErrChannelForumMissing
	}
	if len(ids) == 0 {
		dialog, _ := s.getChannelDialog(ctx, s.db, viewerUserID, channel)
		return domain.ChannelForumTopicList{Channel: channel, Dialog: dialog}, nil
	}
	id32, _, err := validUniqueChannelMessageIDs(ids)
	if err != nil {
		return domain.ChannelForumTopicList{}, err
	}
	rows, err := s.db.Query(ctx, `
SELECT `+channelForumTopicColumns+`
FROM channel_forum_topics
WHERE channel_id = $1 AND NOT deleted AND topic_id > $2 AND topic_id = ANY($3::int[])
ORDER BY pinned DESC, pinned_order DESC, date DESC, topic_id DESC`, channelID, member.AvailableMinID, id32)
	if err != nil {
		return domain.ChannelForumTopicList{}, fmt.Errorf("get forum topics by id: %w", err)
	}
	defer rows.Close()
	topics := make([]domain.ChannelForumTopic, 0, len(id32))
	for rows.Next() {
		topic, err := scanChannelForumTopic(rows)
		if err != nil {
			return domain.ChannelForumTopicList{}, err
		}
		topics = append(topics, s.topicWithViewerCounters(ctx, viewerUserID, channelID, topic, member.ReadInboxMaxID, member.AvailableMinID))
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelForumTopicList{}, err
	}
	messages, err := s.forumTopicRootMessages(ctx, channelID, topics, member.AvailableMinID)
	if err != nil {
		return domain.ChannelForumTopicList{}, err
	}
	dialog, _ := s.getChannelDialog(ctx, s.db, viewerUserID, channel)
	return domain.ChannelForumTopicList{Channel: channel, Dialog: dialog, Topics: topics, Messages: messages, Count: len(topics)}, nil
}

func (s *ChannelStore) ListChannelReplies(ctx context.Context, viewerUserID int64, filter domain.ChannelRepliesFilter) (domain.ChannelHistory, error) {
	source, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	root, err := s.getChannelMessage(ctx, s.db, filter.ChannelID, filter.RootMessageID)
	if err != nil || root.Deleted || root.ID <= member.AvailableMinID {
		return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
	}
	target := source
	availableMinID := member.AvailableMinID
	extraChannels := []domain.Channel(nil)
	rootID := root.ID
	if source.Broadcast {
		if root.Discussion == nil || root.Discussion.ChannelID == 0 || root.Discussion.MessageID == 0 {
			return domain.ChannelHistory{Channel: source}, nil
		}
		linked, err := getChannelByID(ctx, s.db, root.Discussion.ChannelID)
		if err != nil {
			return domain.ChannelHistory{Channel: source}, nil
		}
		target = linked
		rootID = root.Discussion.MessageID
		availableMinID = 0
		if linkedMember, err := s.getChannelMember(ctx, s.db, linked.ID, viewerUserID); err == nil && validateChannelMemberVisible(linkedMember) == nil {
			availableMinID = linkedMember.AvailableMinID
		}
		extraChannels = append(extraChannels, source)
	}
	targetRoot, err := s.getChannelMessage(ctx, s.db, target.ID, rootID)
	if err != nil || targetRoot.Deleted || targetRoot.ID <= availableMinID {
		return domain.ChannelHistory{Channel: target, Channels: extraChannels}, nil
	}
	limit := filter.Limit
	if limit <= 0 || limit > domain.MaxChannelRepliesLimit {
		limit = domain.MaxChannelRepliesLimit
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	count, err := s.countChannelReplies(ctx, target.ID, rootID, availableMinID, filter)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	messages, err := s.queryChannelRepliesPage(ctx, target.ID, rootID, availableMinID, filter, limit)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessageReplies(ctx, s.db, viewerUserID, target, messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, []domain.Channel{target}, messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	topics := []domain.ChannelForumTopic(nil)
	if target.Forum {
		if topic, err := s.getForumTopic(ctx, s.db, target.ID, rootID); err == nil && !topic.Hidden {
			topic = s.topicWithViewerCounters(ctx, viewerUserID, target.ID, topic, availableMinID, availableMinID)
			topics = append(topics, topic)
		} else if err != nil && !errors.Is(err, domain.ErrMessageIDInvalid) {
			return domain.ChannelHistory{}, err
		}
	}
	return domain.ChannelHistory{Channel: target, Channels: extraChannels, Topics: topics, Messages: messages, Count: count}, nil
}

func (s *ChannelStore) ListChannelUnreadMentions(ctx context.Context, viewerUserID int64, filter domain.ChannelUnreadMentionsFilter) (domain.ChannelHistory, error) {
	channel, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > domain.MaxChannelUnreadMentionsLimit {
		limit = domain.MaxChannelUnreadMentionsLimit
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	count, err := s.countChannelUnreadMentions(ctx, viewerUserID, filter, member.AvailableMinID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	messages, err := s.queryChannelUnreadMentionsPage(ctx, viewerUserID, filter, member.AvailableMinID, limit)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessageReplies(ctx, s.db, viewerUserID, channel, messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, []domain.Channel{channel}, messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	return domain.ChannelHistory{Channel: channel, Messages: messages, Count: count}, nil
}

func (s *ChannelStore) ReadChannelMentions(ctx context.Context, req domain.ReadChannelMentionsRequest) (domain.ReadChannelMentionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ReadChannelMentionsResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ReadChannelMentionsResult{}, fmt.Errorf("read channel mentions: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ReadChannelMentionsResult{}, fmt.Errorf("begin read channel mentions: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, _, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelMentionsResult{}, err
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelReadMentionsBatch {
		limit = domain.MaxChannelReadMentionsBatch
	}
	cleared, remaining, err := readChannelMentionsTx(ctx, tx, req.UserID, req.ChannelID, req.TopMsgID, limit)
	if err != nil {
		return domain.ReadChannelMentionsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ReadChannelMentionsResult{}, fmt.Errorf("commit read channel mentions: %w", err)
	}
	committed = true
	offset := 0
	if remaining > 0 {
		offset = 1
	}
	return domain.ReadChannelMentionsResult{
		Channel:    channel,
		Cleared:    cleared,
		Remaining:  remaining,
		Offset:     offset,
		ChannelPts: channel.Pts,
	}, nil
}

func (s *ChannelStore) ListChannelUnreadReactions(ctx context.Context, viewerUserID int64, filter domain.ChannelUnreadReactionsFilter) (domain.ChannelHistory, error) {
	channel, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > domain.MaxChannelUnreadReactionsLimit {
		limit = domain.MaxChannelUnreadReactionsLimit
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	count, err := s.countChannelUnreadReactions(ctx, viewerUserID, filter, member.AvailableMinID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	messages, err := s.queryChannelUnreadReactionsPage(ctx, viewerUserID, filter, member.AvailableMinID, limit)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessageReplies(ctx, s.db, viewerUserID, channel, messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, []domain.Channel{channel}, messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	return domain.ChannelHistory{Channel: channel, Messages: messages, Count: count}, nil
}

func (s *ChannelStore) ReadChannelReactions(ctx context.Context, req domain.ReadChannelReactionsRequest) (domain.ReadChannelReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ReadChannelReactionsResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ReadChannelReactionsResult{}, fmt.Errorf("read channel reactions: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ReadChannelReactionsResult{}, fmt.Errorf("begin read channel reactions: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, _, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelReactionsResult{}, err
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelReadReactionsBatch {
		limit = domain.MaxChannelReadReactionsBatch
	}
	cleared, remaining, err := readChannelReactionsTx(ctx, tx, req.UserID, req.ChannelID, req.TopMsgID, limit)
	if err != nil {
		return domain.ReadChannelReactionsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ReadChannelReactionsResult{}, fmt.Errorf("commit read channel reactions: %w", err)
	}
	committed = true
	offset := 0
	if remaining > 0 {
		offset = 1
	}
	return domain.ReadChannelReactionsResult{
		Channel:    channel,
		Cleared:    cleared,
		Remaining:  remaining,
		Offset:     offset,
		ChannelPts: channel.Pts,
	}, nil
}

func (s *ChannelStore) GetDiscussionMessage(ctx context.Context, viewerUserID, channelID int64, msgID int) (domain.ChannelDiscussionMessage, error) {
	source, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID)
	if err != nil {
		return domain.ChannelDiscussionMessage{}, err
	}
	msg, err := s.getChannelMessage(ctx, s.db, channelID, msgID)
	if err != nil || msg.Deleted || msg.ID <= member.AvailableMinID {
		return domain.ChannelDiscussionMessage{}, domain.ErrMessageIDInvalid
	}
	result := domain.ChannelDiscussionMessage{PostChannel: source, DiscussionChannel: source, Channels: []domain.Channel{source}}
	target := source
	targetMsg := msg
	if source.Broadcast {
		if msg.Discussion == nil || msg.Discussion.ChannelID == 0 || msg.Discussion.MessageID == 0 {
			return result, nil
		}
		linked, err := getChannelByID(ctx, s.db, msg.Discussion.ChannelID)
		if err != nil {
			return result, nil
		}
		linkedMsg, err := s.getChannelMessage(ctx, s.db, linked.ID, msg.Discussion.MessageID)
		if err != nil || linkedMsg.Deleted {
			return result, nil
		}
		target = linked
		targetMsg = linkedMsg
		result.DiscussionChannel = linked
		result.Channels = []domain.Channel{source, linked}
	}
	messages := []domain.ChannelMessage{targetMsg}
	if err := s.populateChannelMessageReplies(ctx, s.db, viewerUserID, target, messages); err != nil {
		return domain.ChannelDiscussionMessage{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, []domain.Channel{target}, messages); err != nil {
		return domain.ChannelDiscussionMessage{}, err
	}
	readInbox, readOutbox := s.channelReadWatermarks(ctx, target.ID, viewerUserID)
	result.Messages = messages
	result.ReadInboxMaxID = readInbox
	result.ReadOutboxMaxID = readOutbox
	if messages[0].Replies != nil {
		result.MaxID = messages[0].Replies.MaxID
	}
	result.UnreadCount = s.channelThreadUnreadCount(ctx, target.ID, targetMsg.ID, viewerUserID, readInbox)
	return result, nil
}

func (s *ChannelStore) ReadChannelHistory(ctx context.Context, req domain.ReadChannelHistoryRequest) (domain.ReadChannelHistoryResult, error) {
	var lastErr error
	for attempt := 0; attempt < retryableChannelTxAttempts; attempt++ {
		res, err := s.readChannelHistoryOnce(ctx, req)
		if err == nil || !isRetryablePostgresTxError(err) || ctx.Err() != nil {
			return res, err
		}
		lastErr = err
	}
	return domain.ReadChannelHistoryResult{}, lastErr
}

func (s *ChannelStore) readChannelHistoryOnce(ctx context.Context, req domain.ReadChannelHistoryRequest) (domain.ReadChannelHistoryResult, error) {
	channel, _, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelHistoryResult{}, err
	}
	maxID := req.MaxID
	if maxID <= 0 || maxID > channel.TopMessageID {
		maxID = channel.TopMessageID
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ReadChannelHistoryResult{}, fmt.Errorf("read channel history: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ReadChannelHistoryResult{}, fmt.Errorf("begin read channel history: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	var previous int
	if err := tx.QueryRow(ctx, `SELECT read_inbox_max_id FROM channel_members WHERE channel_id = $1 AND user_id = $2`, req.ChannelID, req.UserID).Scan(&previous); err != nil {
		return domain.ReadChannelHistoryResult{}, fmt.Errorf("read channel member state: %w", err)
	}
	changed := maxID > previous
	var outboxUpdates []domain.ChannelReadOutboxUpdate
	if _, err := tx.Exec(ctx, `
UPDATE channel_members
SET read_inbox_date = CASE WHEN read_inbox_max_id < $3 THEN $4 ELSE read_inbox_date END,
    read_inbox_max_id = GREATEST(read_inbox_max_id, $3),
    unread_mark = false,
    updated_at = now()
WHERE channel_id = $1 AND user_id = $2`, req.ChannelID, req.UserID, maxID, req.Date); err != nil {
		return domain.ReadChannelHistoryResult{}, fmt.Errorf("update channel member read: %w", err)
	}
	msg, _ := s.getChannelMessage(ctx, tx, req.ChannelID, channel.TopMessageID)
	if changed {
		outboxUpdates, err = advanceChannelReadOutboxTx(ctx, tx, channel, msg, req.UserID, previous, maxID)
		if err != nil {
			return domain.ReadChannelHistoryResult{}, err
		}
	}
	if err := upsertChannelDialogTx(ctx, tx, req.UserID, channel, msg, maxID, 0); err != nil {
		return domain.ReadChannelHistoryResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ReadChannelHistoryResult{}, fmt.Errorf("commit read channel history: %w", err)
	}
	committed = true
	dialog, err := s.getChannelDialog(ctx, s.db, req.UserID, channel)
	if err != nil {
		return domain.ReadChannelHistoryResult{}, err
	}
	return domain.ReadChannelHistoryResult{
		ChannelID:        req.ChannelID,
		MaxID:            maxID,
		StillUnreadCount: dialog.UnreadCount,
		Changed:          changed,
		Pts:              channel.Pts,
		Dialog:           dialog,
		OutboxUpdates:    outboxUpdates,
	}, nil
}

func advanceChannelReadOutboxTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, top domain.ChannelMessage, readerUserID int64, previous, maxID int) ([]domain.ChannelReadOutboxUpdate, error) {
	if maxID <= previous {
		return nil, nil
	}
	lowerID := previous
	if maxID-lowerID > domain.MaxChannelReadOutboxScanMessages {
		lowerID = maxID - domain.MaxChannelReadOutboxScanMessages
	}
	rows, err := tx.Query(ctx, `
WITH latest_sender_messages AS (
    SELECT sender_user_id, MAX(id) AS max_id
    FROM channel_messages
    WHERE channel_id = $1
      AND id > $2
      AND id <= $3
      AND NOT deleted
      AND sender_user_id <> $4
    GROUP BY sender_user_id
    ORDER BY max_id DESC
    LIMIT $5
)
SELECT sender_user_id, max_id
FROM latest_sender_messages
ORDER BY sender_user_id ASC`, channel.ID, lowerID, maxID, readerUserID, domain.MaxChannelReadOutboxFanout)
	if err != nil {
		return nil, fmt.Errorf("list channel read outbox senders: %w", err)
	}
	defer rows.Close()
	type candidate struct {
		userID int64
		maxID  int
	}
	candidates := make([]candidate, 0, domain.MaxChannelReadOutboxFanout)
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.userID, &item.maxID); err != nil {
			return nil, err
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]domain.ChannelReadOutboxUpdate, 0, len(candidates))
	for _, item := range candidates {
		var readOutboxMaxID, readInboxMaxID int
		err := tx.QueryRow(ctx, `
UPDATE channel_members
SET read_outbox_max_id = GREATEST(read_outbox_max_id, $3),
    updated_at = now()
WHERE channel_id = $1
  AND user_id = $2
  AND status = 'active'
  AND read_outbox_max_id < $3
RETURNING read_outbox_max_id, read_inbox_max_id`, channel.ID, item.userID, item.maxID).Scan(&readOutboxMaxID, &readInboxMaxID)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("update channel sender read outbox: %w", err)
		}
		if err := upsertChannelDialogTx(ctx, tx, item.userID, channel, top, readInboxMaxID, readOutboxMaxID); err != nil {
			return nil, err
		}
		out = append(out, domain.ChannelReadOutboxUpdate{UserID: item.userID, MaxID: readOutboxMaxID})
	}
	return out, nil
}

func (s *ChannelStore) ListMessageReadParticipants(ctx context.Context, req domain.ChannelReadParticipantsRequest) (domain.ChannelReadParticipantsResult, error) {
	channel, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelReadParticipantsResult{}, err
	}
	if req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.ChannelReadParticipantsResult{}, domain.ErrMessageIDInvalid
	}
	msg, err := s.getChannelMessage(ctx, s.db, req.ChannelID, req.MessageID)
	if err != nil {
		return domain.ChannelReadParticipantsResult{}, err
	}
	if msg.Deleted || msg.ID <= member.AvailableMinID {
		return domain.ChannelReadParticipantsResult{}, domain.ErrMessageIDInvalid
	}
	result := domain.ChannelReadParticipantsResult{Channel: channel, Message: msg}
	if !channel.Megagroup || channel.ParticipantsHidden || channel.ParticipantsCount > domain.MaxChannelReadParticipants {
		return result, nil
	}
	if req.Date > 0 && msg.Date+domain.ChannelReadMarkExpirePeriod <= req.Date {
		return result, nil
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelReadParticipants {
		limit = domain.MaxChannelReadParticipants
	}
	rows, err := s.db.Query(ctx, `
SELECT user_id, read_inbox_date
FROM channel_members
WHERE channel_id = $1
  AND status = 'active'
  AND user_id <> $2
  AND available_min_id < $3
  AND read_inbox_max_id >= $3
  AND read_inbox_date > 0
  AND NOT COALESCE((banned_rights->>'ViewMessages')::boolean, false)
ORDER BY read_inbox_date ASC, user_id ASC
LIMIT $4`, req.ChannelID, req.UserID, req.MessageID, limit)
	if err != nil {
		return domain.ChannelReadParticipantsResult{}, fmt.Errorf("list channel read participants: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item domain.ChannelReadParticipant
		if err := rows.Scan(&item.UserID, &item.Date); err != nil {
			return domain.ChannelReadParticipantsResult{}, err
		}
		result.Participants = append(result.Participants, item)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelReadParticipantsResult{}, err
	}
	return result, nil
}

func (s *ChannelStore) ListChannelDifference(ctx context.Context, req domain.ChannelDifferenceRequest) (domain.ChannelDifference, error) {
	channel, member, preview, err := s.getChannelForViewer(ctx, s.db, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelDifference{}, err
	}
	if req.Pts < 0 || req.Pts > channel.Pts {
		return domain.ChannelDifference{}, domain.ErrPersistentTimestamp
	}
	if !preview && member.AvailableMinPts > req.Pts {
		req.Pts = minInt(member.AvailableMinPts, channel.Pts)
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelDifferenceLimit {
		limit = domain.MaxChannelDifferenceLimit
	}
	if channel.Pts-req.Pts > limit {
		args := []any{req.ChannelID}
		where := "channel_id = $1 AND NOT deleted"
		if member.AvailableMinID > 0 {
			args = append(args, member.AvailableMinID)
			where += fmt.Sprintf(" AND id > $%d", len(args))
		}
		args = append(args, domain.MaxChannelDifferenceTooLongMessages)
		rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE `+where+`
ORDER BY id DESC
LIMIT $`+fmt.Sprint(len(args)), args...)
		if err != nil {
			return domain.ChannelDifference{}, fmt.Errorf("list channel too long messages: %w", err)
		}
		defer rows.Close()
		diff := domain.ChannelDifference{
			Channel: channel,
			Self:    member,
			Pts:     channel.Pts,
			Final:   true,
			TooLong: true,
			Timeout: 30,
		}
		for rows.Next() {
			msg, err := scanChannelMessage(rows)
			if err != nil {
				return domain.ChannelDifference{}, err
			}
			diff.NewMessages = append(diff.NewMessages, msg)
		}
		if err := rows.Err(); err != nil {
			return domain.ChannelDifference{}, err
		}
		if preview {
			diff.Dialog = previewChannelDialog(req.UserID, channel, member)
		} else {
			dialog, err := s.getChannelDialog(ctx, s.db, req.UserID, channel)
			if err != nil {
				return domain.ChannelDifference{}, err
			}
			diff.Dialog = dialog
		}
		return diff, nil
	}
	rows, err := s.db.Query(ctx, `
SELECT channel_id, pts, pts_count, date, event_type, message_id, message_ids::text, sender_user_id, user_ids::text, payload::text
FROM channel_update_events
WHERE channel_id = $1 AND pts > $2
ORDER BY pts ASC
LIMIT $3`, req.ChannelID, req.Pts, limit)
	if err != nil {
		return domain.ChannelDifference{}, fmt.Errorf("list channel difference: %w", err)
	}
	defer rows.Close()
	diff := domain.ChannelDifference{Channel: channel, Self: member, Pts: channel.Pts, Final: true, Timeout: 30}
	userRefs := make(map[int64]struct{})
	channelRefs := make(map[int64]struct{})
	lastPts := req.Pts
	for rows.Next() {
		event, messageID, err := scanChannelEvent(rows)
		if err != nil {
			return domain.ChannelDifference{}, err
		}
		lastPts = event.Pts
		if messageID != 0 && event.Message.ID == 0 {
			msg, err := s.getChannelMessage(ctx, s.db, req.ChannelID, messageID)
			if err != nil {
				return domain.ChannelDifference{}, err
			}
			event.Message = msg
		}
		visibleEvent, ok := domain.FilterChannelUpdateEventForAvailableMinID(event, member.AvailableMinID)
		if !ok {
			continue
		}
		event = visibleEvent
		if preview && event.Type == domain.ChannelUpdateParticipant {
			continue
		}
		collectChannelEventRefs(event, req.ChannelID, userRefs, channelRefs)
		diff.Events = append(diff.Events, event)
		diff.Pts = event.Pts
		switch event.Type {
		case domain.ChannelUpdateNewMessage:
			diff.NewMessages = append(diff.NewMessages, event.Message)
		default:
			diff.OtherUpdates = append(diff.OtherUpdates, event)
		}
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelDifference{}, err
	}
	if len(diff.Events) == 0 {
		diff.Pts = lastPts
	} else if lastPts > diff.Pts {
		diff.Pts = lastPts
	}
	users, err := listUsersByIDs(ctx, s.db, mapKeysInt64(userRefs))
	if err != nil {
		return domain.ChannelDifference{}, err
	}
	channels, err := listChannelsByIDs(ctx, s.db, mapKeysInt64(channelRefs))
	if err != nil {
		return domain.ChannelDifference{}, err
	}
	diff.Users = users
	diff.Channels = channels
	if preview {
		diff.Dialog = previewChannelDialog(req.UserID, channel, member)
	} else {
		dialog, err := s.getChannelDialog(ctx, s.db, req.UserID, channel)
		if err != nil {
			return domain.ChannelDifference{}, err
		}
		diff.Dialog = dialog
	}
	diff.Final = lastPts >= channel.Pts
	return diff, nil
}

func (s *ChannelStore) ListActiveChannelIDsForUser(ctx context.Context, userID, afterChannelID int64, limit int) ([]int64, error) {
	if userID == 0 || afterChannelID < 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxSynchronousChannelDialogFanout {
		limit = domain.MaxSynchronousChannelDialogFanout
	}
	rows, err := s.db.Query(ctx, `
SELECT channel_id
FROM channel_members
WHERE user_id = $1
  AND status = 'active'
  AND channel_id > $2
ORDER BY channel_id
LIMIT $3`, userID, afterChannelID, limit)
	if err != nil {
		return nil, fmt.Errorf("list active channel ids for user: %w", err)
	}
	defer rows.Close()
	out := make([]int64, 0, limit)
	for rows.Next() {
		var channelID int64
		if err := rows.Scan(&channelID); err != nil {
			return nil, err
		}
		out = append(out, channelID)
	}
	return out, rows.Err()
}

func (s *ChannelStore) ListActiveChannelMemberIDs(ctx context.Context, viewerUserID, channelID int64, limit int) ([]int64, error) {
	if _, _, err := s.getChannelForMember(ctx, s.db, viewerUserID, channelID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > domain.MaxChannelRealtimeFanout {
		limit = domain.MaxChannelRealtimeFanout
	}
	rows, err := s.db.Query(ctx, `SELECT user_id FROM channel_members WHERE channel_id = $1 AND status = 'active' ORDER BY user_id LIMIT $2`, channelID, limit)
	if err != nil {
		return nil, fmt.Errorf("list active channel members: %w", err)
	}
	defer rows.Close()
	out := make([]int64, 0, limit)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		out = append(out, userID)
	}
	return out, rows.Err()
}

func (s *ChannelStore) ListChannelInviteAdminMemberIDs(ctx context.Context, channelID int64, limit int) ([]int64, error) {
	if channelID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxChannelRealtimeFanout {
		limit = domain.MaxChannelRealtimeFanout
	}
	rows, err := s.db.Query(ctx, `
SELECT user_id
FROM channel_members
WHERE channel_id = $1
  AND status = 'active'
  AND (
    role = 'creator' OR
    (role = 'admin' AND (
      (admin_rights->>'InviteUsers')::boolean IS TRUE OR
      (admin_rights->>'ChangeInfo')::boolean IS TRUE
    ))
  )
ORDER BY user_id
LIMIT $2`, channelID, limit)
	if err != nil {
		return nil, fmt.Errorf("list channel invite admin members: %w", err)
	}
	defer rows.Close()
	out := make([]int64, 0, limit)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		out = append(out, userID)
	}
	return out, rows.Err()
}

func (s *ChannelStore) FilterActiveChannelMemberIDs(ctx context.Context, channelID int64, userIDs []int64) ([]int64, error) {
	if channelID == 0 || len(userIDs) == 0 {
		return nil, nil
	}
	candidates := uniqueChannelUserIDs(userIDs, 0)
	if len(candidates) == 0 {
		return nil, nil
	}
	out := make([]int64, 0, len(candidates))
	for start := 0; start < len(candidates); start += channelMemberFilterBatch {
		end := start + channelMemberFilterBatch
		if end > len(candidates) {
			end = len(candidates)
		}
		rows, err := s.db.Query(ctx, `
SELECT user_id
FROM channel_members
WHERE channel_id = $1
  AND user_id = ANY($2::bigint[])
  AND status = 'active'
ORDER BY user_id`, channelID, candidates[start:end])
		if err != nil {
			return nil, fmt.Errorf("filter active channel members: %w", err)
		}
		for rows.Next() {
			var userID int64
			if err := rows.Scan(&userID); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, userID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (s *ChannelStore) MaxChannelPts(ctx context.Context, channelID int64) (int, error) {
	var pts int
	err := s.db.QueryRow(ctx, `SELECT COALESCE(MAX(pts), 0) FROM channel_update_events WHERE channel_id = $1`, channelID).Scan(&pts)
	return pts, err
}

func (s *ChannelStore) MaxChannelMessageID(ctx context.Context, channelID int64) (int, error) {
	var id int
	err := s.db.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM channel_messages WHERE channel_id = $1`, channelID).Scan(&id)
	return id, err
}

const channelColumns = `c.id, c.access_hash, c.creator_user_id, c.title, c.about, COALESCE(c.username, ''),
c.broadcast, c.megagroup, c.forum, c.forum_tabs, c.autotranslation, c.restricted_sponsored, c.broadcast_messages_allowed, c.send_paid_messages_stars, c.noforwards, c.join_to_send, c.join_request, c.signatures, c.pre_history_hidden, c.participants_hidden, c.antispam, c.linked_chat_id, c.slowmode_seconds, c.default_banned_rights::text,
c.available_reactions::text, c.color_set, c.color, c.color_background_emoji_id, c.profile_color_set, c.profile_color, c.profile_color_background_emoji_id, c.emoji_status_document_id, c.emoji_status_until,
c.participants_count, c.admins_count, c.kicked_count, c.banned_count, c.top_message_id, c.pinned_message_id, c.pts,
c.ttl_period, c.date, c.deleted, c.photo_id, c.photo_dc_id, c.photo_stripped`

const channelMessageColumns = `channel_id, id, random_id, sender_user_id, from_peer_type, from_peer_id,
send_as_peer_type, send_as_peer_id, message_date, edit_date, post, silent, noforwards, body,
entities::text, reply_to::text, reply_to_msg_id, reply_to_peer_type, reply_to_peer_id, reply_to_top_id,
fwd_from::text, discussion_channel_id, discussion_message_id, action::text, pts, deleted, media::text`

const channelForumTopicColumns = `channel_id, topic_id, creator_user_id, title, icon_color, icon_emoji_id,
title_missing, closed, hidden, pinned, pinned_order, date, top_message_id, read_inbox_max_id,
read_outbox_max_id, unread_count, unread_mentions_count, unread_reactions_count,
unread_poll_votes_count`

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *ChannelStore) getChannelForMember(ctx context.Context, db sqlcgen.DBTX, viewerUserID, channelID int64) (domain.Channel, domain.ChannelMember, error) {
	row := db.QueryRow(ctx, `
SELECT `+channelColumns+`,
       m.channel_id, m.user_id, m.inviter_user_id, m.role, m.status, m.joined_at, m.left_at,
       m.admin_rights::text, m.banned_rights::text, m.rank, m.available_min_id, m.available_min_pts,
       m.read_inbox_max_id, m.read_outbox_max_id, m.unread_mark, m.slowmode_last_send_date
FROM channels c
JOIN channel_members m ON m.channel_id = c.id AND m.user_id = $1
WHERE c.id = $2 AND NOT c.deleted`, viewerUserID, channelID)
	ch, member, err := scanChannelWithMember(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Channel{}, domain.ChannelMember{}, domain.ErrChannelPrivate
		}
		return domain.Channel{}, domain.ChannelMember{}, err
	}
	if err := validateChannelMemberVisible(member); err != nil {
		return domain.Channel{}, domain.ChannelMember{}, err
	}
	return ch, member, nil
}

func (s *ChannelStore) getChannelForViewer(ctx context.Context, db sqlcgen.DBTX, viewerUserID, channelID int64) (domain.Channel, domain.ChannelMember, bool, error) {
	ch, member, err := s.getChannelForMember(ctx, db, viewerUserID, channelID)
	if err == nil {
		return ch, member, false, nil
	}
	if !errors.Is(err, domain.ErrChannelPrivate) {
		return domain.Channel{}, domain.ChannelMember{}, false, err
	}
	ch, err = getChannelByID(ctx, db, channelID)
	if err != nil {
		return domain.Channel{}, domain.ChannelMember{}, false, err
	}
	if !publicPreviewableChannel(ch) {
		return domain.Channel{}, domain.ChannelMember{}, false, domain.ErrChannelPrivate
	}
	member, err = getPublicPreviewMember(ctx, db, viewerUserID, ch)
	if err != nil {
		return domain.Channel{}, domain.ChannelMember{}, false, err
	}
	return ch, member, true, nil
}

func getPublicPreviewMember(ctx context.Context, db sqlcgen.DBTX, viewerUserID int64, ch domain.Channel) (domain.ChannelMember, error) {
	member, err := scanChannelMember(db.QueryRow(ctx, `
SELECT channel_id, user_id, inviter_user_id, role, status, joined_at, left_at,
       admin_rights::text, banned_rights::text, rank, available_min_id, available_min_pts,
       read_inbox_max_id, read_outbox_max_id, unread_mark, slowmode_last_send_date
FROM channel_members
WHERE channel_id = $1 AND user_id = $2`, ch.ID, viewerUserID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return publicPreviewMember(ch, viewerUserID, domain.ChannelMember{}, false), nil
		}
		return domain.ChannelMember{}, err
	}
	if member.Status == domain.ChannelMemberBanned || member.Status == domain.ChannelMemberKicked || member.BannedRights.ViewMessages {
		return domain.ChannelMember{}, domain.ErrChannelUserBanned
	}
	return publicPreviewMember(ch, viewerUserID, member, true), nil
}

func getChannelByID(ctx context.Context, db sqlcgen.DBTX, channelID int64) (domain.Channel, error) {
	ch, err := scanChannel(db.QueryRow(ctx, `SELECT `+channelColumns+` FROM channels c WHERE c.id = $1 AND NOT c.deleted`, channelID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return ch, err
}

func listChannelsByIDs(ctx context.Context, db sqlcgen.DBTX, ids []int64) ([]domain.Channel, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := db.Query(ctx, `SELECT `+channelColumns+` FROM channels c WHERE c.id = ANY($1::bigint[]) AND NOT c.deleted ORDER BY c.id ASC`, ids)
	if err != nil {
		return nil, fmt.Errorf("list channels by ids: %w", err)
	}
	defer rows.Close()
	out := make([]domain.Channel, 0, len(ids))
	for rows.Next() {
		ch, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ch)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func listUsersByIDs(ctx context.Context, db sqlcgen.DBTX, ids []int64) ([]domain.User, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := db.Query(ctx, `
SELECT id, access_hash, phone, first_name, last_name, username, country_code, verified, support
FROM users
WHERE id = ANY($1::bigint[])
ORDER BY id ASC`, ids)
	if err != nil {
		return nil, fmt.Errorf("list users by ids: %w", err)
	}
	defer rows.Close()
	out := make([]domain.User, 0, len(ids))
	for rows.Next() {
		var u domain.User
		if err := rows.Scan(&u.ID, &u.AccessHash, &u.Phone, &u.FirstName, &u.LastName, &u.Username, &u.CountryCode, &u.Verified, &u.Support); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ChannelStore) getChannelMember(ctx context.Context, db sqlcgen.DBTX, channelID, userID int64) (domain.ChannelMember, error) {
	row := db.QueryRow(ctx, `
SELECT channel_id, user_id, inviter_user_id, role, status, joined_at, left_at, admin_rights::text, banned_rights::text,
       rank, available_min_id, available_min_pts, read_inbox_max_id, read_outbox_max_id, unread_mark, slowmode_last_send_date
FROM channel_members
WHERE channel_id = $1 AND user_id = $2`, channelID, userID)
	member, err := scanChannelMember(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChannelMember{}, domain.ErrChannelPrivate
	}
	return member, err
}

func (s *ChannelStore) getChannelDialog(ctx context.Context, db sqlcgen.DBTX, userID int64, channel domain.Channel) (domain.ChannelDialog, error) {
	dialog := domain.ChannelDialog{UserID: userID, ChannelID: channel.ID, TopMessageID: channel.TopMessageID}
	var defaultSendAsType sql.NullString
	var defaultSendAsID sql.NullInt64
	visibleTopID := "CASE WHEN c.top_message_id > m.available_min_id THEN c.top_message_id ELSE 0 END"
	visibleReadInbox := "COALESCE(d.read_inbox_max_id, m.read_inbox_max_id)"
	visibleUnreadCount := channelDialogVisibleUnreadCountSQL(visibleReadInbox, visibleTopID)
	err := db.QueryRow(ctx, `
SELECT `+visibleTopID+`,
       COALESCE(d.top_message_date, c.date),
       COALESCE(d.folder_id, 0),
       `+visibleReadInbox+`,
       COALESCE(d.read_outbox_max_id, m.read_outbox_max_id),
       `+visibleUnreadCount+`,
       COALESCE(d.pinned, false),
       COALESCE(d.pinned_order, 0),
       COALESCE(d.unread_mark, m.unread_mark),
       COALESCE(d.unread_mentions_count, 0),
       COALESCE(d.unread_reactions_count, 0),
       COALESCE(d.view_forum_as_messages, false),
       d.default_send_as_peer_type,
       d.default_send_as_peer_id
FROM channels c
JOIN channel_members m ON m.channel_id = c.id AND m.user_id = $1
LEFT JOIN channel_dialogs d ON d.user_id = m.user_id AND d.channel_id = m.channel_id
WHERE c.id = $2`, userID, channel.ID).Scan(
		&dialog.TopMessageID,
		&dialog.TopMessageDate,
		&dialog.FolderID,
		&dialog.ReadInboxMaxID,
		&dialog.ReadOutboxMaxID,
		&dialog.UnreadCount,
		&dialog.Pinned,
		&dialog.PinnedOrder,
		&dialog.UnreadMark,
		&dialog.UnreadMentions,
		&dialog.UnreadReactions,
		&dialog.ViewForumAsMessages,
		&defaultSendAsType,
		&defaultSendAsID,
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return domain.ChannelDialog{}, fmt.Errorf("get channel dialog: %w", err)
	}
	if defaultSendAsType.Valid && defaultSendAsID.Valid && defaultSendAsID.Int64 != 0 {
		dialog.DefaultSendAs = &domain.Peer{Type: domain.PeerType(defaultSendAsType.String), ID: defaultSendAsID.Int64}
	}
	if dialog.TopMessageID != 0 {
		if msg, err := s.getChannelMessage(ctx, db, channel.ID, dialog.TopMessageID); err == nil {
			dialog.TopMessageDate = msg.Date
		}
	}
	return dialog, nil
}

func (s *ChannelStore) getChannelMessage(ctx context.Context, db sqlcgen.DBTX, channelID int64, id int) (domain.ChannelMessage, error) {
	if channelID == 0 || id == 0 {
		return domain.ChannelMessage{}, pgx.ErrNoRows
	}
	msg, err := scanChannelMessage(db.QueryRow(ctx, `SELECT `+channelMessageColumns+` FROM channel_messages WHERE channel_id = $1 AND id = $2`, channelID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChannelMessage{}, domain.ErrMessageIDInvalid
	}
	return msg, err
}

func (s *ChannelStore) getForumTopic(ctx context.Context, db sqlcgen.DBTX, channelID int64, topicID int) (domain.ChannelForumTopic, error) {
	if channelID == 0 || topicID == 0 {
		return domain.ChannelForumTopic{}, domain.ErrMessageIDInvalid
	}
	topic, err := scanChannelForumTopic(db.QueryRow(ctx, `
SELECT `+channelForumTopicColumns+`
FROM channel_forum_topics
WHERE channel_id = $1 AND topic_id = $2 AND NOT deleted`, channelID, topicID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChannelForumTopic{}, domain.ErrMessageIDInvalid
	}
	return topic, err
}

func (s *ChannelStore) forumTopicRootMessages(ctx context.Context, channelID int64, topics []domain.ChannelForumTopic, availableMinID int) ([]domain.ChannelMessage, error) {
	if len(topics) == 0 {
		return nil, nil
	}
	ids := make([]int, 0, len(topics))
	seen := make(map[int]struct{}, len(topics))
	for _, topic := range topics {
		if topic.TopMessageID <= 0 {
			continue
		}
		if _, ok := seen[topic.TopMessageID]; ok {
			continue
		}
		seen[topic.TopMessageID] = struct{}{}
		ids = append(ids, topic.TopMessageID)
	}
	id32, _, err := validUniqueChannelMessageIDs(ids)
	if err != nil {
		return nil, err
	}
	if len(id32) == 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE channel_id = $1 AND id = ANY($2::int[]) AND id > $3 AND NOT deleted
ORDER BY id DESC`, channelID, id32, availableMinID)
	if err != nil {
		return nil, fmt.Errorf("list forum topic root messages: %w", err)
	}
	defer rows.Close()
	messages := make([]domain.ChannelMessage, 0, len(id32))
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (s *ChannelStore) nextForumTopicPinnedOrder(ctx context.Context, channelID int64) (int, error) {
	var maxOrder int
	if err := s.db.QueryRow(ctx, `
SELECT COALESCE(MAX(pinned_order), 0)::int
FROM channel_forum_topics
WHERE channel_id = $1 AND pinned AND NOT deleted`, channelID).Scan(&maxOrder); err != nil {
		return 0, fmt.Errorf("next forum topic pinned order: %w", err)
	}
	return maxOrder + 1, nil
}

type channelReplyStatKey struct {
	channelID int64
	rootID    int
}

type channelReactionMessageKey struct {
	channelID int64
	messageID int
}

type channelReactionCursor struct {
	date     int
	userID   int64
	emoticon string
}

func emptyChannelMessageReactions(channel domain.Channel) domain.ChannelMessageReactions {
	return domain.ChannelMessageReactions{
		CanSeeList: !channel.Broadcast || channel.Megagroup,
		Results:    []domain.ChannelMessageReactionCount{},
		Recent:     []domain.ChannelMessagePeerReaction{},
	}
}

func (s *ChannelStore) populateChannelMessagesReactions(ctx context.Context, db sqlcgen.DBTX, viewerUserID int64, channels []domain.Channel, messages []domain.ChannelMessage) error {
	if len(messages) == 0 {
		return nil
	}
	channelsByID := make(map[int64]domain.Channel, len(channels))
	for _, ch := range channels {
		if ch.ID != 0 {
			channelsByID[ch.ID] = ch
		}
	}
	indexes := make(map[channelReactionMessageKey][]int)
	idsByChannel := make(map[int64][]int32)
	for i := range messages {
		if messages[i].ChannelID == 0 || messages[i].ID <= 0 {
			continue
		}
		key := channelReactionMessageKey{channelID: messages[i].ChannelID, messageID: messages[i].ID}
		if _, ok := indexes[key]; !ok {
			idsByChannel[messages[i].ChannelID] = append(idsByChannel[messages[i].ChannelID], int32(messages[i].ID))
		}
		indexes[key] = append(indexes[key], i)
	}
	for channelID, ids := range idsByChannel {
		ch := channelsByID[channelID]
		if ch.ID == 0 {
			var err error
			ch, err = getChannelByID(ctx, db, channelID)
			if err != nil {
				return err
			}
			channelsByID[channelID] = ch
		}
		rows, err := db.Query(ctx, `
SELECT message_id, reaction_type, reaction_value, COUNT(*)::int,
       COALESCE(MAX(CASE WHEN reacted_user_id = $3 THEN chosen_order ELSE 0 END), 0)::int,
       COALESCE(MAX(reaction_date), 0)::int
FROM channel_message_reactions
WHERE channel_id = $1 AND message_id = ANY($2::int[])
GROUP BY message_id, reaction_type, reaction_value
ORDER BY message_id ASC, COUNT(*) DESC, COALESCE(MAX(reaction_date), 0) DESC, reaction_value ASC`, channelID, ids, viewerUserID)
		if err != nil {
			return fmt.Errorf("load channel message reaction counts: %w", err)
		}
		for rows.Next() {
			var msgID int
			var reactionType, reactionValue string
			var count, chosenOrder, latestDate int
			if err := rows.Scan(&msgID, &reactionType, &reactionValue, &count, &chosenOrder, &latestDate); err != nil {
				rows.Close()
				return err
			}
			_ = latestDate
			key := channelReactionMessageKey{channelID: channelID, messageID: msgID}
			for _, idx := range indexes[key] {
				if messages[idx].Reactions == nil {
					reactions := emptyChannelMessageReactions(ch)
					messages[idx].Reactions = &reactions
				}
				messages[idx].Reactions.Results = append(messages[idx].Reactions.Results, domain.ChannelMessageReactionCount{
					Reaction: domain.MessageReaction{
						Type:     domain.MessageReactionType(reactionType),
						Emoticon: reactionValue,
					},
					Count:       count,
					ChosenOrder: chosenOrder,
				})
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		rows, err = db.Query(ctx, `
SELECT channel_id, message_id, reacted_user_id, sender_user_id, reaction_type, reaction_value,
       big, unread, chosen_order, reaction_date
FROM (
    SELECT channel_id, message_id, reacted_user_id, sender_user_id, reaction_type, reaction_value,
           big, unread, chosen_order, reaction_date,
           row_number() OVER (
               PARTITION BY message_id
               ORDER BY reaction_date DESC, reacted_user_id DESC, reaction_value ASC
           ) AS rn
    FROM channel_message_reactions
    WHERE channel_id = $1 AND message_id = ANY($2::int[])
) ranked
WHERE rn <= $3
ORDER BY message_id ASC, reaction_date DESC, reacted_user_id DESC, reaction_value ASC`, channelID, ids, domain.MaxChannelMessageReactionRecent)
		if err != nil {
			return fmt.Errorf("load channel message recent reactions: %w", err)
		}
		for rows.Next() {
			row, err := scanChannelMessagePeerReaction(rows, viewerUserID)
			if err != nil {
				rows.Close()
				return err
			}
			key := channelReactionMessageKey{channelID: row.ChannelID, messageID: row.MessageID}
			for _, idx := range indexes[key] {
				if messages[idx].Reactions == nil {
					reactions := emptyChannelMessageReactions(ch)
					messages[idx].Reactions = &reactions
				}
				messages[idx].Reactions.Recent = append(messages[idx].Reactions.Recent, row)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}
	return nil
}

func channelReactionOffset(row domain.ChannelMessagePeerReaction) string {
	return strconv.Itoa(row.Date) + ":" + strconv.FormatInt(row.UserID, 10) + ":" + row.Reaction.Emoticon
}

func parseChannelReactionOffset(offset string) (channelReactionCursor, bool) {
	parts := strings.SplitN(offset, ":", 3)
	if len(parts) != 3 {
		return channelReactionCursor{}, false
	}
	date, err := strconv.Atoi(parts[0])
	if err != nil || date < 0 {
		return channelReactionCursor{}, false
	}
	userID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || userID < 0 {
		return channelReactionCursor{}, false
	}
	return channelReactionCursor{date: date, userID: userID, emoticon: parts[2]}, true
}

func (s *ChannelStore) populateChannelMessageReplies(ctx context.Context, db sqlcgen.DBTX, viewerUserID int64, channel domain.Channel, messages []domain.ChannelMessage) error {
	if len(messages) == 0 || channel.ID == 0 {
		return nil
	}
	indexes := make(map[channelReplyStatKey][]int)
	rootsByChannel := make(map[int64][]int32)
	readMaxByChannel := make(map[int64]int)
	for i := range messages {
		targetChannelID := channel.ID
		rootID := messages[i].ID
		replies := &domain.ChannelMessageReplies{}
		if messages[i].Discussion != nil && messages[i].Discussion.ChannelID != 0 && messages[i].Discussion.MessageID != 0 {
			targetChannelID = messages[i].Discussion.ChannelID
			rootID = messages[i].Discussion.MessageID
			replies.Comments = true
			replies.ChannelID = messages[i].Discussion.ChannelID
		} else if channel.Broadcast && channel.LinkedChatID != 0 && messages[i].Post {
			replies.Comments = true
			replies.ChannelID = channel.LinkedChatID
		}
		if _, ok := readMaxByChannel[targetChannelID]; !ok {
			readInbox, _ := s.channelReadWatermarks(ctx, targetChannelID, viewerUserID)
			readMaxByChannel[targetChannelID] = readInbox
		}
		replies.ReadMaxID = readMaxByChannel[targetChannelID]
		key := channelReplyStatKey{channelID: targetChannelID, rootID: rootID}
		if _, ok := indexes[key]; !ok {
			rootsByChannel[targetChannelID] = append(rootsByChannel[targetChannelID], int32(rootID))
		}
		indexes[key] = append(indexes[key], i)
		if replies.Comments {
			messages[i].Replies = replies
		}
	}
	for channelID, roots := range rootsByChannel {
		rows, err := db.Query(ctx, `
SELECT reply_to_top_id, COUNT(*)::int, COALESCE(MAX(id), 0)::int, COALESCE((array_agg(pts ORDER BY id DESC))[1], 0)::int
FROM channel_messages
WHERE channel_id = $1 AND reply_to_top_id = ANY($2::int[]) AND NOT deleted
GROUP BY reply_to_top_id`, channelID, roots)
		if err != nil {
			return fmt.Errorf("load channel reply stats: %w", err)
		}
		for rows.Next() {
			var rootID, count, maxID, repliesPts int
			if err := rows.Scan(&rootID, &count, &maxID, &repliesPts); err != nil {
				rows.Close()
				return err
			}
			for _, idx := range indexes[channelReplyStatKey{channelID: channelID, rootID: rootID}] {
				replies := messages[idx].Replies
				if replies == nil {
					replies = &domain.ChannelMessageReplies{ReadMaxID: readMaxByChannel[channelID]}
				}
				replies.Replies = count
				replies.MaxID = maxID
				replies.RepliesPts = repliesPts
				messages[idx].Replies = replies
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}
	return nil
}

func (s *ChannelStore) channelReadWatermarks(ctx context.Context, channelID, userID int64) (int, int) {
	var inbox, outbox int
	_ = s.db.QueryRow(ctx, `SELECT read_inbox_max_id, read_outbox_max_id FROM channel_members WHERE channel_id = $1 AND user_id = $2`, channelID, userID).Scan(&inbox, &outbox)
	return inbox, outbox
}

func (s *ChannelStore) channelThreadUnreadCount(ctx context.Context, channelID int64, rootID int, viewerUserID int64, readMaxID int) int {
	var count int
	_ = s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_messages
WHERE channel_id = $1 AND reply_to_top_id = $2 AND id > $3 AND sender_user_id <> $4 AND NOT deleted`, channelID, rootID, readMaxID, viewerUserID).Scan(&count)
	return count
}

func countChannelUnreadMessages(ctx context.Context, db sqlcgen.DBTX, userID, channelID int64, readMaxID, topID int) (int, error) {
	if userID == 0 || channelID == 0 || topID <= readMaxID {
		return 0, nil
	}
	var count int
	if err := db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_messages
WHERE channel_id = $1
  AND id > $2
  AND id <= $3
  AND sender_user_id <> $4
  AND NOT deleted`, channelID, readMaxID, topID, userID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count channel unread messages: %w", err)
	}
	return count, nil
}

func (s *ChannelStore) topicWithViewerCounters(ctx context.Context, viewerUserID, channelID int64, topic domain.ChannelForumTopic, readMaxID, availableMinID int) domain.ChannelForumTopic {
	topic.UnreadCount = s.channelThreadUnreadCount(ctx, channelID, topic.TopicID, viewerUserID, readMaxID)
	topic.UnreadMentionsCount = s.countChannelUnreadMentionsForTop(ctx, viewerUserID, channelID, topic.TopicID)
	topic.UnreadReactionsCount = s.countChannelUnreadReactionsForTop(ctx, viewerUserID, channelID, topic.TopicID, availableMinID)
	return topic
}

func (s *ChannelStore) countChannelUnreadMentionsForTop(ctx context.Context, userID, channelID int64, topMsgID int) int {
	var count int
	_ = s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_unread_mentions
WHERE user_id = $1 AND channel_id = $2 AND top_message_id = $3`, userID, channelID, topMsgID).Scan(&count)
	return count
}

func (s *ChannelStore) countChannelUnreadReactionsForTop(ctx context.Context, userID, channelID int64, topMsgID, availableMinID int) int {
	var count int
	_ = s.db.QueryRow(ctx, `
SELECT COUNT(DISTINCT r.message_id)::int
FROM channel_message_reactions r
JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
WHERE r.sender_user_id = $1
  AND r.channel_id = $2
  AND r.unread
  AND r.reacted_user_id <> $1
  AND cm.id > $4
  AND NOT cm.deleted
  AND (cm.id = $3 OR COALESCE(NULLIF(cm.reply_to_top_id, 0), NULLIF(cm.reply_to_msg_id, 0), 0) = $3)`, userID, channelID, topMsgID, availableMinID).Scan(&count)
	return count
}

func (s *ChannelStore) countChannelReplies(ctx context.Context, channelID int64, rootID, availableMinID int, filter domain.ChannelRepliesFilter) (int, error) {
	where, args := channelRepliesBaseWhere(channelID, rootID, availableMinID, filter)
	var count int
	if err := s.db.QueryRow(ctx, `SELECT COUNT(*)::int FROM channel_messages WHERE `+where, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count channel replies: %w", err)
	}
	return count, nil
}

func (s *ChannelStore) countChannelUnreadMentions(ctx context.Context, userID int64, filter domain.ChannelUnreadMentionsFilter, availableMinID int) (int, error) {
	where, args := channelUnreadMentionBaseWhere(userID, filter, availableMinID)
	var count int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_unread_mentions um
JOIN channel_messages cm ON cm.channel_id = um.channel_id AND cm.id = um.message_id
WHERE `+where, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count channel unread mentions: %w", err)
	}
	return count, nil
}

func (s *ChannelStore) queryChannelUnreadMentionsPage(ctx context.Context, userID int64, filter domain.ChannelUnreadMentionsFilter, availableMinID, limit int) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	switch messageHistoryLoadType(filter.AddOffset, limit) {
	case messageHistoryLoadForward:
		return s.queryChannelUnreadMentionsForward(ctx, userID, filter, availableMinID, limit)
	case messageHistoryLoadAround:
		forwardLimit := -filter.AddOffset
		if forwardLimit > limit {
			forwardLimit = limit
		}
		backwardLimit := limit + filter.AddOffset
		if backwardLimit < 0 {
			backwardLimit = 0
		}
		forward, err := s.queryChannelUnreadMentionsForward(ctx, userID, filter, availableMinID, forwardLimit)
		if err != nil {
			return nil, err
		}
		backward, err := s.queryChannelUnreadMentionsBackward(ctx, userID, filter, availableMinID, backwardLimit, true)
		if err != nil {
			return nil, err
		}
		out := append(forward, backward...)
		sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
		return out, nil
	default:
		start := filter.AddOffset
		if start < 0 {
			start = 0
		}
		items, err := s.queryChannelUnreadMentionsBackward(ctx, userID, filter, availableMinID, limit+start, false)
		if err != nil || start >= len(items) {
			return nil, err
		}
		return items[start:], nil
	}
}

func (s *ChannelStore) queryChannelUnreadMentionsBackward(ctx context.Context, userID int64, filter domain.ChannelUnreadMentionsFilter, availableMinID, limit int, includeOffset bool) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	where, args := channelUnreadMentionBaseWhere(userID, filter, availableMinID)
	where, args = appendChannelUnreadMentionBackwardOffset(where, args, filter, includeOffset)
	args = append(args, limit)
	return s.queryChannelUnreadMentions(ctx, filter.ChannelID, where, args, "DESC")
}

func (s *ChannelStore) queryChannelUnreadMentionsForward(ctx context.Context, userID int64, filter domain.ChannelUnreadMentionsFilter, availableMinID, limit int) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	where, args := channelUnreadMentionBaseWhere(userID, filter, availableMinID)
	where, args = appendChannelUnreadMentionForwardOffset(where, args, filter)
	args = append(args, limit)
	out, err := s.queryChannelUnreadMentions(ctx, filter.ChannelID, where, args, "ASC")
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
	return out, nil
}

func (s *ChannelStore) queryChannelUnreadMentions(ctx context.Context, channelID int64, where string, args []any, order string) ([]domain.ChannelMessage, error) {
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE channel_id = $2
  AND id = ANY(ARRAY(
      SELECT cm.id
      FROM channel_unread_mentions um
      JOIN channel_messages cm ON cm.channel_id = um.channel_id AND cm.id = um.message_id
      WHERE `+where+`
      ORDER BY cm.id `+order+`
      LIMIT $`+fmt.Sprint(len(args))+`
  )::int[])
ORDER BY id `+order, args...)
	_ = channelID
	if err != nil {
		return nil, fmt.Errorf("list channel unread mentions: %w", err)
	}
	defer rows.Close()
	out := make([]domain.ChannelMessage, 0)
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func channelUnreadMentionBaseWhere(userID int64, filter domain.ChannelUnreadMentionsFilter, availableMinID int) (string, []any) {
	args := []any{userID, filter.ChannelID}
	where := "um.user_id = $1 AND um.channel_id = $2 AND NOT cm.deleted"
	if availableMinID > 0 {
		args = append(args, availableMinID)
		where += fmt.Sprintf(" AND cm.id > $%d", len(args))
	}
	if filter.TopMsgID > 0 {
		args = append(args, filter.TopMsgID)
		where += fmt.Sprintf(" AND um.top_message_id = $%d", len(args))
	}
	if filter.MaxID > 0 {
		args = append(args, filter.MaxID)
		where += fmt.Sprintf(" AND cm.id < $%d", len(args))
	}
	if filter.MinID > 0 {
		args = append(args, filter.MinID)
		where += fmt.Sprintf(" AND cm.id > $%d", len(args))
	}
	return where, args
}

func appendChannelUnreadMentionBackwardOffset(where string, args []any, filter domain.ChannelUnreadMentionsFilter, include bool) (string, []any) {
	if filter.OffsetDate > 0 {
		args = append(args, filter.OffsetDate)
		if include {
			return where + fmt.Sprintf(" AND cm.message_date <= $%d", len(args)), args
		}
		return where + fmt.Sprintf(" AND cm.message_date < $%d", len(args)), args
	}
	if filter.OffsetID > 0 {
		args = append(args, filter.OffsetID)
		if include {
			return where + fmt.Sprintf(" AND cm.id <= $%d", len(args)), args
		}
		return where + fmt.Sprintf(" AND cm.id < $%d", len(args)), args
	}
	return where, args
}

func appendChannelUnreadMentionForwardOffset(where string, args []any, filter domain.ChannelUnreadMentionsFilter) (string, []any) {
	if filter.OffsetDate > 0 {
		args = append(args, filter.OffsetDate)
		return where + fmt.Sprintf(" AND cm.message_date >= $%d", len(args)), args
	}
	if filter.OffsetID > 0 {
		args = append(args, filter.OffsetID)
		return where + fmt.Sprintf(" AND cm.id > $%d", len(args)), args
	}
	return where, args
}

func (s *ChannelStore) countChannelUnreadReactions(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID int) (int, error) {
	where, args := channelUnreadReactionBaseWhere(userID, filter, availableMinID)
	var count int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(DISTINCT cm.id)::int
FROM channel_message_reactions r
JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
WHERE `+where, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count channel unread reactions: %w", err)
	}
	return count, nil
}

func (s *ChannelStore) queryChannelUnreadReactionsPage(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID, limit int) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	switch messageHistoryLoadType(filter.AddOffset, limit) {
	case messageHistoryLoadForward:
		return s.queryChannelUnreadReactionsForward(ctx, userID, filter, availableMinID, limit)
	case messageHistoryLoadAround:
		forwardLimit := -filter.AddOffset
		if forwardLimit > limit {
			forwardLimit = limit
		}
		backwardLimit := limit + filter.AddOffset
		if backwardLimit < 0 {
			backwardLimit = 0
		}
		forward, err := s.queryChannelUnreadReactionsForward(ctx, userID, filter, availableMinID, forwardLimit)
		if err != nil {
			return nil, err
		}
		backward, err := s.queryChannelUnreadReactionsBackward(ctx, userID, filter, availableMinID, backwardLimit, true)
		if err != nil {
			return nil, err
		}
		out := append(forward, backward...)
		sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
		return out, nil
	default:
		start := filter.AddOffset
		if start < 0 {
			start = 0
		}
		items, err := s.queryChannelUnreadReactionsBackward(ctx, userID, filter, availableMinID, limit+start, false)
		if err != nil || start >= len(items) {
			return nil, err
		}
		return items[start:], nil
	}
}

func (s *ChannelStore) queryChannelUnreadReactionsBackward(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID, limit int, includeOffset bool) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	where, args := channelUnreadReactionBaseWhere(userID, filter, availableMinID)
	where, args = appendChannelUnreadReactionBackwardOffset(where, args, filter, includeOffset)
	args = append(args, limit)
	return s.queryChannelUnreadReactions(ctx, filter.ChannelID, where, args, "DESC")
}

func (s *ChannelStore) queryChannelUnreadReactionsForward(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID, limit int) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	where, args := channelUnreadReactionBaseWhere(userID, filter, availableMinID)
	where, args = appendChannelUnreadReactionForwardOffset(where, args, filter)
	args = append(args, limit)
	out, err := s.queryChannelUnreadReactions(ctx, filter.ChannelID, where, args, "ASC")
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
	return out, nil
}

func (s *ChannelStore) queryChannelUnreadReactions(ctx context.Context, channelID int64, where string, args []any, order string) ([]domain.ChannelMessage, error) {
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE channel_id = $2
  AND id = ANY(ARRAY(
      SELECT DISTINCT cm.id
      FROM channel_message_reactions r
      JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
      WHERE `+where+`
      ORDER BY cm.id `+order+`
      LIMIT $`+fmt.Sprint(len(args))+`
  )::int[])
ORDER BY id `+order, args...)
	_ = channelID
	if err != nil {
		return nil, fmt.Errorf("list channel unread reactions: %w", err)
	}
	defer rows.Close()
	out := make([]domain.ChannelMessage, 0)
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func channelUnreadReactionBaseWhere(userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID int) (string, []any) {
	args := []any{userID, filter.ChannelID}
	where := "r.sender_user_id = $1 AND r.channel_id = $2 AND r.unread AND r.reacted_user_id <> $1 AND NOT cm.deleted"
	if availableMinID > 0 {
		args = append(args, availableMinID)
		where += fmt.Sprintf(" AND cm.id > $%d", len(args))
	}
	if filter.TopMsgID > 0 {
		args = append(args, filter.TopMsgID)
		where += fmt.Sprintf(" AND (cm.id = $%d OR COALESCE(NULLIF(cm.reply_to_top_id, 0), NULLIF(cm.reply_to_msg_id, 0), 0) = $%d)", len(args), len(args))
	}
	if filter.MaxID > 0 {
		args = append(args, filter.MaxID)
		where += fmt.Sprintf(" AND cm.id < $%d", len(args))
	}
	if filter.MinID > 0 {
		args = append(args, filter.MinID)
		where += fmt.Sprintf(" AND cm.id > $%d", len(args))
	}
	return where, args
}

func appendChannelUnreadReactionBackwardOffset(where string, args []any, filter domain.ChannelUnreadReactionsFilter, include bool) (string, []any) {
	if filter.OffsetID > 0 {
		args = append(args, filter.OffsetID)
		if include {
			return where + fmt.Sprintf(" AND cm.id <= $%d", len(args)), args
		}
		return where + fmt.Sprintf(" AND cm.id < $%d", len(args)), args
	}
	return where, args
}

func appendChannelUnreadReactionForwardOffset(where string, args []any, filter domain.ChannelUnreadReactionsFilter) (string, []any) {
	if filter.OffsetID > 0 {
		args = append(args, filter.OffsetID)
		return where + fmt.Sprintf(" AND cm.id > $%d", len(args)), args
	}
	return where, args
}

func (s *ChannelStore) queryChannelRepliesPage(ctx context.Context, channelID int64, rootID, availableMinID int, filter domain.ChannelRepliesFilter, limit int) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	switch messageHistoryLoadType(filter.AddOffset, limit) {
	case messageHistoryLoadForward:
		return s.queryChannelRepliesForward(ctx, channelID, rootID, availableMinID, filter, limit)
	case messageHistoryLoadAround:
		forwardLimit := -filter.AddOffset
		if forwardLimit > limit {
			forwardLimit = limit
		}
		backwardLimit := limit + filter.AddOffset
		if backwardLimit < 0 {
			backwardLimit = 0
		}
		forward, err := s.queryChannelRepliesForward(ctx, channelID, rootID, availableMinID, filter, forwardLimit)
		if err != nil {
			return nil, err
		}
		backward, err := s.queryChannelRepliesBackward(ctx, channelID, rootID, availableMinID, filter, backwardLimit, true)
		if err != nil {
			return nil, err
		}
		out := append(forward, backward...)
		sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
		return out, nil
	default:
		start := filter.AddOffset
		if start < 0 {
			start = 0
		}
		items, err := s.queryChannelRepliesBackward(ctx, channelID, rootID, availableMinID, filter, limit+start, false)
		if err != nil || start >= len(items) {
			return nil, err
		}
		return items[start:], nil
	}
}

func (s *ChannelStore) queryChannelRepliesBackward(ctx context.Context, channelID int64, rootID, availableMinID int, filter domain.ChannelRepliesFilter, limit int, includeOffset bool) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	where, args := channelRepliesBaseWhere(channelID, rootID, availableMinID, filter)
	where, args = appendChannelRepliesBackwardOffset(where, args, filter, includeOffset)
	args = append(args, limit)
	return s.queryChannelReplies(ctx, where, args, "DESC")
}

func (s *ChannelStore) queryChannelRepliesForward(ctx context.Context, channelID int64, rootID, availableMinID int, filter domain.ChannelRepliesFilter, limit int) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	where, args := channelRepliesBaseWhere(channelID, rootID, availableMinID, filter)
	where, args = appendChannelRepliesForwardOffset(where, args, filter)
	args = append(args, limit)
	out, err := s.queryChannelReplies(ctx, where, args, "ASC")
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
	return out, nil
}

func (s *ChannelStore) queryChannelReplies(ctx context.Context, where string, args []any, order string) ([]domain.ChannelMessage, error) {
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE `+where+`
ORDER BY id `+order+`
LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("list channel replies: %w", err)
	}
	defer rows.Close()
	out := make([]domain.ChannelMessage, 0)
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func channelRepliesBaseWhere(channelID int64, rootID, availableMinID int, filter domain.ChannelRepliesFilter) (string, []any) {
	args := []any{channelID, rootID}
	where := "channel_id = $1 AND reply_to_top_id = $2 AND NOT deleted"
	if availableMinID > 0 {
		args = append(args, availableMinID)
		where += fmt.Sprintf(" AND id > $%d", len(args))
	}
	if filter.MaxID > 0 {
		args = append(args, filter.MaxID)
		where += fmt.Sprintf(" AND id < $%d", len(args))
	}
	if filter.MinID > 0 {
		args = append(args, filter.MinID)
		where += fmt.Sprintf(" AND id > $%d", len(args))
	}
	return where, args
}

func appendChannelRepliesBackwardOffset(where string, args []any, filter domain.ChannelRepliesFilter, include bool) (string, []any) {
	if filter.OffsetDate > 0 {
		args = append(args, filter.OffsetDate)
		if include {
			return where + fmt.Sprintf(" AND message_date <= $%d", len(args)), args
		}
		return where + fmt.Sprintf(" AND message_date < $%d", len(args)), args
	}
	if filter.OffsetID > 0 {
		args = append(args, filter.OffsetID)
		if include {
			return where + fmt.Sprintf(" AND id <= $%d", len(args)), args
		}
		return where + fmt.Sprintf(" AND id < $%d", len(args)), args
	}
	return where, args
}

func appendChannelRepliesForwardOffset(where string, args []any, filter domain.ChannelRepliesFilter) (string, []any) {
	if filter.OffsetDate > 0 {
		args = append(args, filter.OffsetDate)
		return where + fmt.Sprintf(" AND message_date >= $%d", len(args)), args
	}
	if filter.OffsetID > 0 {
		args = append(args, filter.OffsetID)
		return where + fmt.Sprintf(" AND id > $%d", len(args)), args
	}
	return where + " AND false", args
}

type messageHistoryLoad int

const (
	messageHistoryLoadBackward messageHistoryLoad = iota
	messageHistoryLoadForward
	messageHistoryLoadAround
)

func messageHistoryLoadType(addOffset, limit int) messageHistoryLoad {
	if addOffset >= 0 {
		return messageHistoryLoadBackward
	}
	if addOffset+limit > 0 {
		return messageHistoryLoadAround
	}
	return messageHistoryLoadForward
}

func channelMessageLess(a, b domain.ChannelMessage) bool {
	if a.Date != b.Date {
		return a.Date > b.Date
	}
	return a.ID > b.ID
}

func (s *ChannelStore) resolveChannelReply(ctx context.Context, db sqlcgen.DBTX, req domain.SendChannelMessageRequest, member domain.ChannelMember, channel domain.Channel) (*domain.MessageReply, error) {
	if req.ReplyTo == nil {
		return nil, nil
	}
	if err := domain.ValidateMessageReplyBounds(req.ReplyTo); err != nil {
		return nil, err
	}
	peer := req.ReplyTo.Peer
	channelPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: req.ChannelID}
	if peer.ID == 0 {
		peer = channelPeer
	}
	if peer != channelPeer {
		return nil, domain.ErrReplyMessageIDInvalid
	}
	if req.ReplyTo.MessageID == 0 {
		if req.ReplyTo.TopMessageID <= 0 || !channel.Forum {
			return nil, domain.ErrReplyMessageIDInvalid
		}
		topic, err := s.getForumTopic(ctx, db, req.ChannelID, req.ReplyTo.TopMessageID)
		if err != nil {
			return nil, domain.ErrReplyMessageIDInvalid
		}
		if topic.Hidden {
			return nil, domain.ErrReplyMessageIDInvalid
		}
		if topic.Closed && !canManageForumTopic(channel, member, topic, req.UserID) {
			return nil, domain.ErrChannelWriteForbidden
		}
		reply := cloneMessageReply(req.ReplyTo)
		reply.MessageID = 0
		reply.Peer = channelPeer
		reply.TopMessageID = topic.TopicID
		reply.ForumTopic = true
		return reply, nil
	}
	target, err := s.getChannelMessage(ctx, db, req.ChannelID, req.ReplyTo.MessageID)
	if err != nil {
		if errors.Is(err, domain.ErrMessageIDInvalid) || errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrReplyMessageIDInvalid
		}
		return nil, err
	}
	if target.Deleted || target.ID <= member.AvailableMinID {
		return nil, domain.ErrReplyMessageIDInvalid
	}
	reply := cloneMessageReply(req.ReplyTo)
	reply.MessageID = target.ID
	reply.Peer = channelPeer
	reply.TopMessageID = target.ID
	if target.ReplyTo != nil && target.ReplyTo.TopMessageID > 0 {
		reply.TopMessageID = target.ReplyTo.TopMessageID
	}
	if req.ReplyTo.TopMessageID > 0 && req.ReplyTo.TopMessageID != reply.TopMessageID {
		return nil, domain.ErrReplyMessageIDInvalid
	}
	if channel.Forum && reply.TopMessageID > 0 {
		if topic, err := s.getForumTopic(ctx, db, req.ChannelID, reply.TopMessageID); err == nil && !topic.Hidden {
			if topic.Closed && !canManageForumTopic(channel, member, topic, req.UserID) {
				return nil, domain.ErrChannelWriteForbidden
			}
			reply.ForumTopic = true
		} else if err != nil && !errors.Is(err, domain.ErrMessageIDInvalid) {
			return nil, err
		}
	}
	return reply, nil
}

func (s *ChannelStore) duplicateChannelMessage(ctx context.Context, channelID, userID, randomID int64) (domain.SendChannelMessageResult, bool, error) {
	row := s.db.QueryRow(ctx, `SELECT `+channelMessageColumns+` FROM channel_messages WHERE channel_id = $1 AND sender_user_id = $2 AND random_id = $3`, channelID, userID, randomID)
	msg, err := scanChannelMessage(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SendChannelMessageResult{}, false, nil
	}
	if err != nil {
		return domain.SendChannelMessageResult{}, false, err
	}
	channel, err := getChannelByID(ctx, s.db, channelID)
	if err != nil {
		return domain.SendChannelMessageResult{}, false, err
	}
	event, err := s.eventForChannelMessage(ctx, channelID, msg.ID)
	if err != nil {
		return domain.SendChannelMessageResult{}, false, err
	}
	if event.Message.ID != 0 {
		msg = event.Message
	}
	return domain.SendChannelMessageResult{Channel: channel, Message: msg, Event: event, Duplicate: true}, true, nil
}

func (s *ChannelStore) eventForChannelMessage(ctx context.Context, channelID int64, messageID int) (domain.ChannelUpdateEvent, error) {
	row := s.db.QueryRow(ctx, `
SELECT channel_id, pts, pts_count, date, event_type, message_id, message_ids::text, sender_user_id, user_ids::text, payload::text
FROM channel_update_events
WHERE channel_id = $1 AND message_id = $2 AND event_type = $3
ORDER BY pts ASC LIMIT 1`, channelID, messageID, string(domain.ChannelUpdateNewMessage))
	event, _, err := scanChannelEvent(row)
	return event, err
}

func (s *ChannelStore) insertServiceMessage(ctx context.Context, tx pgx.Tx, channel domain.Channel, senderUserID int64, date int, action domain.ChannelMessageAction, reserved *[]reservedChannelPts) (domain.ChannelMessage, domain.ChannelUpdateEvent, error) {
	msgID, err := s.msgIDs.NextChannelMessageID(ctx, channel.ID)
	if err != nil {
		return domain.ChannelMessage{}, domain.ChannelUpdateEvent{}, fmt.Errorf("allocate channel service message id: %w", err)
	}
	pts, err := s.pts.NextChannelPts(ctx, channel.ID)
	if err != nil {
		return domain.ChannelMessage{}, domain.ChannelUpdateEvent{}, fmt.Errorf("allocate channel service pts: %w", err)
	}
	reserveChannelPts(reserved, channel.ID, pts, 1)
	msg := domain.ChannelMessage{
		ChannelID:    channel.ID,
		ID:           msgID,
		SenderUserID: senderUserID,
		From:         domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
		Date:         date,
		Post:         channel.Broadcast,
		Action:       &action,
		Pts:          pts,
	}
	event := domain.ChannelUpdateEvent{
		ChannelID:    channel.ID,
		Type:         domain.ChannelUpdateNewMessage,
		Pts:          pts,
		PtsCount:     1,
		Date:         date,
		Message:      msg,
		SenderUserID: senderUserID,
		UserIDs:      append([]int64(nil), action.UserIDs...),
	}
	if err := insertChannelMessageTx(ctx, tx, msg); err != nil {
		return domain.ChannelMessage{}, domain.ChannelUpdateEvent{}, err
	}
	if err := insertChannelEventTx(ctx, tx, event); err != nil {
		return domain.ChannelMessage{}, domain.ChannelUpdateEvent{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET top_message_id = $2, pts = $3, updated_at = now() WHERE id = $1`, channel.ID, msgID, pts); err != nil {
		return domain.ChannelMessage{}, domain.ChannelUpdateEvent{}, fmt.Errorf("update channel service top: %w", err)
	}
	return msg, event, nil
}

func (s *ChannelStore) insertParticipantEventTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, actorUserID int64, previous, participant domain.ChannelMember, date int, reserved *[]reservedChannelPts) (domain.ChannelUpdateEvent, domain.Channel, error) {
	pts, err := s.pts.NextChannelPts(ctx, channel.ID)
	if err != nil {
		return domain.ChannelUpdateEvent{}, channel, fmt.Errorf("allocate channel participant pts: %w", err)
	}
	reserveChannelPts(reserved, channel.ID, pts, 1)
	event := domain.ChannelUpdateEvent{
		ChannelID:    channel.ID,
		Type:         domain.ChannelUpdateParticipant,
		Pts:          pts,
		PtsCount:     1,
		Date:         date,
		SenderUserID: actorUserID,
		UserIDs:      uniqueNonZeroInt64s(actorUserID, previous.UserID, previous.InviterUserID, participant.UserID, participant.InviterUserID),
		Previous:     previous,
		Participant:  participant,
	}
	if err := insertChannelEventTx(ctx, tx, event); err != nil {
		return domain.ChannelUpdateEvent{}, channel, err
	}
	if _, err := tx.Exec(ctx, `UPDATE channels SET pts = $2, updated_at = now() WHERE id = $1`, channel.ID, pts); err != nil {
		return domain.ChannelUpdateEvent{}, channel, fmt.Errorf("update channel participant pts: %w", err)
	}
	channel.Pts = pts
	return event, channel, nil
}

func (s *ChannelStore) deleteChannelMessagesTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, member domain.ChannelMember, ids []int, actorUserID int64, date int, reserved *[]reservedChannelPts) ([]int, domain.ChannelUpdateEvent, domain.Channel, error) {
	if len(ids) == 0 {
		return nil, domain.ChannelUpdateEvent{}, channel, nil
	}
	id32, ordered, err := validUniqueChannelMessageIDs(ids)
	if err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, err
	}
	rows, err := tx.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE channel_id = $1 AND id = ANY($2::int[]) AND NOT deleted
ORDER BY id`, channel.ID, id32)
	if err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, fmt.Errorf("list channel messages for delete: %w", err)
	}
	byID := make(map[int]domain.ChannelMessage, len(ordered))
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			rows.Close()
			return nil, domain.ChannelUpdateEvent{}, channel, err
		}
		byID[msg.ID] = msg
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, domain.ChannelUpdateEvent{}, channel, err
	}
	rows.Close()
	deleted := make([]int, 0, len(ordered))
	for _, id := range ordered {
		msg, ok := byID[id]
		if !ok {
			continue
		}
		if msg.SenderUserID != actorUserID && !canDeleteAnyChannelMessage(member) {
			return nil, domain.ChannelUpdateEvent{}, channel, domain.ErrChannelAdminRequired
		}
		deleted = append(deleted, id)
		if err := s.insertChannelAdminLogTx(ctx, tx, domain.ChannelAdminLogEvent{
			ChannelID: channel.ID,
			UserID:    actorUserID,
			Date:      date,
			Type:      domain.ChannelAdminLogDeleteMessage,
			Message:   &msg,
			Query:     msg.Body,
		}); err != nil {
			return nil, domain.ChannelUpdateEvent{}, channel, err
		}
	}
	if len(deleted) == 0 {
		return nil, domain.ChannelUpdateEvent{}, channel, nil
	}
	pts, err := s.nextChannelPtsN(ctx, channel.ID, len(deleted))
	if err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, fmt.Errorf("allocate channel delete pts: %w", err)
	}
	reserveChannelPts(reserved, channel.ID, pts, len(deleted))
	deleted32 := int32s(deleted)
	if _, err := tx.Exec(ctx, `
UPDATE channel_messages
SET deleted = true, pts = $3, updated_at = now()
WHERE channel_id = $1 AND id = ANY($2::int[])`, channel.ID, deleted32, pts); err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, fmt.Errorf("soft delete channel messages: %w", err)
	}
	if err := deleteChannelUnreadMentionsTx(ctx, tx, channel.ID, deleted); err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, err
	}
	if err := refreshChannelUnreadReactionsCountsForMessagesTx(ctx, tx, channel.ID, deleted); err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, err
	}
	topID, err := topNonDeletedChannelMessageID(ctx, tx, channel.ID)
	if err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, err
	}
	if _, err := tx.Exec(ctx, `
UPDATE channels
SET top_message_id = $2, pts = $3, updated_at = now()
WHERE id = $1`, channel.ID, topID, pts); err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, fmt.Errorf("update channel top after delete: %w", err)
	}
	channel.TopMessageID = topID
	channel.Pts = pts
	event := domain.ChannelUpdateEvent{
		ChannelID:    channel.ID,
		Type:         domain.ChannelUpdateDeleteMessages,
		Pts:          pts,
		PtsCount:     len(deleted),
		Date:         date,
		MessageIDs:   append([]int(nil), deleted...),
		SenderUserID: actorUserID,
	}
	if err := insertChannelEventTx(ctx, tx, event); err != nil {
		return nil, domain.ChannelUpdateEvent{}, channel, err
	}
	return deleted, event, channel, nil
}

func (s *ChannelStore) nextChannelPtsN(ctx context.Context, channelID int64, count int) (int, error) {
	if count <= 1 {
		return s.pts.NextChannelPts(ctx, channelID)
	}
	if ranges, ok := s.pts.(store.ChannelPtsRangeAllocator); ok {
		return ranges.NextChannelPtsN(ctx, channelID, count)
	}
	var pts int
	var err error
	for i := 0; i < count; i++ {
		pts, err = s.pts.NextChannelPts(ctx, channelID)
		if err != nil {
			return 0, err
		}
	}
	return pts, nil
}

type reservedChannelPts struct {
	channelID int64
	pts       int
	count     int
}

func reserveChannelPts(items *[]reservedChannelPts, channelID int64, pts, count int) {
	if items == nil || channelID == 0 || pts == 0 {
		return
	}
	if count <= 0 {
		count = 1
	}
	*items = append(*items, reservedChannelPts{channelID: channelID, pts: pts, count: count})
}

func (s *ChannelStore) recordChannelPtsGaps(ctx context.Context, items []reservedChannelPts, date int) {
	if len(items) == 0 {
		return
	}
	if date == 0 {
		date = nowUnix()
	}
	for _, item := range items {
		count := item.count
		if count <= 0 {
			count = 1
		}
		_, _ = s.db.Exec(ctx, `
INSERT INTO channel_update_events (
    channel_id, pts, pts_count, date, event_type, message_id, message_ids, sender_user_id, user_ids, payload
) VALUES ($1,$2,$3,$4,$5,0,'[]'::jsonb,0,'[]'::jsonb,'{}'::jsonb)
ON CONFLICT (channel_id, pts) DO NOTHING`,
			item.channelID, item.pts, count, date, string(domain.ChannelUpdateNoop))
	}
}

func validUniqueChannelMessageIDs(ids []int) ([]int32, []int, error) {
	seen := make(map[int]struct{}, len(ids))
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return nil, nil, domain.ErrMessageIDInvalid
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return int32s(out), out, nil
}

func topNonDeletedChannelMessageID(ctx context.Context, db sqlcgen.DBTX, channelID int64) (int, error) {
	var id int
	if err := db.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM channel_messages WHERE channel_id = $1 AND NOT deleted`, channelID).Scan(&id); err != nil {
		return 0, fmt.Errorf("select channel top after delete: %w", err)
	}
	return id, nil
}

func visibleChannelTopAfter(ctx context.Context, db sqlcgen.DBTX, channelID int64, availableMinID int, fallbackDate int) (int, int, error) {
	var id, date int
	err := db.QueryRow(ctx, `
SELECT id, message_date
FROM channel_messages
WHERE channel_id = $1 AND id > $2 AND NOT deleted
ORDER BY id DESC
LIMIT 1`, channelID, availableMinID).Scan(&id, &date)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fallbackDate, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("select visible channel top: %w", err)
	}
	return id, date, nil
}

func insertChannelTx(ctx context.Context, tx pgx.Tx, ch domain.Channel) error {
	rights, err := marshalJSON(ch.DefaultBannedRights, "{}")
	if err != nil {
		return err
	}
	reactions, err := marshalJSON(ch.ReactionPolicy, "{}")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO channels (
    id, access_hash, creator_user_id, title, about, username, broadcast, megagroup, forum, forum_tabs,
    autotranslation, restricted_sponsored, broadcast_messages_allowed, send_paid_messages_stars,
    noforwards, join_to_send, join_request, signatures, pre_history_hidden, participants_hidden, antispam, linked_chat_id, slowmode_seconds, default_banned_rights, available_reactions,
    color_set, color, color_background_emoji_id, profile_color_set, profile_color, profile_color_background_emoji_id, emoji_status_document_id, emoji_status_until,
    participants_count, admins_count, kicked_count, banned_count, top_message_id, pinned_message_id, pts, ttl_period, date, deleted
) VALUES ($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43)`,
		ch.ID, ch.AccessHash, ch.CreatorUserID, ch.Title, ch.About, ch.Username, ch.Broadcast, ch.Megagroup, ch.Forum,
		ch.ForumTabs, ch.Autotranslation, ch.RestrictedSponsored, ch.BroadcastMessagesAllowed, ch.SendPaidMessagesStars, ch.NoForwards, ch.JoinToSend, ch.JoinRequest, ch.Signatures, ch.PreHistoryHidden, ch.ParticipantsHidden, ch.AntiSpam, ch.LinkedChatID, ch.SlowmodeSeconds, rights, reactions,
		ch.Color.HasColor, ch.Color.Color, ch.Color.BackgroundEmojiID, ch.ProfileColor.HasColor, ch.ProfileColor.Color, ch.ProfileColor.BackgroundEmojiID, ch.EmojiStatus.DocumentID, ch.EmojiStatus.Until,
		ch.ParticipantsCount, ch.AdminsCount,
		ch.KickedCount, ch.BannedCount, ch.TopMessageID, ch.PinnedMessageID, ch.Pts, ch.TTLPeriod, ch.Date, ch.Deleted); err != nil {
		return fmt.Errorf("insert channel: %w", err)
	}
	return nil
}

func upsertChannelMemberTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, member domain.ChannelMember) error {
	adminRights, err := marshalJSON(member.AdminRights, "{}")
	if err != nil {
		return err
	}
	bannedRights, err := marshalJSON(member.BannedRights, "{}")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_members (
    channel_id, user_id, inviter_user_id, role, status, joined_at, left_at, admin_rights, banned_rights,
    rank, available_min_id, available_min_pts, read_inbox_max_id, read_outbox_max_id, unread_mark, slowmode_last_send_date
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT (channel_id, user_id) DO UPDATE SET
    inviter_user_id = EXCLUDED.inviter_user_id,
    role = EXCLUDED.role,
    status = EXCLUDED.status,
    joined_at = EXCLUDED.joined_at,
    left_at = EXCLUDED.left_at,
    admin_rights = EXCLUDED.admin_rights,
    banned_rights = EXCLUDED.banned_rights,
    rank = EXCLUDED.rank,
    available_min_id = GREATEST(channel_members.available_min_id, EXCLUDED.available_min_id),
    available_min_pts = GREATEST(channel_members.available_min_pts, EXCLUDED.available_min_pts),
    read_inbox_max_id = GREATEST(channel_members.read_inbox_max_id, EXCLUDED.read_inbox_max_id),
    updated_at = now()`,
		member.ChannelID, member.UserID, member.InviterUserID, string(member.Role), string(member.Status),
		member.JoinedAt, member.LeftAt, adminRights, bannedRights, member.Rank, member.AvailableMinID,
		member.AvailableMinPts, member.ReadInboxMaxID, member.ReadOutboxMaxID, member.UnreadMark, member.SlowmodeLastSendDate); err != nil {
		return fmt.Errorf("upsert channel member: %w", err)
	}
	return upsertUserChannelMemberIndexTx(ctx, tx, channel, member)
}

func upsertUserChannelMemberIndexTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, member domain.ChannelMember) error {
	if channel.ID == 0 || member.UserID == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO user_channel_member_index (
    user_id, channel_id, status, megagroup, broadcast, deleted
) VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    status = EXCLUDED.status,
    megagroup = EXCLUDED.megagroup,
    broadcast = EXCLUDED.broadcast,
    deleted = EXCLUDED.deleted,
    updated_at = now()`,
		member.UserID, channel.ID, string(member.Status), channel.Megagroup, channel.Broadcast, channel.Deleted); err != nil {
		return fmt.Errorf("upsert user channel member index: %w", err)
	}
	return nil
}

func markUserChannelMemberIndexDeletedTx(ctx context.Context, tx pgx.Tx, channelID int64, deleted bool) error {
	if channelID == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE user_channel_member_index
SET deleted = $2, updated_at = now()
WHERE channel_id = $1`, channelID, deleted); err != nil {
		return fmt.Errorf("mark user channel member index deleted: %w", err)
	}
	return nil
}

func insertChannelMessageTx(ctx context.Context, tx pgx.Tx, msg domain.ChannelMessage) error {
	entities, err := encodeMessageEntities(msg.Entities)
	if err != nil {
		return err
	}
	reply, err := marshalJSON(msg.ReplyTo, "{}")
	if err != nil {
		return err
	}
	forward, err := marshalJSON(msg.Forward, "{}")
	if err != nil {
		return err
	}
	action, err := marshalJSON(msg.Action, "{}")
	if err != nil {
		return err
	}
	media, err := encodeMessageMedia(msg.Media)
	if err != nil {
		return err
	}
	var sendAsType sql.NullString
	var sendAsID sql.NullInt64
	if msg.SendAs != nil && msg.SendAs.ID != 0 {
		sendAsType = sql.NullString{String: string(msg.SendAs.Type), Valid: true}
		sendAsID = sql.NullInt64{Int64: msg.SendAs.ID, Valid: true}
	}
	if msg.From.Type == "" {
		msg.From = domain.Peer{Type: domain.PeerTypeUser, ID: msg.SenderUserID}
	}
	replyMsgID, replyTopID := 0, 0
	replyPeerType := ""
	replyPeerID := int64(0)
	if msg.ReplyTo != nil {
		replyMsgID = msg.ReplyTo.MessageID
		replyTopID = msg.ReplyTo.TopMessageID
		replyPeerType = string(msg.ReplyTo.Peer.Type)
		replyPeerID = msg.ReplyTo.Peer.ID
	}
	discussionChannelID, discussionMessageID := int64(0), 0
	if msg.Discussion != nil {
		discussionChannelID = msg.Discussion.ChannelID
		discussionMessageID = msg.Discussion.MessageID
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_messages (
    channel_id, id, random_id, sender_user_id, from_peer_type, from_peer_id,
    send_as_peer_type, send_as_peer_id, message_date, edit_date, post, silent, noforwards,
    body, entities, reply_to, reply_to_msg_id, reply_to_peer_type, reply_to_peer_id, reply_to_top_id,
    fwd_from, discussion_channel_id, discussion_message_id, action, pts, deleted, media
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)`,
		msg.ChannelID, msg.ID, msg.RandomID, msg.SenderUserID, string(msg.From.Type), msg.From.ID,
		sendAsType, sendAsID, msg.Date, msg.EditDate, msg.Post, msg.Silent, msg.NoForwards,
		msg.Body, entities, reply, replyMsgID, replyPeerType, replyPeerID, replyTopID,
		forward, discussionChannelID, discussionMessageID, action, msg.Pts, msg.Deleted, media); err != nil {
		return fmt.Errorf("insert channel message: %w", err)
	}
	return nil
}

func updateForumTopicTopMessageTx(ctx context.Context, tx pgx.Tx, channelID int64, msg domain.ChannelMessage) error {
	if msg.ReplyTo == nil || !msg.ReplyTo.ForumTopic || msg.ReplyTo.TopMessageID <= 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_forum_topics
SET top_message_id = $3,
    date = $4,
    updated_at = now()
WHERE channel_id = $1 AND topic_id = $2 AND NOT deleted`,
		channelID, msg.ReplyTo.TopMessageID, msg.ID, msg.Date); err != nil {
		return fmt.Errorf("update forum topic top message: %w", err)
	}
	return nil
}

func insertChannelEventTx(ctx context.Context, tx pgx.Tx, event domain.ChannelUpdateEvent) error {
	ids, err := marshalJSON(event.MessageIDs, "[]")
	if err != nil {
		return err
	}
	userIDs, err := marshalJSON(event.UserIDs, "[]")
	if err != nil {
		return err
	}
	payloadData := map[string]any{
		"message_id": event.Message.ID,
		"pinned":     event.Pinned,
	}
	if event.Message.ID != 0 {
		payloadData["message"] = event.Message
	}
	if event.Previous.UserID != 0 {
		payloadData["previous_participant"] = event.Previous
	}
	if event.Participant.UserID != 0 {
		payloadData["participant"] = event.Participant
	}
	payload, err := marshalJSON(payloadData, "{}")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_update_events (
    channel_id, pts, pts_count, date, event_type, message_id, message_ids, sender_user_id, user_ids, payload
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		event.ChannelID, event.Pts, event.PtsCount, event.Date, string(event.Type), event.Message.ID,
		ids, event.SenderUserID, userIDs, payload); err != nil {
		return fmt.Errorf("insert channel event: %w", err)
	}
	return nil
}

func (s *ChannelStore) insertChannelAdminLogTx(ctx context.Context, tx pgx.Tx, event domain.ChannelAdminLogEvent) error {
	if event.ChannelID == 0 || event.UserID == 0 || event.Type == "" {
		return nil
	}
	if event.Date == 0 {
		event.Date = nowUnix()
	}
	id, err := nextChannelAdminLogIDTx(ctx, tx, event.ChannelID)
	if err != nil {
		return err
	}
	prevParticipant, err := marshalJSON(event.PrevParticipant, "{}")
	if err != nil {
		return err
	}
	newParticipant, err := marshalJSON(event.NewParticipant, "{}")
	if err != nil {
		return err
	}
	participant, err := marshalJSON(event.Participant, "{}")
	if err != nil {
		return err
	}
	message, err := marshalJSON(event.Message, "{}")
	if err != nil {
		return err
	}
	prevMessage, err := marshalJSON(event.PrevMessage, "{}")
	if err != nil {
		return err
	}
	newMessage, err := marshalJSON(event.NewMessage, "{}")
	if err != nil {
		return err
	}
	query := adminLogSearchText(event)
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_admin_log_events (
    channel_id, id, actor_user_id, event_date, event_type, prev_string, new_string,
    prev_bool, new_bool, prev_int, new_int, prev_participant, new_participant,
    participant, message, prev_message, new_message, query
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		event.ChannelID, id, event.UserID, event.Date, string(event.Type), event.PrevString, event.NewString,
		event.PrevBool, event.NewBool, event.PrevInt, event.NewInt, prevParticipant, newParticipant,
		participant, message, prevMessage, newMessage, query); err != nil {
		return fmt.Errorf("insert channel admin log: %w", err)
	}
	return nil
}

func nextChannelAdminLogIDTx(ctx context.Context, tx pgx.Tx, channelID int64) (int64, error) {
	var id int64
	if err := tx.QueryRow(ctx, `
UPDATE channels
SET admin_log_seq = admin_log_seq + 1, updated_at = now()
WHERE id = $1
RETURNING admin_log_seq`, channelID).Scan(&id); err != nil {
		return 0, fmt.Errorf("allocate channel admin log id: %w", err)
	}
	return id, nil
}

func upsertChannelDialogTx(ctx context.Context, tx pgx.Tx, userID int64, channel domain.Channel, top domain.ChannelMessage, readInboxMaxID, readOutboxMaxID int) error {
	topDate := top.Date
	if topDate == 0 {
		topDate = channel.Date
	}
	unread, err := countChannelUnreadMessages(ctx, tx, userID, channel.ID, readInboxMaxID, channel.TopMessageID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_dialogs (
    user_id, channel_id, top_message_id, top_message_date, read_inbox_max_id, read_outbox_max_id, unread_count, unread_mark
) VALUES ($1,$2,$3,$4,$5,$6,$7,false)
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    top_message_id = GREATEST(channel_dialogs.top_message_id, EXCLUDED.top_message_id),
    top_message_date = GREATEST(channel_dialogs.top_message_date, EXCLUDED.top_message_date),
    read_inbox_max_id = GREATEST(channel_dialogs.read_inbox_max_id, EXCLUDED.read_inbox_max_id),
    read_outbox_max_id = GREATEST(channel_dialogs.read_outbox_max_id, EXCLUDED.read_outbox_max_id),
    unread_count = (
        SELECT COUNT(*)::int
        FROM channel_messages msg
        WHERE msg.channel_id = channel_dialogs.channel_id
          AND msg.id > GREATEST(channel_dialogs.read_inbox_max_id, EXCLUDED.read_inbox_max_id)
          AND msg.id <= GREATEST(channel_dialogs.top_message_id, EXCLUDED.top_message_id)
          AND NOT msg.deleted
          AND msg.sender_user_id <> channel_dialogs.user_id
    ),
    unread_mark = false,
    updated_at = now()`,
		userID, channel.ID, channel.TopMessageID, topDate, readInboxMaxID, readOutboxMaxID, unread); err != nil {
		return fmt.Errorf("upsert channel dialog: %w", err)
	}
	return nil
}

func upsertChannelDialogsForMessageTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, top domain.ChannelMessage, selfReadUserID int64) error {
	if channel.ID == 0 || top.ID == 0 {
		return nil
	}
	if !shouldSynchronouslyUpsertChannelDialogs(channel) {
		return nil
	}
	topDate := top.Date
	if topDate == 0 {
		topDate = channel.Date
	}
	if _, err := tx.Exec(ctx, `
WITH active AS (
    SELECT
        m.user_id,
        CASE
            WHEN m.user_id = $4 THEN GREATEST(m.read_inbox_max_id, $2)
            ELSE m.read_inbox_max_id
        END AS read_inbox_max_id,
        CASE
            WHEN m.user_id = $4 THEN GREATEST(m.read_outbox_max_id, $2)
            ELSE m.read_outbox_max_id
        END AS read_outbox_max_id
    FROM channel_members m
    WHERE m.channel_id = $1
      AND m.status = 'active'
      AND NOT COALESCE((m.banned_rights->>'ViewMessages')::boolean, false)
      AND $2 > m.available_min_id
)
INSERT INTO channel_dialogs (
    user_id, channel_id, top_message_id, top_message_date,
    read_inbox_max_id, read_outbox_max_id, unread_count, unread_mark
)
SELECT
    user_id, $1, $2, $3,
    read_inbox_max_id, read_outbox_max_id,
    (
        SELECT COUNT(*)::int
        FROM channel_messages msg
        WHERE msg.channel_id = $1
          AND msg.id > active.read_inbox_max_id
          AND msg.id <= $2
          AND NOT msg.deleted
          AND msg.sender_user_id <> active.user_id
    ),
    false
FROM active
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    top_message_id = GREATEST(channel_dialogs.top_message_id, EXCLUDED.top_message_id),
    top_message_date = GREATEST(channel_dialogs.top_message_date, EXCLUDED.top_message_date),
    read_inbox_max_id = GREATEST(channel_dialogs.read_inbox_max_id, EXCLUDED.read_inbox_max_id),
    read_outbox_max_id = GREATEST(channel_dialogs.read_outbox_max_id, EXCLUDED.read_outbox_max_id),
    unread_count = (
        SELECT COUNT(*)::int
        FROM channel_messages msg
        WHERE msg.channel_id = channel_dialogs.channel_id
          AND msg.id > GREATEST(channel_dialogs.read_inbox_max_id, EXCLUDED.read_inbox_max_id)
          AND msg.id <= GREATEST(channel_dialogs.top_message_id, EXCLUDED.top_message_id)
          AND NOT msg.deleted
          AND msg.sender_user_id <> channel_dialogs.user_id
    ),
    unread_mark = CASE WHEN channel_dialogs.user_id = $4 THEN false ELSE channel_dialogs.unread_mark END,
    updated_at = now()`, channel.ID, top.ID, topDate, selfReadUserID); err != nil {
		return fmt.Errorf("upsert channel message dialogs: %w", err)
	}
	return nil
}

func shouldSynchronouslyUpsertChannelDialogs(channel domain.Channel) bool {
	if channel.Broadcast {
		return false
	}
	return channel.ParticipantsCount > 0 && channel.ParticipantsCount <= domain.MaxSynchronousChannelDialogFanout
}

func insertChannelUnreadMentionsTx(ctx context.Context, tx pgx.Tx, channelID int64, msg domain.ChannelMessage, senderUserID int64, userIDs []int64) error {
	candidates := uniqueChannelUserIDs(userIDs, senderUserID)
	if len(candidates) == 0 || msg.ID == 0 {
		return nil
	}
	if len(candidates) > domain.MaxChannelMentionRecipients {
		candidates = candidates[:domain.MaxChannelMentionRecipients]
	}
	topID := channelMentionTopID(msg)
	if _, err := tx.Exec(ctx, `
WITH input(user_id) AS (
    SELECT DISTINCT unnest($4::bigint[])
),
active AS (
    SELECT i.user_id
    FROM input i
    JOIN channel_members m ON m.channel_id = $1 AND m.user_id = i.user_id
    WHERE m.status = 'active'
      AND NOT COALESCE((m.banned_rights->>'ViewMessages')::boolean, false)
      AND $2 > m.available_min_id
      AND $2 > m.read_inbox_max_id
    LIMIT $6
),
inserted AS (
    INSERT INTO channel_unread_mentions (user_id, channel_id, message_id, top_message_id)
    SELECT user_id, $1, $2, $3
    FROM active
    ON CONFLICT DO NOTHING
    RETURNING user_id
)
INSERT INTO channel_dialogs (
    user_id, channel_id, top_message_id, top_message_date, unread_mentions_count
)
SELECT user_id, $1, $2, $5, 1
FROM inserted
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    top_message_id = GREATEST(channel_dialogs.top_message_id, EXCLUDED.top_message_id),
    top_message_date = GREATEST(channel_dialogs.top_message_date, EXCLUDED.top_message_date),
    unread_mentions_count = channel_dialogs.unread_mentions_count + 1,
    updated_at = now()`, channelID, msg.ID, topID, candidates, msg.Date, domain.MaxChannelMentionRecipients); err != nil {
		return fmt.Errorf("insert channel unread mentions: %w", err)
	}
	return nil
}

func readChannelMentionsTx(ctx context.Context, tx pgx.Tx, userID, channelID int64, topMsgID, limit int) (int, int, error) {
	var cleared, remaining int
	if err := tx.QueryRow(ctx, `
WITH target AS (
    SELECT user_id, channel_id, message_id
    FROM channel_unread_mentions
    WHERE user_id = $1
      AND channel_id = $2
      AND ($3 = 0 OR top_message_id = $3)
    ORDER BY message_id DESC
    LIMIT $4
),
deleted AS (
    DELETE FROM channel_unread_mentions um
    USING target t
    WHERE um.user_id = t.user_id
      AND um.channel_id = t.channel_id
      AND um.message_id = t.message_id
    RETURNING um.message_id
),
remaining_scoped AS (
    SELECT COUNT(*)::int AS count
    FROM channel_unread_mentions
    WHERE user_id = $1
      AND channel_id = $2
      AND ($3 = 0 OR top_message_id = $3)
),
remaining_all AS (
    SELECT COUNT(*)::int AS count
    FROM channel_unread_mentions
    WHERE user_id = $1 AND channel_id = $2
),
updated_dialog AS (
    UPDATE channel_dialogs
    SET unread_mentions_count = (SELECT count FROM remaining_all),
        updated_at = now()
    WHERE user_id = $1 AND channel_id = $2
)
SELECT (SELECT COUNT(*)::int FROM deleted), (SELECT count FROM remaining_scoped)`, userID, channelID, topMsgID, limit).Scan(&cleared, &remaining); err != nil {
		return 0, 0, fmt.Errorf("read channel mentions: %w", err)
	}
	return cleared, remaining, nil
}

func readChannelReactionsTx(ctx context.Context, tx pgx.Tx, userID, channelID int64, topMsgID, limit int) (int, int, error) {
	var cleared, remaining int
	if err := tx.QueryRow(ctx, `
WITH member_scope AS (
    SELECT available_min_id
    FROM channel_members
    WHERE user_id = $1 AND channel_id = $2
),
target_messages AS (
    SELECT DISTINCT r.message_id
    FROM channel_message_reactions r
    JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
    JOIN member_scope ms ON true
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.unread
      AND r.reacted_user_id <> $1
      AND cm.id > ms.available_min_id
      AND NOT cm.deleted
      AND ($3 = 0 OR cm.id = $3 OR COALESCE(NULLIF(cm.reply_to_top_id, 0), NULLIF(cm.reply_to_msg_id, 0), 0) = $3)
    ORDER BY r.message_id DESC
    LIMIT $4
),
updated AS (
    UPDATE channel_message_reactions r
    SET unread = false,
        updated_at = now()
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.message_id IN (SELECT message_id FROM target_messages)
      AND r.unread
    RETURNING r.message_id
),
remaining_scoped AS (
    SELECT COUNT(DISTINCT r.message_id)::int AS count
    FROM channel_message_reactions r
    JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
    JOIN member_scope ms ON true
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.unread
      AND r.reacted_user_id <> $1
      AND cm.id > ms.available_min_id
      AND NOT cm.deleted
      AND ($3 = 0 OR cm.id = $3 OR COALESCE(NULLIF(cm.reply_to_top_id, 0), NULLIF(cm.reply_to_msg_id, 0), 0) = $3)
),
remaining_all AS (
    SELECT COUNT(DISTINCT r.message_id)::int AS count
    FROM channel_message_reactions r
    JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
    JOIN member_scope ms ON true
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.unread
      AND r.reacted_user_id <> $1
      AND cm.id > ms.available_min_id
      AND NOT cm.deleted
),
updated_dialog AS (
    UPDATE channel_dialogs
    SET unread_reactions_count = (SELECT count FROM remaining_all),
        updated_at = now()
    WHERE user_id = $1 AND channel_id = $2
)
SELECT (SELECT COUNT(DISTINCT message_id)::int FROM updated), (SELECT count FROM remaining_scoped)`, userID, channelID, topMsgID, limit).Scan(&cleared, &remaining); err != nil {
		return 0, 0, fmt.Errorf("read channel reactions: %w", err)
	}
	return cleared, remaining, nil
}

func clearChannelUnreadReactionsForMessageIDsTx(ctx context.Context, tx pgx.Tx, userID, channelID int64, ids []int32) ([]int, error) {
	if userID == 0 || channelID == 0 || len(ids) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `
UPDATE channel_message_reactions
SET unread = false,
    updated_at = now()
WHERE sender_user_id = $1
  AND channel_id = $2
  AND message_id = ANY($3::int[])
  AND unread
  AND reacted_user_id <> $1
RETURNING message_id`, userID, channelID, ids)
	if err != nil {
		return nil, fmt.Errorf("clear visible channel unread reactions: %w", err)
	}
	clearedSet := make(map[int]struct{})
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		clearedSet[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(clearedSet) == 0 {
		return nil, nil
	}
	cleared := make([]int, 0, len(clearedSet))
	for id := range clearedSet {
		cleared = append(cleared, id)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(cleared)))
	if err := refreshChannelUnreadReactionsCountTx(ctx, tx, userID, channelID); err != nil {
		return nil, err
	}
	return cleared, nil
}

func refreshChannelUnreadReactionsCountTx(ctx context.Context, tx pgx.Tx, userID, channelID int64) error {
	if userID == 0 || channelID == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
WITH active AS (
    SELECT m.available_min_id
    FROM channel_members m
    WHERE m.user_id = $1
      AND m.channel_id = $2
      AND m.status = 'active'
      AND NOT COALESCE((m.banned_rights->>'ViewMessages')::boolean, false)
),
counts AS (
    SELECT COUNT(DISTINCT r.message_id)::int AS count
    FROM channel_message_reactions r
    JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
    JOIN active a ON true
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.unread
      AND r.reacted_user_id <> $1
      AND cm.id > a.available_min_id
      AND NOT cm.deleted
)
INSERT INTO channel_dialogs (user_id, channel_id, unread_reactions_count)
SELECT $1, $2, counts.count
FROM active, counts
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    unread_reactions_count = EXCLUDED.unread_reactions_count,
    updated_at = now()`, userID, channelID); err != nil {
		return fmt.Errorf("refresh channel unread reactions count: %w", err)
	}
	return nil
}

func deleteChannelUnreadMentionsTx(ctx context.Context, tx pgx.Tx, channelID int64, ids []int) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
WITH deleted AS (
    DELETE FROM channel_unread_mentions
    WHERE channel_id = $1 AND message_id = ANY($2::int[])
    RETURNING user_id
),
affected AS (
    SELECT DISTINCT user_id FROM deleted
),
counts AS (
    SELECT user_id, COUNT(*)::int AS count
    FROM channel_unread_mentions
    WHERE channel_id = $1
      AND user_id IN (SELECT user_id FROM affected)
    GROUP BY user_id
)
UPDATE channel_dialogs d
SET unread_mentions_count = COALESCE(c.count, 0),
    updated_at = now()
FROM affected a
LEFT JOIN counts c ON c.user_id = a.user_id
WHERE d.channel_id = $1 AND d.user_id = a.user_id`, channelID, int32s(ids)); err != nil {
		return fmt.Errorf("delete channel unread mentions: %w", err)
	}
	return nil
}

func refreshChannelUnreadReactionsCountsForMessagesTx(ctx context.Context, tx pgx.Tx, channelID int64, ids []int) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `
SELECT DISTINCT sender_user_id
FROM channel_message_reactions
WHERE channel_id = $1
  AND message_id = ANY($2::int[])
  AND sender_user_id <> 0`, channelID, int32s(ids))
	if err != nil {
		return fmt.Errorf("list channel unread reaction owners: %w", err)
	}
	defer rows.Close()
	userIDs := make([]int64, 0)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return err
		}
		userIDs = append(userIDs, userID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, userID := range userIDs {
		if err := refreshChannelUnreadReactionsCountTx(ctx, tx, userID, channelID); err != nil {
			return err
		}
	}
	return nil
}

func deleteChannelUnreadMentionsUpToTx(ctx context.Context, tx pgx.Tx, userID, channelID int64, maxID int) error {
	if maxID <= 0 {
		return nil
	}
	var deleted int
	if err := tx.QueryRow(ctx, `
WITH deleted AS (
    DELETE FROM channel_unread_mentions
    WHERE user_id = $1 AND channel_id = $2 AND message_id <= $3
    RETURNING message_id
),
remaining_all AS (
    SELECT COUNT(*)::int AS count
    FROM channel_unread_mentions
    WHERE user_id = $1 AND channel_id = $2
),
updated_dialog AS (
    UPDATE channel_dialogs
    SET unread_mentions_count = (SELECT count FROM remaining_all),
        updated_at = now()
    WHERE user_id = $1 AND channel_id = $2
)
SELECT COUNT(*)::int FROM deleted`, userID, channelID, maxID).Scan(&deleted); err != nil {
		return fmt.Errorf("delete channel unread mentions up to: %w", err)
	}
	return nil
}

func channelMentionTopID(msg domain.ChannelMessage) int {
	if msg.ReplyTo == nil {
		return 0
	}
	if msg.ReplyTo.TopMessageID > 0 {
		return msg.ReplyTo.TopMessageID
	}
	return msg.ReplyTo.MessageID
}

func scanChannelWithMember(row rowScanner) (domain.Channel, domain.ChannelMember, error) {
	var ch domain.Channel
	var member domain.ChannelMember
	var defaultRights, reactionPolicy, adminRights, bannedRights string
	var role, status string
	if err := row.Scan(
		&ch.ID, &ch.AccessHash, &ch.CreatorUserID, &ch.Title, &ch.About, &ch.Username,
		&ch.Broadcast, &ch.Megagroup, &ch.Forum, &ch.ForumTabs, &ch.Autotranslation, &ch.RestrictedSponsored, &ch.BroadcastMessagesAllowed, &ch.SendPaidMessagesStars, &ch.NoForwards, &ch.JoinToSend, &ch.JoinRequest, &ch.Signatures, &ch.PreHistoryHidden, &ch.ParticipantsHidden, &ch.AntiSpam, &ch.LinkedChatID, &ch.SlowmodeSeconds, &defaultRights,
		&reactionPolicy, &ch.Color.HasColor, &ch.Color.Color, &ch.Color.BackgroundEmojiID, &ch.ProfileColor.HasColor, &ch.ProfileColor.Color, &ch.ProfileColor.BackgroundEmojiID, &ch.EmojiStatus.DocumentID, &ch.EmojiStatus.Until,
		&ch.ParticipantsCount, &ch.AdminsCount, &ch.KickedCount, &ch.BannedCount, &ch.TopMessageID,
		&ch.PinnedMessageID, &ch.Pts, &ch.TTLPeriod, &ch.Date, &ch.Deleted,
		&ch.PhotoID, &ch.PhotoDCID, &ch.PhotoStripped,
		&member.ChannelID, &member.UserID, &member.InviterUserID, &role, &status,
		&member.JoinedAt, &member.LeftAt, &adminRights, &bannedRights, &member.Rank,
		&member.AvailableMinID, &member.AvailableMinPts, &member.ReadInboxMaxID, &member.ReadOutboxMaxID, &member.UnreadMark, &member.SlowmodeLastSendDate,
	); err != nil {
		return domain.Channel{}, domain.ChannelMember{}, err
	}
	member.Role = domain.ChannelMemberRole(role)
	member.Status = domain.ChannelMemberStatus(status)
	_ = json.Unmarshal([]byte(defaultRights), &ch.DefaultBannedRights)
	_ = json.Unmarshal([]byte(reactionPolicy), &ch.ReactionPolicy)
	_ = json.Unmarshal([]byte(adminRights), &member.AdminRights)
	_ = json.Unmarshal([]byte(bannedRights), &member.BannedRights)
	return ch, member, nil
}

func scanChannel(row rowScanner) (domain.Channel, error) {
	var ch domain.Channel
	var rights, reactionPolicy string
	if err := row.Scan(
		&ch.ID, &ch.AccessHash, &ch.CreatorUserID, &ch.Title, &ch.About, &ch.Username,
		&ch.Broadcast, &ch.Megagroup, &ch.Forum, &ch.ForumTabs, &ch.Autotranslation, &ch.RestrictedSponsored, &ch.BroadcastMessagesAllowed, &ch.SendPaidMessagesStars, &ch.NoForwards, &ch.JoinToSend, &ch.JoinRequest, &ch.Signatures, &ch.PreHistoryHidden, &ch.ParticipantsHidden, &ch.AntiSpam, &ch.LinkedChatID, &ch.SlowmodeSeconds, &rights,
		&reactionPolicy, &ch.Color.HasColor, &ch.Color.Color, &ch.Color.BackgroundEmojiID, &ch.ProfileColor.HasColor, &ch.ProfileColor.Color, &ch.ProfileColor.BackgroundEmojiID, &ch.EmojiStatus.DocumentID, &ch.EmojiStatus.Until,
		&ch.ParticipantsCount, &ch.AdminsCount, &ch.KickedCount, &ch.BannedCount, &ch.TopMessageID,
		&ch.PinnedMessageID, &ch.Pts, &ch.TTLPeriod, &ch.Date, &ch.Deleted,
		&ch.PhotoID, &ch.PhotoDCID, &ch.PhotoStripped,
	); err != nil {
		return domain.Channel{}, err
	}
	_ = json.Unmarshal([]byte(rights), &ch.DefaultBannedRights)
	_ = json.Unmarshal([]byte(reactionPolicy), &ch.ReactionPolicy)
	return ch, nil
}

func scanChannelWithViewerMember(row rowScanner) (domain.Channel, bool, error) {
	var ch domain.Channel
	var viewerMember bool
	var rights, reactionPolicy string
	if err := row.Scan(
		&ch.ID, &ch.AccessHash, &ch.CreatorUserID, &ch.Title, &ch.About, &ch.Username,
		&ch.Broadcast, &ch.Megagroup, &ch.Forum, &ch.ForumTabs, &ch.Autotranslation, &ch.RestrictedSponsored, &ch.BroadcastMessagesAllowed, &ch.SendPaidMessagesStars, &ch.NoForwards, &ch.JoinToSend, &ch.JoinRequest, &ch.Signatures, &ch.PreHistoryHidden, &ch.ParticipantsHidden, &ch.AntiSpam, &ch.LinkedChatID, &ch.SlowmodeSeconds, &rights,
		&reactionPolicy, &ch.Color.HasColor, &ch.Color.Color, &ch.Color.BackgroundEmojiID, &ch.ProfileColor.HasColor, &ch.ProfileColor.Color, &ch.ProfileColor.BackgroundEmojiID, &ch.EmojiStatus.DocumentID, &ch.EmojiStatus.Until,
		&ch.ParticipantsCount, &ch.AdminsCount, &ch.KickedCount, &ch.BannedCount, &ch.TopMessageID,
		&ch.PinnedMessageID, &ch.Pts, &ch.TTLPeriod, &ch.Date, &ch.Deleted,
		&ch.PhotoID, &ch.PhotoDCID, &ch.PhotoStripped,
		&viewerMember,
	); err != nil {
		return domain.Channel{}, false, err
	}
	_ = json.Unmarshal([]byte(rights), &ch.DefaultBannedRights)
	_ = json.Unmarshal([]byte(reactionPolicy), &ch.ReactionPolicy)
	return ch, viewerMember, nil
}

func scanChannelMember(row rowScanner) (domain.ChannelMember, error) {
	var member domain.ChannelMember
	var adminRights, bannedRights string
	var role, status string
	if err := row.Scan(
		&member.ChannelID, &member.UserID, &member.InviterUserID, &role, &status,
		&member.JoinedAt, &member.LeftAt, &adminRights, &bannedRights, &member.Rank,
		&member.AvailableMinID, &member.AvailableMinPts, &member.ReadInboxMaxID, &member.ReadOutboxMaxID, &member.UnreadMark, &member.SlowmodeLastSendDate,
	); err != nil {
		return domain.ChannelMember{}, err
	}
	member.Role = domain.ChannelMemberRole(role)
	member.Status = domain.ChannelMemberStatus(status)
	_ = json.Unmarshal([]byte(adminRights), &member.AdminRights)
	_ = json.Unmarshal([]byte(bannedRights), &member.BannedRights)
	return member, nil
}

func discussionGroupUpdateResult(changed map[int64]domain.Channel) domain.DiscussionGroupUpdateResult {
	ids := make([]int64, 0, len(changed))
	for id := range changed {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	out := domain.DiscussionGroupUpdateResult{Channels: make([]domain.Channel, 0, len(ids))}
	for _, id := range ids {
		out.Channels = append(out.Channels, changed[id])
	}
	return out
}

func scanChannelMemberWithCount(row rowScanner) (domain.ChannelMember, int, error) {
	var member domain.ChannelMember
	var adminRights, bannedRights string
	var role, status string
	var count int
	if err := row.Scan(
		&member.ChannelID, &member.UserID, &member.InviterUserID, &role, &status,
		&member.JoinedAt, &member.LeftAt, &adminRights, &bannedRights, &member.Rank,
		&member.AvailableMinID, &member.AvailableMinPts, &member.ReadInboxMaxID, &member.ReadOutboxMaxID, &member.UnreadMark, &member.SlowmodeLastSendDate,
		&count,
	); err != nil {
		return domain.ChannelMember{}, 0, err
	}
	member.Role = domain.ChannelMemberRole(role)
	member.Status = domain.ChannelMemberStatus(status)
	_ = json.Unmarshal([]byte(adminRights), &member.AdminRights)
	_ = json.Unmarshal([]byte(bannedRights), &member.BannedRights)
	return member, count, nil
}

func scanChannelDialogRow(row rowScanner, userID int64) (domain.Channel, domain.Dialog, error) {
	var ch domain.Channel
	var rights, reactionPolicy string
	var topID, topDate, folderID, readInbox, readOutbox, unreadCount, pinnedOrder, unreadMentions, unreadReactions int
	var pinned, unreadMark, viewForumAsMessages bool
	if err := row.Scan(
		&ch.ID, &ch.AccessHash, &ch.CreatorUserID, &ch.Title, &ch.About, &ch.Username,
		&ch.Broadcast, &ch.Megagroup, &ch.Forum, &ch.ForumTabs, &ch.Autotranslation, &ch.RestrictedSponsored, &ch.BroadcastMessagesAllowed, &ch.SendPaidMessagesStars, &ch.NoForwards, &ch.JoinToSend, &ch.JoinRequest, &ch.Signatures, &ch.PreHistoryHidden, &ch.ParticipantsHidden, &ch.AntiSpam, &ch.LinkedChatID, &ch.SlowmodeSeconds, &rights,
		&reactionPolicy, &ch.Color.HasColor, &ch.Color.Color, &ch.Color.BackgroundEmojiID, &ch.ProfileColor.HasColor, &ch.ProfileColor.Color, &ch.ProfileColor.BackgroundEmojiID, &ch.EmojiStatus.DocumentID, &ch.EmojiStatus.Until,
		&ch.ParticipantsCount, &ch.AdminsCount, &ch.KickedCount, &ch.BannedCount, &ch.TopMessageID,
		&ch.PinnedMessageID, &ch.Pts, &ch.TTLPeriod, &ch.Date, &ch.Deleted,
		&ch.PhotoID, &ch.PhotoDCID, &ch.PhotoStripped,
		&topID, &topDate,
		&folderID, &readInbox, &readOutbox, &unreadCount, &pinned, &pinnedOrder, &unreadMark, &unreadMentions, &unreadReactions, &viewForumAsMessages,
	); err != nil {
		return domain.Channel{}, domain.Dialog{}, err
	}
	_ = json.Unmarshal([]byte(rights), &ch.DefaultBannedRights)
	_ = json.Unmarshal([]byte(reactionPolicy), &ch.ReactionPolicy)
	dialog := domain.Dialog{
		Peer:                domain.Peer{Type: domain.PeerTypeChannel, ID: ch.ID},
		FolderID:            folderID,
		TopMessage:          topID,
		TopMessageDate:      topDate,
		ReadInboxMaxID:      readInbox,
		ReadOutboxMaxID:     readOutbox,
		UnreadCount:         unreadCount,
		UnreadMentions:      unreadMentions,
		UnreadReactions:     unreadReactions,
		Pinned:              pinned,
		PinnedOrder:         pinnedOrder,
		UnreadMark:          unreadMark,
		ViewForumAsMessages: viewForumAsMessages,
	}
	_ = userID
	return ch, dialog, nil
}

func scanChannelMessage(row rowScanner) (domain.ChannelMessage, error) {
	var msg domain.ChannelMessage
	var fromType string
	var sendAsType sql.NullString
	var sendAsID sql.NullInt64
	var replyMsgID, replyTopID int
	var replyPeerType string
	var replyPeerID int64
	var discussionChannelID int64
	var discussionMessageID int
	var entities, reply, forward, action string
	var mediaJSON string
	if err := row.Scan(
		&msg.ChannelID, &msg.ID, &msg.RandomID, &msg.SenderUserID, &fromType, &msg.From.ID,
		&sendAsType, &sendAsID, &msg.Date, &msg.EditDate, &msg.Post, &msg.Silent, &msg.NoForwards,
		&msg.Body, &entities, &reply, &replyMsgID, &replyPeerType, &replyPeerID, &replyTopID,
		&forward, &discussionChannelID, &discussionMessageID, &action, &msg.Pts, &msg.Deleted, &mediaJSON,
	); err != nil {
		return domain.ChannelMessage{}, err
	}
	msg.From.Type = domain.PeerType(fromType)
	if sendAsType.Valid && sendAsID.Valid {
		msg.SendAs = &domain.Peer{Type: domain.PeerType(sendAsType.String), ID: sendAsID.Int64}
	}
	parsedEntities, err := decodeMessageEntities(entities)
	if err != nil {
		return domain.ChannelMessage{}, err
	}
	msg.Entities = parsedEntities
	msg.ReplyTo = channelMessageReplyFromColumns(decodeJSONPtr[domain.MessageReply](reply), replyMsgID, replyPeerType, replyPeerID, replyTopID)
	msg.Forward = decodeJSONPtr[domain.MessageForward](forward)
	if discussionChannelID != 0 && discussionMessageID != 0 {
		msg.Discussion = &domain.ChannelDiscussionRef{ChannelID: discussionChannelID, MessageID: discussionMessageID}
	}
	msg.Action = decodeJSONPtr[domain.ChannelMessageAction](action)
	msg.Media, err = decodeMessageMedia(mediaJSON)
	if err != nil {
		return domain.ChannelMessage{}, err
	}
	return msg, nil
}

func scanChannelForumTopic(row rowScanner) (domain.ChannelForumTopic, error) {
	var topic domain.ChannelForumTopic
	if err := row.Scan(
		&topic.ChannelID,
		&topic.TopicID,
		&topic.CreatorUserID,
		&topic.Title,
		&topic.IconColor,
		&topic.IconEmojiID,
		&topic.TitleMissing,
		&topic.Closed,
		&topic.Hidden,
		&topic.Pinned,
		&topic.PinnedOrder,
		&topic.Date,
		&topic.TopMessageID,
		&topic.ReadInboxMaxID,
		&topic.ReadOutboxMaxID,
		&topic.UnreadCount,
		&topic.UnreadMentionsCount,
		&topic.UnreadReactionsCount,
		&topic.UnreadPollVotesCount,
	); err != nil {
		return domain.ChannelForumTopic{}, err
	}
	return topic, nil
}

func scanChannelMessageWithCount(row rowScanner) (domain.ChannelMessage, int, error) {
	var msg domain.ChannelMessage
	var fromType string
	var sendAsType sql.NullString
	var sendAsID sql.NullInt64
	var replyMsgID, replyTopID int
	var replyPeerType string
	var replyPeerID int64
	var discussionChannelID int64
	var discussionMessageID int
	var entities, reply, forward, action string
	var count int
	var mediaJSON string
	if err := row.Scan(
		&msg.ChannelID, &msg.ID, &msg.RandomID, &msg.SenderUserID, &fromType, &msg.From.ID,
		&sendAsType, &sendAsID, &msg.Date, &msg.EditDate, &msg.Post, &msg.Silent, &msg.NoForwards,
		&msg.Body, &entities, &reply, &replyMsgID, &replyPeerType, &replyPeerID, &replyTopID,
		&forward, &discussionChannelID, &discussionMessageID, &action, &msg.Pts, &msg.Deleted, &mediaJSON, &count,
	); err != nil {
		return domain.ChannelMessage{}, 0, err
	}
	msg.From.Type = domain.PeerType(fromType)
	if sendAsType.Valid && sendAsID.Valid {
		msg.SendAs = &domain.Peer{Type: domain.PeerType(sendAsType.String), ID: sendAsID.Int64}
	}
	parsedEntities, err := decodeMessageEntities(entities)
	if err != nil {
		return domain.ChannelMessage{}, 0, err
	}
	msg.Entities = parsedEntities
	msg.ReplyTo = channelMessageReplyFromColumns(decodeJSONPtr[domain.MessageReply](reply), replyMsgID, replyPeerType, replyPeerID, replyTopID)
	msg.Forward = decodeJSONPtr[domain.MessageForward](forward)
	if discussionChannelID != 0 && discussionMessageID != 0 {
		msg.Discussion = &domain.ChannelDiscussionRef{ChannelID: discussionChannelID, MessageID: discussionMessageID}
	}
	msg.Action = decodeJSONPtr[domain.ChannelMessageAction](action)
	msg.Media, err = decodeMessageMedia(mediaJSON)
	if err != nil {
		return domain.ChannelMessage{}, 0, err
	}
	return msg, count, nil
}

func scanChannelMessagePeerReaction(row rowScanner, viewerUserID int64) (domain.ChannelMessagePeerReaction, error) {
	var out domain.ChannelMessagePeerReaction
	var reactionType, reactionValue string
	if err := row.Scan(
		&out.ChannelID,
		&out.MessageID,
		&out.UserID,
		&out.SenderUserID,
		&reactionType,
		&reactionValue,
		&out.Big,
		&out.Unread,
		&out.ChosenOrder,
		&out.Date,
	); err != nil {
		return domain.ChannelMessagePeerReaction{}, err
	}
	out.My = out.UserID == viewerUserID
	out.Reaction = domain.MessageReaction{
		Type:     domain.MessageReactionType(reactionType),
		Emoticon: reactionValue,
	}
	return out, nil
}

func channelMessageReplyFromColumns(reply *domain.MessageReply, msgID int, peerType string, peerID int64, topID int) *domain.MessageReply {
	if reply != nil {
		if reply.MessageID == 0 {
			reply.MessageID = msgID
		}
		if reply.TopMessageID == 0 {
			reply.TopMessageID = topID
		}
		if reply.Peer.ID == 0 && peerType != "" && peerID != 0 {
			reply.Peer = domain.Peer{Type: domain.PeerType(peerType), ID: peerID}
		}
		if reply.MessageID <= 0 && reply.TopMessageID <= 0 {
			return nil
		}
		return reply
	}
	if msgID <= 0 && topID <= 0 {
		return nil
	}
	out := &domain.MessageReply{
		MessageID:    msgID,
		TopMessageID: topID,
	}
	if peerType != "" && peerID != 0 {
		out.Peer = domain.Peer{Type: domain.PeerType(peerType), ID: peerID}
	}
	return out
}

func scanChannelEvent(row rowScanner) (domain.ChannelUpdateEvent, int, error) {
	var event domain.ChannelUpdateEvent
	var typ string
	var messageID int
	var messageIDs, userIDs, payload string
	if err := row.Scan(
		&event.ChannelID, &event.Pts, &event.PtsCount, &event.Date, &typ, &messageID,
		&messageIDs, &event.SenderUserID, &userIDs, &payload,
	); err != nil {
		return domain.ChannelUpdateEvent{}, 0, err
	}
	event.Type = domain.ChannelUpdateEventType(typ)
	_ = json.Unmarshal([]byte(messageIDs), &event.MessageIDs)
	_ = json.Unmarshal([]byte(userIDs), &event.UserIDs)
	var data struct {
		Pinned              bool                  `json:"pinned"`
		Message             domain.ChannelMessage `json:"message"`
		PreviousParticipant domain.ChannelMember  `json:"previous_participant"`
		Participant         domain.ChannelMember  `json:"participant"`
	}
	_ = json.Unmarshal([]byte(payload), &data)
	event.Pinned = data.Pinned
	if data.Message.ID != 0 {
		event.Message = data.Message
	}
	event.Previous = data.PreviousParticipant
	event.Participant = data.Participant
	return event, messageID, nil
}

func scanChannelAdminLogEvent(row rowScanner) (domain.ChannelAdminLogEvent, error) {
	var event domain.ChannelAdminLogEvent
	var typ string
	var prevParticipant, newParticipant, participant, message, prevMessage, newMessage string
	if err := row.Scan(
		&event.ChannelID, &event.ID, &event.UserID, &event.Date, &typ,
		&event.PrevString, &event.NewString, &event.PrevBool, &event.NewBool,
		&event.PrevInt, &event.NewInt, &prevParticipant, &newParticipant, &participant,
		&message, &prevMessage, &newMessage, &event.Query,
	); err != nil {
		return domain.ChannelAdminLogEvent{}, err
	}
	event.Type = domain.ChannelAdminLogEventType(typ)
	event.PrevParticipant = decodeJSONPtr[domain.ChannelMember](prevParticipant)
	event.NewParticipant = decodeJSONPtr[domain.ChannelMember](newParticipant)
	event.Participant = decodeJSONPtr[domain.ChannelMember](participant)
	event.Message = decodeJSONPtr[domain.ChannelMessage](message)
	event.PrevMessage = decodeJSONPtr[domain.ChannelMessage](prevMessage)
	event.NewMessage = decodeJSONPtr[domain.ChannelMessage](newMessage)
	return event, nil
}

func channelDialogToDialog(dialog domain.ChannelDialog) domain.Dialog {
	return domain.Dialog{
		Peer:                domain.Peer{Type: domain.PeerTypeChannel, ID: dialog.ChannelID},
		FolderID:            dialog.FolderID,
		TopMessage:          dialog.TopMessageID,
		TopMessageDate:      dialog.TopMessageDate,
		ReadInboxMaxID:      dialog.ReadInboxMaxID,
		ReadOutboxMaxID:     dialog.ReadOutboxMaxID,
		UnreadCount:         dialog.UnreadCount,
		UnreadMentions:      dialog.UnreadMentions,
		UnreadReactions:     dialog.UnreadReactions,
		Pinned:              dialog.Pinned,
		PinnedOrder:         dialog.PinnedOrder,
		UnreadMark:          dialog.UnreadMark,
		ViewForumAsMessages: dialog.ViewForumAsMessages,
	}
}

func channelDialogMatchesFilter(dialog domain.Dialog, channel domain.Channel, filter domain.DialogFilter) bool {
	if filter.HasFolderID {
		if filter.FolderID < domain.DialogCustomFolderMinID {
			if dialog.FolderID != filter.FolderID {
				return false
			}
		} else if filter.Folder == nil {
			return false
		}
	}
	if filter.PinnedOnly && !dialog.Pinned {
		return false
	}
	if filter.ExcludePinned && dialog.Pinned {
		return false
	}
	if !channelDialogAfterOffset(dialog, filter) {
		return false
	}
	if filter.Folder == nil {
		return true
	}
	folder := filter.Folder
	if peerInDialogFolder(dialog.Peer, folder.ExcludePeers) {
		return false
	}
	if folder.ExcludeRead && dialog.UnreadCount == 0 && !dialog.UnreadMark {
		return false
	}
	if folder.ExcludeArchived && dialog.FolderID == domain.DialogArchiveFolderID {
		return false
	}
	if peerInDialogFolder(dialog.Peer, folder.IncludePeers) || peerInDialogFolder(dialog.Peer, folder.PinnedPeers) {
		return true
	}
	if channel.Megagroup && folder.Groups {
		return true
	}
	if channel.Broadcast && folder.Broadcasts {
		return true
	}
	return !folder.Groups && !folder.Broadcasts && len(folder.IncludePeers) == 0
}

func channelDialogAfterOffset(dialog domain.Dialog, filter domain.DialogFilter) bool {
	if filter.OffsetDate <= 0 && filter.OffsetID <= 0 {
		if filter.HasOffsetPeer && filter.OffsetPeer == dialog.Peer {
			return false
		}
		return true
	}
	if filter.OffsetDate > 0 {
		if dialog.TopMessageDate != filter.OffsetDate {
			return dialog.TopMessageDate < filter.OffsetDate
		}
		if filter.OffsetID <= 0 {
			return false
		}
		if dialog.TopMessage != filter.OffsetID {
			return dialog.TopMessage < filter.OffsetID
		}
		if filter.HasOffsetPeer && filter.OffsetPeer.Type == dialog.Peer.Type {
			return dialog.Peer.ID < filter.OffsetPeer.ID
		}
		return false
	}
	if dialog.TopMessage != filter.OffsetID {
		return dialog.TopMessage < filter.OffsetID
	}
	if filter.HasOffsetPeer && filter.OffsetPeer.Type == dialog.Peer.Type {
		return dialog.Peer.ID < filter.OffsetPeer.ID
	}
	return false
}

func peerInDialogFolder(peer domain.Peer, items []domain.DialogFolderPeer) bool {
	for _, item := range items {
		if item.Peer == peer {
			return true
		}
	}
	return false
}

func channelFolderPeerIDs(primary []domain.DialogFolderPeer, rest ...[]domain.DialogFolderPeer) []int64 {
	total := len(primary)
	for _, items := range rest {
		total += len(items)
	}
	seen := make(map[int64]struct{}, minInt(total, domain.MaxDialogFolderPeers))
	out := make([]int64, 0, minInt(total, domain.MaxDialogFolderPeers))
	appendOne := func(items []domain.DialogFolderPeer) {
		for _, item := range items {
			if len(out) >= domain.MaxDialogFolderPeers {
				return
			}
			if item.Peer.Type != domain.PeerTypeChannel || item.Peer.ID == 0 {
				continue
			}
			if _, ok := seen[item.Peer.ID]; ok {
				continue
			}
			seen[item.Peer.ID] = struct{}{}
			out = append(out, item.Peer.ID)
		}
	}
	appendOne(primary)
	for _, items := range rest {
		appendOne(items)
	}
	return out
}

func validateChannelMemberVisible(member domain.ChannelMember) error {
	switch member.Status {
	case domain.ChannelMemberActive:
		if member.BannedRights.ViewMessages {
			return domain.ErrChannelUserBanned
		}
		return nil
	case domain.ChannelMemberBanned, domain.ChannelMemberKicked:
		return domain.ErrChannelUserBanned
	default:
		return domain.ErrChannelPrivate
	}
}

func canPostChannel(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.PostMessages)
}

func canSendChannelMessage(channel domain.Channel, member domain.ChannelMember) bool {
	if channel.Broadcast {
		return canPostChannel(member)
	}
	if member.Role == domain.ChannelRoleCreator || member.Role == domain.ChannelRoleAdmin {
		return true
	}
	return !channel.DefaultBannedRights.SendMessages && !member.BannedRights.SendMessages
}

func canInviteToChannel(channel domain.Channel, member domain.ChannelMember) bool {
	if member.Role == domain.ChannelRoleCreator ||
		(member.Role == domain.ChannelRoleAdmin && (member.AdminRights.InviteUsers || member.AdminRights.ChangeInfo)) {
		return true
	}
	return channel.Megagroup && !channel.DefaultBannedRights.InviteUsers && !member.BannedRights.InviteUsers
}

func isChannelAdmin(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || member.Role == domain.ChannelRoleAdmin
}

func canChangeChannelInfo(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.ChangeInfo)
}

func canManageDiscussionBroadcast(member domain.ChannelMember) bool {
	return canChangeChannelInfo(member)
}

func canManageDiscussionGroup(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.PinMessages)
}

func validDiscussionGroup(channel domain.Channel) bool {
	return channel.Megagroup && !channel.Broadcast && !channel.Forum && !channel.Deleted
}

func canAddChannelAdmins(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.AddAdmins)
}

func canBanChannelUsers(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.BanUsers)
}

func canExportChannelInvite(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator ||
		(member.Role == domain.ChannelRoleAdmin && (member.AdminRights.InviteUsers || member.AdminRights.ChangeInfo))
}

func canPinChannelMessages(channel domain.Channel, member domain.ChannelMember) bool {
	if member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.PinMessages) {
		return true
	}
	return channel.Megagroup && !channel.DefaultBannedRights.PinMessages && !member.BannedRights.PinMessages
}

func canManageForumTopic(channel domain.Channel, member domain.ChannelMember, topic domain.ChannelForumTopic, userID int64) bool {
	if topic.CreatorUserID == userID {
		return true
	}
	return canPinChannelMessages(channel, member)
}

func canEditChannelMessage(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.EditMessages)
}

func canDeleteAnyChannelMessage(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.DeleteMessages)
}

func channelSlowModeWait(channel domain.Channel, member domain.ChannelMember, now int) int {
	if channel.SlowmodeSeconds <= 0 || member.Role == domain.ChannelRoleCreator || member.Role == domain.ChannelRoleAdmin {
		return 0
	}
	next := member.SlowmodeLastSendDate + channel.SlowmodeSeconds
	if now >= next {
		return 0
	}
	return next - now
}

func boolPtr(v bool) *bool {
	return &v
}

func channelInitialAvailableMinID(channel domain.Channel) int {
	if channel.PreHistoryHidden {
		return channel.TopMessageID
	}
	return 0
}

func channelInitialAvailableMinPts(channel domain.Channel) int {
	return channel.Pts
}

func publicPreviewableChannel(channel domain.Channel) bool {
	return !channel.Deleted &&
		(channel.Broadcast || channel.Megagroup) &&
		strings.TrimSpace(channel.Username) != ""
}

func publicPreviewMember(channel domain.Channel, userID int64, existing domain.ChannelMember, found bool) domain.ChannelMember {
	member := domain.ChannelMember{
		ChannelID:       channel.ID,
		UserID:          userID,
		Role:            domain.ChannelRoleMember,
		Status:          domain.ChannelMemberLeft,
		AvailableMinID:  channelInitialAvailableMinID(channel),
		AvailableMinPts: channelInitialAvailableMinPts(channel),
		ReadInboxMaxID:  channel.TopMessageID,
		ReadOutboxMaxID: channel.TopMessageID,
	}
	if found {
		member.InviterUserID = existing.InviterUserID
		member.JoinedAt = existing.JoinedAt
		member.LeftAt = existing.LeftAt
		member.AvailableMinID = maxInt(member.AvailableMinID, existing.AvailableMinID)
		member.AvailableMinPts = maxInt(member.AvailableMinPts, existing.AvailableMinPts)
		member.ReadInboxMaxID = maxInt(member.ReadInboxMaxID, existing.ReadInboxMaxID)
		member.ReadOutboxMaxID = maxInt(member.ReadOutboxMaxID, existing.ReadOutboxMaxID)
	}
	return member
}

func previewChannelDialog(userID int64, channel domain.Channel, member domain.ChannelMember) domain.ChannelDialog {
	topMessageID := channel.TopMessageID
	if topMessageID <= member.AvailableMinID {
		topMessageID = 0
	}
	return domain.ChannelDialog{
		UserID:          userID,
		ChannelID:       channel.ID,
		TopMessageID:    topMessageID,
		TopMessageDate:  channel.Date,
		ReadInboxMaxID:  maxInt(channel.TopMessageID, member.ReadInboxMaxID),
		ReadOutboxMaxID: maxInt(channel.TopMessageID, member.ReadOutboxMaxID),
	}
}

func zeroChannelAdminRights(rights domain.ChannelAdminRights) bool {
	return rights == domain.ChannelAdminRights{}
}

func adminRightsSubset(want, have domain.ChannelAdminRights) bool {
	return (!want.ChangeInfo || have.ChangeInfo) &&
		(!want.PostMessages || have.PostMessages) &&
		(!want.EditMessages || have.EditMessages) &&
		(!want.DeleteMessages || have.DeleteMessages) &&
		(!want.BanUsers || have.BanUsers) &&
		(!want.InviteUsers || have.InviteUsers) &&
		(!want.PinMessages || have.PinMessages) &&
		(!want.AddAdmins || have.AddAdmins) &&
		(!want.ManageCall || have.ManageCall) &&
		(!want.Anonymous || have.Anonymous)
}

func zeroChannelBannedRights(rights domain.ChannelBannedRights) bool {
	return rights == domain.ChannelBannedRights{}
}

func adminLogBanType(previous, next domain.ChannelMember) domain.ChannelAdminLogEventType {
	if next.Status == domain.ChannelMemberKicked || next.BannedRights.ViewMessages {
		return domain.ChannelAdminLogParticipantKick
	}
	if previous.Status == domain.ChannelMemberKicked || previous.BannedRights.ViewMessages {
		return domain.ChannelAdminLogParticipantUnkick
	}
	if !zeroChannelBannedRights(next.BannedRights) {
		return domain.ChannelAdminLogParticipantBan
	}
	return domain.ChannelAdminLogParticipantUnban
}

func adminLogEventTypesForFilter(filter domain.ChannelAdminLogFilter) []string {
	if filter.Empty() {
		return nil
	}
	types := make([]string, 0, 16)
	add := func(enabled bool, typ domain.ChannelAdminLogEventType) {
		if enabled {
			types = append(types, string(typ))
		}
	}
	add(filter.Join, domain.ChannelAdminLogParticipantJoin)
	add(filter.Leave, domain.ChannelAdminLogParticipantLeave)
	add(filter.Invite || filter.Invites, domain.ChannelAdminLogParticipantInvite)
	add(filter.Ban, domain.ChannelAdminLogParticipantBan)
	add(filter.Unban, domain.ChannelAdminLogParticipantUnban)
	add(filter.Kick, domain.ChannelAdminLogParticipantKick)
	add(filter.Unkick, domain.ChannelAdminLogParticipantUnkick)
	add(filter.Promote, domain.ChannelAdminLogParticipantPromote)
	add(filter.Demote, domain.ChannelAdminLogParticipantDemote)
	if filter.Info {
		types = append(types,
			string(domain.ChannelAdminLogChangeTitle),
			string(domain.ChannelAdminLogChangeUsername),
			string(domain.ChannelAdminLogChangeLinkedChat),
			string(domain.ChannelAdminLogToggleSlowMode),
		)
	}
	if filter.Settings {
		types = append(types,
			string(domain.ChannelAdminLogToggleSignatures),
			string(domain.ChannelAdminLogTogglePreHistoryHidden),
			string(domain.ChannelAdminLogToggleAntiSpam),
			string(domain.ChannelAdminLogToggleAutotranslation),
		)
	}
	add(filter.Forums || filter.Settings, domain.ChannelAdminLogToggleForum)
	add(filter.Pinned, domain.ChannelAdminLogUpdatePinned)
	add(filter.Edit, domain.ChannelAdminLogEditMessage)
	add(filter.Delete, domain.ChannelAdminLogDeleteMessage)
	add(filter.Send, domain.ChannelAdminLogSendMessage)
	return types
}

func adminLogSearchText(event domain.ChannelAdminLogEvent) string {
	parts := []string{
		event.Query,
		event.PrevString,
		event.NewString,
	}
	for _, msg := range []*domain.ChannelMessage{event.Message, event.PrevMessage, event.NewMessage} {
		if msg != nil {
			parts = append(parts, msg.Body)
		}
	}
	return strings.ToLower(strings.TrimSpace(strings.Join(parts, " ")))
}

func adminLogLikePattern(query string) string {
	query = strings.ReplaceAll(query, `\`, `\\`)
	query = strings.ReplaceAll(query, `%`, `\%`)
	query = strings.ReplaceAll(query, `_`, `\_`)
	return "%" + query + "%"
}

func refreshChannelCountsTx(ctx context.Context, tx pgx.Tx, channel domain.Channel) (domain.Channel, error) {
	var participants, admins, kicked, banned int
	rows, err := tx.Query(ctx, `
SELECT channel_id, user_id, inviter_user_id, role, status, joined_at, left_at, admin_rights::text, banned_rights::text,
       rank, available_min_id, available_min_pts, read_inbox_max_id, read_outbox_max_id, unread_mark, slowmode_last_send_date
FROM channel_members
WHERE channel_id = $1`, channel.ID)
	if err != nil {
		return domain.Channel{}, fmt.Errorf("list channel members for counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		member, err := scanChannelMember(rows)
		if err != nil {
			return domain.Channel{}, err
		}
		if member.Status == domain.ChannelMemberKicked {
			kicked++
		}
		if member.Status != domain.ChannelMemberActive {
			continue
		}
		participants++
		if member.Role == domain.ChannelRoleCreator || member.Role == domain.ChannelRoleAdmin {
			admins++
		}
		if !zeroChannelBannedRights(member.BannedRights) {
			banned++
		}
	}
	if err := rows.Err(); err != nil {
		return domain.Channel{}, err
	}
	if _, err := tx.Exec(ctx, `
UPDATE channels
SET participants_count = $2, admins_count = $3, kicked_count = $4, banned_count = $5, updated_at = now()
WHERE id = $1`, channel.ID, participants, admins, kicked, banned); err != nil {
		return domain.Channel{}, fmt.Errorf("refresh channel counts: %w", err)
	}
	channel.ParticipantsCount = participants
	channel.AdminsCount = admins
	channel.KickedCount = kicked
	channel.BannedCount = banned
	return channel, nil
}

func creatorChannelMember(channelID, userID int64, date int) domain.ChannelMember {
	return domain.ChannelMember{
		ChannelID: channelID,
		UserID:    userID,
		Role:      domain.ChannelRoleCreator,
		Status:    domain.ChannelMemberActive,
		JoinedAt:  date,
		AdminRights: domain.ChannelAdminRights{
			ChangeInfo:     true,
			PostMessages:   true,
			EditMessages:   true,
			DeleteMessages: true,
			BanUsers:       true,
			InviteUsers:    true,
			PinMessages:    true,
			AddAdmins:      true,
			ManageCall:     true,
		},
	}
}

func collectChannelEventRefs(event domain.ChannelUpdateEvent, currentChannelID int64, userRefs, channelRefs map[int64]struct{}) {
	if event.SenderUserID != 0 {
		userRefs[event.SenderUserID] = struct{}{}
	}
	for _, id := range event.UserIDs {
		if id != 0 {
			userRefs[id] = struct{}{}
		}
	}
	for _, member := range []domain.ChannelMember{event.Previous, event.Participant} {
		if member.UserID != 0 {
			userRefs[member.UserID] = struct{}{}
		}
		if member.InviterUserID != 0 {
			userRefs[member.InviterUserID] = struct{}{}
		}
	}
	collectChannelMessageRefs(event.Message, currentChannelID, userRefs, channelRefs)
}

func collectChannelMessageRefs(msg domain.ChannelMessage, currentChannelID int64, userRefs, channelRefs map[int64]struct{}) {
	if msg.SenderUserID != 0 {
		userRefs[msg.SenderUserID] = struct{}{}
	}
	addPeerRef(msg.From, currentChannelID, userRefs, channelRefs)
	if msg.SendAs != nil {
		addPeerRef(*msg.SendAs, currentChannelID, userRefs, channelRefs)
	}
	if msg.Forward != nil {
		addPeerRef(msg.Forward.From, currentChannelID, userRefs, channelRefs)
	}
	if msg.ReplyTo != nil {
		addPeerRef(msg.ReplyTo.Peer, currentChannelID, userRefs, channelRefs)
	}
	if msg.Action != nil {
		for _, id := range msg.Action.UserIDs {
			if id != 0 {
				userRefs[id] = struct{}{}
			}
		}
	}
}

func addPeerRef(peer domain.Peer, currentChannelID int64, userRefs, channelRefs map[int64]struct{}) {
	switch peer.Type {
	case domain.PeerTypeUser:
		if peer.ID != 0 {
			userRefs[peer.ID] = struct{}{}
		}
	case domain.PeerTypeChannel:
		if peer.ID != 0 && peer.ID != currentChannelID {
			channelRefs[peer.ID] = struct{}{}
		}
	}
}

func mapKeysInt64(items map[int64]struct{}) []int64 {
	if len(items) == 0 {
		return nil
	}
	out := make([]int64, 0, len(items))
	for id := range items {
		if id != 0 {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func uniqueChannelUserIDs(ids []int64, exclude int64) []int64 {
	seen := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id == 0 || id == exclude {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func uniqueNonZeroInt64s(items ...int64) []int64 {
	seen := make(map[int64]struct{}, len(items))
	out := make([]int64, 0, len(items))
	for _, item := range items {
		if item == 0 {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func channelMemberIDs(members []domain.ChannelMember) []int64 {
	out := make([]int64, 0, len(members))
	for _, member := range members {
		if member.UserID != 0 {
			out = append(out, member.UserID)
		}
	}
	return out
}

func marshalJSON(v any, empty string) ([]byte, error) {
	if v == nil {
		return []byte(empty), nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		return []byte(empty), nil
	}
	return raw, nil
}

func int64s(ids []int64) []int64 {
	return append([]int64(nil), ids...)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func insertChannelInviteTx(ctx context.Context, tx pgx.Tx, invite domain.ChannelInvite) error {
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_invites (
    channel_id, invite_id, hash, admin_user_id, title, permanent, revoked, request_needed,
    expire_date, usage_limit, usage_count, requested_count, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,0),NULLIF($10,0),$11,$12,to_timestamp($13),to_timestamp($13))`,
		invite.ChannelID, invite.InviteID, invite.Hash, invite.AdminUserID, invite.Title,
		invite.Permanent, invite.Revoked, invite.RequestNeeded, invite.ExpireDate,
		invite.UsageLimit, invite.UsageCount, invite.RequestedCount, invite.Date); err != nil {
		return fmt.Errorf("insert channel invite: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO channel_invite_hashes (hash, channel_id, invite_id)
VALUES ($1,$2,$3)
ON CONFLICT (hash) DO UPDATE SET channel_id = EXCLUDED.channel_id, invite_id = EXCLUDED.invite_id, updated_at = now()`,
		invite.Hash, invite.ChannelID, invite.InviteID); err != nil {
		return fmt.Errorf("insert channel invite hash: %w", err)
	}
	return nil
}

func (s *ChannelStore) getInviteByHash(ctx context.Context, db sqlcgen.DBTX, hash string) (domain.Channel, domain.ChannelInvite, error) {
	return s.getInviteByHashLocked(ctx, db, hash, false)
}

func (s *ChannelStore) getInviteByHashForUpdate(ctx context.Context, tx pgx.Tx, hash string) (domain.Channel, domain.ChannelInvite, error) {
	return s.getInviteByHashLocked(ctx, tx, hash, true)
}

func (s *ChannelStore) getInviteByHashLocked(ctx context.Context, db sqlcgen.DBTX, hash string, forUpdate bool) (domain.Channel, domain.ChannelInvite, error) {
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE OF i"
	}
	row := db.QueryRow(ctx, `
SELECT `+channelColumns+`,
       i.channel_id, i.invite_id, i.hash, i.admin_user_id, i.title, i.permanent, i.revoked, i.request_needed,
       COALESCE(i.expire_date, 0), COALESCE(i.usage_limit, 0), i.usage_count, i.requested_count, EXTRACT(EPOCH FROM i.created_at)::int
FROM channel_invite_hashes h
JOIN channel_invites i ON i.channel_id = h.channel_id AND i.invite_id = h.invite_id
JOIN channels c ON c.id = i.channel_id AND NOT c.deleted
WHERE h.hash = $1 AND NOT i.revoked`+lockClause, hash)
	var ch domain.Channel
	var invite domain.ChannelInvite
	var rights, reactionPolicy string
	if err := row.Scan(
		&ch.ID, &ch.AccessHash, &ch.CreatorUserID, &ch.Title, &ch.About, &ch.Username,
		&ch.Broadcast, &ch.Megagroup, &ch.Forum, &ch.ForumTabs, &ch.Autotranslation, &ch.RestrictedSponsored, &ch.BroadcastMessagesAllowed, &ch.SendPaidMessagesStars, &ch.NoForwards, &ch.JoinToSend, &ch.JoinRequest, &ch.Signatures, &ch.PreHistoryHidden, &ch.ParticipantsHidden, &ch.AntiSpam, &ch.LinkedChatID, &ch.SlowmodeSeconds, &rights,
		&reactionPolicy, &ch.Color.HasColor, &ch.Color.Color, &ch.Color.BackgroundEmojiID, &ch.ProfileColor.HasColor, &ch.ProfileColor.Color, &ch.ProfileColor.BackgroundEmojiID, &ch.EmojiStatus.DocumentID, &ch.EmojiStatus.Until,
		&ch.ParticipantsCount, &ch.AdminsCount, &ch.KickedCount, &ch.BannedCount, &ch.TopMessageID,
		&ch.PinnedMessageID, &ch.Pts, &ch.TTLPeriod, &ch.Date, &ch.Deleted,
		&ch.PhotoID, &ch.PhotoDCID, &ch.PhotoStripped,
		&invite.ChannelID, &invite.InviteID, &invite.Hash, &invite.AdminUserID, &invite.Title,
		&invite.Permanent, &invite.Revoked, &invite.RequestNeeded, &invite.ExpireDate,
		&invite.UsageLimit, &invite.UsageCount, &invite.RequestedCount, &invite.Date,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Channel{}, domain.ChannelInvite{}, domain.ErrInviteHashInvalid
		}
		return domain.Channel{}, domain.ChannelInvite{}, err
	}
	_ = json.Unmarshal([]byte(rights), &ch.DefaultBannedRights)
	_ = json.Unmarshal([]byte(reactionPolicy), &ch.ReactionPolicy)
	return ch, invite, nil
}

func (s *ChannelStore) getInviteByChannelHash(ctx context.Context, db sqlcgen.DBTX, channelID int64, hash string, forUpdate bool) (domain.ChannelInvite, error) {
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE"
	}
	row := db.QueryRow(ctx, `
SELECT channel_id, invite_id, hash, admin_user_id, title, permanent, revoked, request_needed,
       COALESCE(expire_date, 0), COALESCE(usage_limit, 0), usage_count, requested_count,
       EXTRACT(EPOCH FROM created_at)::int
FROM channel_invites
WHERE channel_id = $1 AND hash = $2`+lockClause, channelID, strings.TrimSpace(hash))
	invite, err := scanChannelInvite(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChannelInvite{}, domain.ErrInviteHashInvalid
	}
	return invite, err
}

func (s *ChannelStore) getInviteByID(ctx context.Context, db sqlcgen.DBTX, channelID, inviteID int64, forUpdate bool) (domain.ChannelInvite, error) {
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE"
	}
	row := db.QueryRow(ctx, `
SELECT channel_id, invite_id, hash, admin_user_id, title, permanent, revoked, request_needed,
       COALESCE(expire_date, 0), COALESCE(usage_limit, 0), usage_count, requested_count,
       EXTRACT(EPOCH FROM created_at)::int
FROM channel_invites
WHERE channel_id = $1 AND invite_id = $2`+lockClause, channelID, inviteID)
	invite, err := scanChannelInvite(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChannelInvite{}, domain.ErrInviteHashInvalid
	}
	return invite, err
}

func scanChannelInvite(row rowScanner) (domain.ChannelInvite, error) {
	var invite domain.ChannelInvite
	err := row.Scan(
		&invite.ChannelID,
		&invite.InviteID,
		&invite.Hash,
		&invite.AdminUserID,
		&invite.Title,
		&invite.Permanent,
		&invite.Revoked,
		&invite.RequestNeeded,
		&invite.ExpireDate,
		&invite.UsageLimit,
		&invite.UsageCount,
		&invite.RequestedCount,
		&invite.Date,
	)
	return invite, err
}

func (s *ChannelStore) newPostgresReplacementInvite(old domain.ChannelInvite, date int) (domain.ChannelInvite, error) {
	inviteID, err := randomPositiveInt64()
	if err != nil {
		return domain.ChannelInvite{}, err
	}
	hash, err := randomInviteHash()
	if err != nil {
		return domain.ChannelInvite{}, err
	}
	if date == 0 {
		date = nowUnix()
	}
	return domain.ChannelInvite{
		ChannelID:   old.ChannelID,
		InviteID:    inviteID,
		Hash:        hash,
		AdminUserID: old.AdminUserID,
		Permanent:   old.Permanent,
		Date:        date,
	}, nil
}

func (s *ChannelStore) getPendingInviteImporterTx(ctx context.Context, tx pgx.Tx, channelID, userID int64, forUpdate bool) (domain.ChannelInviteImporter, error) {
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE"
	}
	row := tx.QueryRow(ctx, `
SELECT channel_id, invite_id, user_id, date, requested, approved_by, via_chatlist, about
FROM channel_invite_importers
WHERE channel_id = $1 AND user_id = $2 AND requested`+lockClause, channelID, userID)
	var importer domain.ChannelInviteImporter
	err := row.Scan(&importer.ChannelID, &importer.InviteID, &importer.UserID, &importer.Date, &importer.Requested, &importer.ApprovedBy, &importer.ViaChatlist, &importer.About)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChannelInviteImporter{}, domain.ErrHideRequesterMissing
	}
	return importer, err
}

func deletePendingInviteImporterTx(ctx context.Context, tx pgx.Tx, invite domain.ChannelInvite, userID int64) error {
	tag, err := tx.Exec(ctx, `
DELETE FROM channel_invite_importers
WHERE channel_id = $1 AND user_id = $2 AND requested`, invite.ChannelID, userID)
	if err != nil {
		return fmt.Errorf("delete pending channel invite importer: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrHideRequesterMissing
	}
	if invite.InviteID == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_invites
SET requested_count = CASE WHEN requested_count > 0 THEN requested_count - 1 ELSE 0 END,
    updated_at = now()
WHERE channel_id = $1 AND invite_id = $2`, invite.ChannelID, invite.InviteID); err != nil {
		return fmt.Errorf("decrement channel invite requested count: %w", err)
	}
	return nil
}

func decodeJSONPtr[T any](raw string) *T {
	if raw == "" || raw == "{}" || raw == "null" {
		return nil
	}
	var out T
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return &out
}

func randomChannelAccessHash() (int64, error) {
	return randomPositiveInt64()
}

func randomPositiveInt64() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("rand int64: %w", err)
	}
	return int64(binary.LittleEndian.Uint64(b[:]) & ((1 << 63) - 1)), nil
}

func randomInviteHash() (string, error) {
	var b [18]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("rand invite hash: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func nowUnix() int {
	return int(time.Now().Unix())
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isRetryablePostgresTxError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40P01" || pgErr.Code == "40001"
}

type pgChannelIDAllocator struct {
	db sqlcgen.DBTX
}

func (a pgChannelIDAllocator) NextChannelID(ctx context.Context) (int64, error) {
	current, err := a.CurrentChannelID(ctx)
	if err != nil {
		return 0, err
	}
	return current + 1, nil
}

func (a pgChannelIDAllocator) CurrentChannelID(ctx context.Context) (int64, error) {
	var id int64
	err := a.db.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM channels`).Scan(&id)
	return id, err
}

type pgChannelPtsAllocator struct {
	db sqlcgen.DBTX
}

func (a pgChannelPtsAllocator) NextChannelPts(ctx context.Context, channelID int64) (int, error) {
	current, err := a.CurrentChannelPts(ctx, channelID)
	if err != nil {
		return 0, err
	}
	return current + 1, nil
}

func (a pgChannelPtsAllocator) NextChannelPtsN(ctx context.Context, channelID int64, count int) (int, error) {
	if count <= 0 {
		count = 1
	}
	current, err := a.CurrentChannelPts(ctx, channelID)
	if err != nil {
		return 0, err
	}
	return current + count, nil
}

func (a pgChannelPtsAllocator) CurrentChannelPts(ctx context.Context, channelID int64) (int, error) {
	var pts int
	err := a.db.QueryRow(ctx, `SELECT COALESCE(MAX(pts), 0) FROM channel_update_events WHERE channel_id = $1`, channelID).Scan(&pts)
	return pts, err
}

type pgChannelMessageIDAllocator struct {
	db sqlcgen.DBTX
}

func (a pgChannelMessageIDAllocator) NextChannelMessageID(ctx context.Context, channelID int64) (int, error) {
	current, err := a.CurrentChannelMessageID(ctx, channelID)
	if err != nil {
		return 0, err
	}
	return current + 1, nil
}

func (a pgChannelMessageIDAllocator) CurrentChannelMessageID(ctx context.Context, channelID int64) (int, error) {
	var id int
	err := a.db.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM channel_messages WHERE channel_id = $1`, channelID).Scan(&id)
	return id, err
}
