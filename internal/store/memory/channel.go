package memory

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"telesrv/internal/domain"
)

const firstMemoryChannelID int64 = 2000000000

type channelRandomKey struct {
	channelID int64
	userID    int64
	randomID  int64
}

// ChannelStore is an in-memory channel/supergroup store for tests and local development.
type ChannelStore struct {
	mu         sync.RWMutex
	nextID     int64
	nextHash   int64
	channels   map[int64]domain.Channel
	members    map[int64]map[int64]domain.ChannelMember
	dialogs    map[int64]map[int64]domain.ChannelDialog
	topics     map[int64]map[int]domain.ChannelForumTopic
	messages   map[int64][]domain.ChannelMessage
	reactions  map[int64]map[int]map[int64][]domain.ChannelMessagePeerReaction
	top        map[int64]map[string]domain.TopMessageReaction
	recent     map[int64]map[string]domain.RecentMessageReaction
	savedTags  map[int64]map[string]domain.SavedReactionTag
	mentions   map[int64]map[int64]map[int]int
	msgViews   map[int64]map[int]int
	msgViewers map[int64]map[int]map[int64]struct{}
	events     map[int64][]domain.ChannelUpdateEvent
	adminLogs  map[int64][]domain.ChannelAdminLogEvent
	invites    map[string]domain.ChannelInvite
	importers  map[int64]map[int64]domain.ChannelInviteImporter
	msgSeq     map[int64]int
	ptsSeq     map[int64]int
	logSeq     map[int64]int64
	randomToID map[channelRandomKey]int
}

// NewChannelStore creates an in-memory ChannelStore.
func NewChannelStore() *ChannelStore {
	return &ChannelStore{
		nextID:     firstMemoryChannelID,
		nextHash:   900000000000,
		channels:   make(map[int64]domain.Channel),
		members:    make(map[int64]map[int64]domain.ChannelMember),
		dialogs:    make(map[int64]map[int64]domain.ChannelDialog),
		topics:     make(map[int64]map[int]domain.ChannelForumTopic),
		messages:   make(map[int64][]domain.ChannelMessage),
		reactions:  make(map[int64]map[int]map[int64][]domain.ChannelMessagePeerReaction),
		top:        make(map[int64]map[string]domain.TopMessageReaction),
		recent:     make(map[int64]map[string]domain.RecentMessageReaction),
		savedTags:  make(map[int64]map[string]domain.SavedReactionTag),
		mentions:   make(map[int64]map[int64]map[int]int),
		msgViews:   make(map[int64]map[int]int),
		msgViewers: make(map[int64]map[int]map[int64]struct{}),
		events:     make(map[int64][]domain.ChannelUpdateEvent),
		adminLogs:  make(map[int64][]domain.ChannelAdminLogEvent),
		invites:    make(map[string]domain.ChannelInvite),
		importers:  make(map[int64]map[int64]domain.ChannelInviteImporter),
		msgSeq:     make(map[int64]int),
		ptsSeq:     make(map[int64]int),
		logSeq:     make(map[int64]int64),
		randomToID: make(map[channelRandomKey]int),
	}
}

func (s *ChannelStore) CreateChannel(_ context.Context, req domain.CreateChannelRequest) (domain.CreateChannelResult, error) {
	if req.CreatorUserID == 0 || strings.TrimSpace(req.Title) == "" {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	channelID := s.nextChannelIDLocked()
	channel := domain.Channel{
		ID:                channelID,
		AccessHash:        s.nextAccessHashLocked(),
		CreatorUserID:     req.CreatorUserID,
		Title:             strings.TrimSpace(req.Title),
		About:             req.About,
		Broadcast:         req.Broadcast,
		Megagroup:         req.Megagroup,
		Forum:             req.Forum,
		ForumTabs:         req.ForumTabs,
		ParticipantsCount: 1,
		AdminsCount:       1,
		TTLPeriod:         req.TTLPeriod,
		Date:              req.Date,
	}
	if !channel.Broadcast && !channel.Megagroup {
		channel.Broadcast = true
	}
	creator := domain.ChannelMember{
		ChannelID: channelID,
		UserID:    req.CreatorUserID,
		Role:      domain.ChannelRoleCreator,
		Status:    domain.ChannelMemberActive,
		JoinedAt:  req.Date,
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
	s.channels[channelID] = channel
	s.members[channelID] = map[int64]domain.ChannelMember{creator.UserID: creator}
	members := []domain.ChannelMember{creator}
	for _, userID := range uniqueNonZero(req.MemberUserIDs, req.CreatorUserID) {
		member := domain.ChannelMember{
			ChannelID:     channelID,
			UserID:        userID,
			InviterUserID: req.CreatorUserID,
			Role:          domain.ChannelRoleMember,
			Status:        domain.ChannelMemberActive,
			JoinedAt:      req.Date,
		}
		s.members[channelID][userID] = member
		members = append(members, member)
		channel.ParticipantsCount++
	}
	msg, event := s.appendChannelServiceMessageLocked(channelID, req.CreatorUserID, req.Date, domain.ChannelMessageAction{
		Type:  domain.ChannelActionCreate,
		Title: channel.Title,
	})
	channel.TopMessageID = msg.ID
	channel.Pts = event.Pts
	s.channels[channelID] = channel
	for _, member := range members {
		s.upsertChannelDialogLocked(member.UserID, channel, msg, member.UserID == req.CreatorUserID)
	}
	return domain.CreateChannelResult{
		Channel:    channel,
		Members:    cloneChannelMembers(members),
		Message:    cloneChannelMessage(msg),
		Event:      cloneChannelEvent(event),
		Recipients: s.activeMemberIDsLocked(channelID, 0, 0),
	}, nil
}

func (s *ChannelStore) GetChannel(_ context.Context, viewerUserID, channelID int64) (domain.ChannelView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, preview, err := s.channelForViewerLocked(viewerUserID, channelID)
	if err != nil {
		return domain.ChannelView{}, err
	}
	dialog := s.dialogForUserLocked(viewerUserID, channel)
	if preview {
		dialog = previewChannelDialog(viewerUserID, channel, member)
	}
	return domain.ChannelView{
		Channel: cloneChannel(channel),
		Self:    member,
		Dialog:  dialog,
	}, nil
}

func (s *ChannelStore) SaveChannelDefaultSendAs(_ context.Context, req domain.SaveChannelDefaultSendAsRequest) (domain.ChannelView, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelView{}, domain.ErrChannelInvalid
	}
	if req.SendAs != nil && req.SendAs.Type != domain.PeerTypeUser && req.SendAs.Type != domain.PeerTypeChannel {
		return domain.ChannelView{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelView{}, err
	}
	dialog := s.dialogForUserLocked(req.UserID, channel)
	if req.SendAs != nil {
		p := *req.SendAs
		dialog.DefaultSendAs = &p
	} else {
		dialog.DefaultSendAs = nil
	}
	if s.dialogs[req.UserID] == nil {
		s.dialogs[req.UserID] = make(map[int64]domain.ChannelDialog)
	}
	s.dialogs[req.UserID][req.ChannelID] = dialog
	member := s.members[req.ChannelID][req.UserID]
	return domain.ChannelView{Channel: cloneChannel(channel), Self: member, Dialog: dialog}, nil
}

func (s *ChannelStore) GetChannelByID(_ context.Context, channelID int64) (domain.Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, ok := s.channels[channelID]
	if !ok || channel.Deleted {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return cloneChannel(channel), nil
}

func (s *ChannelStore) GetParticipants(_ context.Context, viewerUserID, channelID int64, filter domain.ChannelParticipantsFilter, offset, limit int) (domain.ChannelParticipantList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, err := s.channelForMemberLocked(viewerUserID, channelID)
	if err != nil {
		return domain.ChannelParticipantList{}, err
	}
	if limit <= 0 || limit > domain.MaxChannelParticipantsLimit {
		limit = domain.MaxChannelParticipantsLimit
	}
	viewer := s.members[channelID][viewerUserID]
	if channel.ParticipantsHidden && !isChannelAdmin(viewer) {
		switch filter.Kind {
		case domain.ChannelParticipantsAdmins:
		case domain.ChannelParticipantsBots:
			return domain.ChannelParticipantList{Channel: channel, Count: 0}, nil
		default:
			return domain.ChannelParticipantList{Channel: channel, Count: channel.ParticipantsCount}, nil
		}
	}
	if (filter.Kind == domain.ChannelParticipantsBanned || filter.Kind == domain.ChannelParticipantsKicked) && !isChannelAdmin(viewer) {
		return domain.ChannelParticipantList{Channel: channel}, nil
	}
	query := strings.ToLower(strings.TrimSpace(filter.Query))
	items := make([]domain.ChannelMember, 0, len(s.members[channelID]))
	for _, member := range s.members[channelID] {
		if !channelParticipantMatchesFilter(member, filter.Kind, query) {
			continue
		}
		items = append(items, member)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Role != items[j].Role {
			return channelRoleOrder(items[i].Role) < channelRoleOrder(items[j].Role)
		}
		return items[i].UserID < items[j].UserID
	})
	count := len(items)
	if offset < 0 {
		offset = 0
	}
	if offset > domain.MaxChannelParticipantsOffset {
		offset = domain.MaxChannelParticipantsOffset
	}
	if offset >= len(items) {
		items = nil
	} else {
		end := offset + limit
		if end > len(items) {
			end = len(items)
		}
		items = items[offset:end]
	}
	return domain.ChannelParticipantList{
		Channel:      channel,
		Participants: cloneChannelMembers(items),
		Count:        count,
	}, nil
}

func (s *ChannelStore) GetParticipant(_ context.Context, viewerUserID, channelID, participantUserID int64) (domain.ChannelMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.channelForMemberLocked(viewerUserID, channelID); err != nil {
		return domain.ChannelMember{}, err
	}
	member, ok := s.members[channelID][participantUserID]
	if !ok {
		return domain.ChannelMember{}, domain.ErrChannelPrivate
	}
	return member, nil
}

func (s *ChannelStore) InviteToChannel(_ context.Context, channelID, inviterUserID int64, userIDs []int64, date int) (domain.CreateChannelResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(inviterUserID, channelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	inviter := s.members[channelID][inviterUserID]
	if !canInviteToChannel(channel, inviter) {
		return domain.CreateChannelResult{}, domain.ErrChannelAdminRequired
	}
	requested := uniqueNonZero(userIDs, 0)
	inviteOne := len(requested) == 1
	canRestoreKicked := canBanChannelUsers(inviter)
	added := make([]int64, 0, len(requested))
	members := make([]domain.ChannelMember, 0, len(requested))
	restoredKicked := 0
	for _, userID := range requested {
		if existing, ok := s.members[channelID][userID]; ok {
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
		s.members[channelID][userID] = member
		members = append(members, member)
		added = append(added, userID)
		channel.ParticipantsCount++
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID:   channelID,
			UserID:      inviterUserID,
			Date:        date,
			Type:        domain.ChannelAdminLogParticipantInvite,
			Participant: ptrChannelMember(member),
		})
	}
	if restoredKicked > 0 {
		channel.KickedCount = maxInt(channel.KickedCount-restoredKicked, 0)
	}
	s.channels[channelID] = channel
	var msg domain.ChannelMessage
	var event domain.ChannelUpdateEvent
	if len(added) > 0 && channel.Megagroup {
		msg, event = s.appendChannelServiceMessageLocked(channelID, inviterUserID, date, domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatAddUser,
			UserIDs: append([]int64(nil), added...),
		})
		channel.TopMessageID = msg.ID
		channel.Pts = event.Pts
		s.channels[channelID] = channel
	}
	for _, member := range members {
		s.upsertChannelDialogLocked(member.UserID, channel, msg, false)
	}
	return domain.CreateChannelResult{
		Channel:    channel,
		Members:    cloneChannelMembers(members),
		Message:    cloneChannelMessage(msg),
		Event:      cloneChannelEvent(event),
		Recipients: s.activeMemberIDsLocked(channelID, 0, 0),
	}, nil
}

func (s *ChannelStore) JoinChannel(_ context.Context, channelID, userID int64, date int) (domain.CreateChannelResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, ok := s.channels[channelID]
	if !ok || channel.Deleted {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	preJoinTopID := channel.TopMessageID
	if existing, ok := s.members[channelID][userID]; ok {
		if existing.Status == domain.ChannelMemberActive {
			return domain.CreateChannelResult{}, domain.ErrUserAlreadyParticipant
		}
		if existing.Status == domain.ChannelMemberBanned || existing.Status == domain.ChannelMemberKicked || existing.BannedRights.ViewMessages {
			return domain.CreateChannelResult{}, domain.ErrChannelUserBanned
		}
	}
	if channel.JoinRequest {
		if err := s.recordPublicJoinRequestLocked(channel, userID, date); err != nil {
			return domain.CreateChannelResult{}, err
		}
		return domain.CreateChannelResult{Channel: channel}, domain.ErrInviteRequestSent
	}
	member := domain.ChannelMember{
		ChannelID: channelID,
		UserID:    userID,
		Role:      domain.ChannelRoleMember,
		Status:    domain.ChannelMemberActive,
		JoinedAt:  date,
	}
	if existing, ok := s.members[channelID][userID]; ok {
		member = existing
		member.Status = domain.ChannelMemberActive
		member.LeftAt = 0
		if minID := channelInitialAvailableMinID(channel); minID > member.AvailableMinID {
			member.AvailableMinID = minID
			member.ReadInboxMaxID = maxInt(member.ReadInboxMaxID, minID)
		}
		member.ReadInboxMaxID = maxInt(member.ReadInboxMaxID, preJoinTopID)
		if minPts := channelInitialAvailableMinPts(channel); minPts > member.AvailableMinPts {
			member.AvailableMinPts = minPts
		}
		channel.ParticipantsCount++
	} else {
		member.AvailableMinID = channelInitialAvailableMinID(channel)
		member.AvailableMinPts = channelInitialAvailableMinPts(channel)
		member.ReadInboxMaxID = maxInt(member.AvailableMinID, preJoinTopID)
		channel.ParticipantsCount++
	}
	if s.members[channelID] == nil {
		s.members[channelID] = make(map[int64]domain.ChannelMember)
	}
	s.members[channelID][userID] = member
	s.channels[channelID] = channel
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID: channelID,
		UserID:    userID,
		Date:      date,
		Type:      domain.ChannelAdminLogParticipantJoin,
	})
	var msg domain.ChannelMessage
	var event domain.ChannelUpdateEvent
	if channel.Megagroup {
		msg, event = s.appendChannelServiceMessageLocked(channelID, userID, date, domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatJoined,
			UserIDs: []int64{userID},
		})
		channel.TopMessageID = msg.ID
		channel.Pts = event.Pts
		s.channels[channelID] = channel
	}
	member.ReadInboxMaxID = maxInt(member.ReadInboxMaxID, channel.TopMessageID)
	if msg.ID != 0 && msg.SenderUserID == userID {
		member.ReadOutboxMaxID = maxInt(member.ReadOutboxMaxID, msg.ID)
	}
	s.members[channelID][userID] = member
	s.upsertChannelDialogLocked(userID, channel, msg, true)
	return domain.CreateChannelResult{
		Channel:    channel,
		Members:    []domain.ChannelMember{member},
		Message:    cloneChannelMessage(msg),
		Event:      cloneChannelEvent(event),
		Recipients: s.activeMemberIDsLocked(channelID, 0, 0),
	}, nil
}

func (s *ChannelStore) LeaveChannel(_ context.Context, channelID, userID int64, date int) (domain.CreateChannelResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	member := s.members[channelID][userID]
	member.Status = domain.ChannelMemberLeft
	member.LeftAt = date
	s.members[channelID][userID] = member
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID: channelID,
		UserID:    userID,
		Date:      date,
		Type:      domain.ChannelAdminLogParticipantLeave,
	})
	if channel.ParticipantsCount > 0 {
		channel.ParticipantsCount--
	}
	var msg domain.ChannelMessage
	var event domain.ChannelUpdateEvent
	if channel.Megagroup {
		msg, event = s.appendChannelServiceMessageLocked(channelID, userID, date, domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatDelete,
			UserIDs: []int64{userID},
		})
		channel.TopMessageID = msg.ID
		channel.Pts = event.Pts
	}
	s.channels[channelID] = channel
	return domain.CreateChannelResult{
		Channel:    channel,
		Members:    []domain.ChannelMember{member},
		Message:    cloneChannelMessage(msg),
		Event:      cloneChannelEvent(event),
		Recipients: append(s.activeMemberIDsLocked(channelID, 0, 0), userID),
	}, nil
}

func (s *ChannelStore) EditChannelTitle(_ context.Context, req domain.EditChannelTitleRequest) (domain.EditChannelTitleResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || strings.TrimSpace(req.Title) == "" {
		return domain.EditChannelTitleResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelTitleResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canChangeChannelInfo(member) {
		return domain.EditChannelTitleResult{}, domain.ErrChannelAdminRequired
	}
	title := strings.TrimSpace(req.Title)
	if channel.Title == title {
		return domain.EditChannelTitleResult{}, domain.ErrChannelNotModified
	}
	prevTitle := channel.Title
	channel.Title = title
	msg, event := s.appendChannelServiceMessageLocked(req.ChannelID, req.UserID, req.Date, domain.ChannelMessageAction{
		Type:  domain.ChannelActionEditTitle,
		Title: title,
	})
	channel.TopMessageID = msg.ID
	channel.Pts = event.Pts
	s.channels[req.ChannelID] = channel
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID:  req.ChannelID,
		UserID:     req.UserID,
		Date:       req.Date,
		Type:       domain.ChannelAdminLogChangeTitle,
		PrevString: prevTitle,
		NewString:  title,
	})
	s.upsertChannelDialogLocked(req.UserID, channel, msg, true)
	return domain.EditChannelTitleResult{
		Channel:    channel,
		Message:    cloneChannelMessage(msg),
		Event:      cloneChannelEvent(event),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
	}, nil
}

func (s *ChannelStore) EditChannelAbout(_ context.Context, req domain.EditChannelAboutRequest) (domain.Channel, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	channel.About = req.About
	s.channels[req.ChannelID] = channel
	return channel, nil
}

func (s *ChannelStore) EditChannelAdmin(_ context.Context, req domain.EditChannelAdminRequest) (domain.EditChannelAdminResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MemberID == 0 {
		return domain.EditChannelAdminResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelAdminResult{}, err
	}
	actor := s.members[req.ChannelID][req.UserID]
	if !canAddChannelAdmins(actor) {
		return domain.EditChannelAdminResult{}, domain.ErrChannelAdminRequired
	}
	if actor.Role != domain.ChannelRoleCreator && !adminRightsSubset(req.AdminRights, actor.AdminRights) {
		return domain.EditChannelAdminResult{}, domain.ErrChannelRightForbidden
	}
	previous, ok := s.members[req.ChannelID][req.MemberID]
	if !ok {
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
	if previous.Status != domain.ChannelMemberActive {
		if minPts := channelInitialAvailableMinPts(channel); minPts > member.AvailableMinPts {
			member.AvailableMinPts = minPts
		}
	}
	member.AdminRights = req.AdminRights
	member.Rank = req.Rank
	if zeroChannelAdminRights(req.AdminRights) {
		member.Role = domain.ChannelRoleMember
		member.Rank = ""
	} else {
		member.Role = domain.ChannelRoleAdmin
	}
	s.members[req.ChannelID][req.MemberID] = member
	logType := domain.ChannelAdminLogParticipantPromote
	if member.Role != domain.ChannelRoleAdmin {
		logType = domain.ChannelAdminLogParticipantDemote
	}
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID:       req.ChannelID,
		UserID:          req.UserID,
		Date:            req.Date,
		Type:            logType,
		PrevParticipant: ptrChannelMember(previous),
		NewParticipant:  ptrChannelMember(member),
	})
	s.refreshChannelCountsLocked(req.ChannelID)
	channel = s.channels[req.ChannelID]
	event := transientChannelParticipantEvent(channel.ID, req.UserID, previous, member, req.Date)
	if msg, ok := s.findMessageLocked(req.ChannelID, channel.TopMessageID); ok {
		s.upsertChannelDialogLocked(member.UserID, channel, msg, false)
	}
	recipients := s.activeMemberIDsLocked(req.ChannelID, 0, 0)
	recipients = append(recipients, req.MemberID)
	return domain.EditChannelAdminResult{
		Channel:     channel,
		Previous:    previous,
		Participant: member,
		Event:       event,
		Recipients:  recipients,
		Date:        req.Date,
	}, nil
}

func (s *ChannelStore) EditChannelBanned(_ context.Context, req domain.EditChannelBannedRequest) (domain.EditChannelBannedResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.Participant.Type != domain.PeerTypeUser || req.Participant.ID == 0 {
		return domain.EditChannelBannedResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelBannedResult{}, err
	}
	actor := s.members[req.ChannelID][req.UserID]
	if !canBanChannelUsers(actor) {
		return domain.EditChannelBannedResult{}, domain.ErrChannelAdminRequired
	}
	previous, ok := s.members[req.ChannelID][req.Participant.ID]
	if !ok {
		previous = domain.ChannelMember{ChannelID: req.ChannelID, UserID: req.Participant.ID, Role: domain.ChannelRoleMember, Status: domain.ChannelMemberLeft}
	}
	if previous.Role == domain.ChannelRoleCreator {
		return domain.EditChannelBannedResult{}, domain.ErrChannelUserCreator
	}
	member := previous
	member.Role = domain.ChannelRoleMember
	member.BannedRights = req.BannedRights
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
	s.members[req.ChannelID][req.Participant.ID] = member
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID:       req.ChannelID,
		UserID:          req.UserID,
		Date:            req.Date,
		Type:            adminLogBanType(previous, member),
		PrevParticipant: ptrChannelMember(previous),
		NewParticipant:  ptrChannelMember(member),
	})
	s.refreshChannelCountsLocked(req.ChannelID)
	channel = s.channels[req.ChannelID]
	event := transientChannelParticipantEvent(channel.ID, req.UserID, previous, member, req.Date)
	if member.Status == domain.ChannelMemberActive {
		if msg, ok := s.findMessageLocked(req.ChannelID, channel.TopMessageID); ok {
			s.upsertChannelDialogLocked(member.UserID, channel, msg, false)
		}
	}
	recipients := s.activeMemberIDsLocked(req.ChannelID, 0, 0)
	recipients = append(recipients, req.Participant.ID)
	return domain.EditChannelBannedResult{
		Channel:     channel,
		Previous:    previous,
		Participant: member,
		Event:       event,
		Recipients:  recipients,
		Date:        req.Date,
	}, nil
}

func (s *ChannelStore) EditChannelDefaultBannedRights(_ context.Context, req domain.EditChannelDefaultBannedRightsRequest) (domain.Channel, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.Channel{}, err
	}
	actor := s.members[req.ChannelID][req.UserID]
	if !canBanChannelUsers(actor) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if channel.DefaultBannedRights == req.BannedRights {
		return domain.Channel{}, domain.ErrChannelNotModified
	}
	channel.DefaultBannedRights = req.BannedRights
	s.channels[req.ChannelID] = channel
	return channel, nil
}

func (s *ChannelStore) DeleteChannel(_ context.Context, req domain.DeleteChannelRequest) (domain.DeleteChannelResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.DeleteChannelResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if member.Role != domain.ChannelRoleCreator {
		return domain.DeleteChannelResult{}, domain.ErrChannelAdminRequired
	}
	recipients := s.activeMemberIDsLocked(req.ChannelID, 0, 0)
	channel.Deleted = true
	s.channels[req.ChannelID] = channel
	return domain.DeleteChannelResult{Channel: channel, Recipients: recipients}, nil
}

func (s *ChannelStore) CheckUsername(_ context.Context, userID, channelID int64, username string) (bool, error) {
	if userID == 0 || channelID == 0 || strings.TrimSpace(username) == "" {
		return false, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.channelForMemberLocked(userID, channelID); err != nil {
		return false, err
	}
	usernameLower := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(username, "@")))
	for id, channel := range s.channels {
		if channel.Deleted || channel.Username == "" {
			continue
		}
		if strings.ToLower(channel.Username) == usernameLower && id != channelID {
			return false, nil
		}
	}
	return true, nil
}

func (s *ChannelStore) UpdateUsername(_ context.Context, req domain.UpdateChannelUsernameRequest) (domain.Channel, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if member.Role != domain.ChannelRoleCreator {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	username := strings.TrimSpace(strings.TrimPrefix(req.Username, "@"))
	usernameLower := strings.ToLower(username)
	if strings.EqualFold(channel.Username, username) {
		return domain.Channel{}, domain.ErrChannelNotModified
	}
	if usernameLower != "" {
		for id, existing := range s.channels {
			if existing.Deleted || existing.Username == "" {
				continue
			}
			if strings.ToLower(existing.Username) == usernameLower && id != req.ChannelID {
				return domain.Channel{}, domain.ErrUsernameOccupied
			}
		}
	}
	prevUsername := channel.Username
	channel.Username = username
	s.channels[req.ChannelID] = channel
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID:  req.ChannelID,
		UserID:     req.UserID,
		Date:       int(time.Now().Unix()),
		Type:       domain.ChannelAdminLogChangeUsername,
		PrevString: prevUsername,
		NewString:  username,
	})
	return channel, nil
}

func (s *ChannelStore) ListAdminedPublicChannels(_ context.Context, userID int64) ([]domain.Channel, error) {
	if userID == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Channel, 0)
	for channelID, members := range s.members {
		member := members[userID]
		if member.Status != domain.ChannelMemberActive || !isChannelAdmin(member) {
			continue
		}
		channel, ok := s.channels[channelID]
		if !ok || channel.Deleted || channel.Username == "" {
			continue
		}
		out = append(out, channel)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if len(out) > domain.MaxAdminedPublicChannels {
		out = out[:domain.MaxAdminedPublicChannels]
	}
	return append([]domain.Channel(nil), out...), nil
}

func (s *ChannelStore) ResolvePublicChannelUsername(_ context.Context, viewerUserID int64, username string) (domain.Channel, bool, error) {
	if viewerUserID == 0 {
		return domain.Channel{}, false, domain.ErrChannelInvalid
	}
	username = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(username, "@")))
	if username == "" {
		return domain.Channel{}, false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, channel := range s.channels {
		if !publicSearchableChannel(channel) {
			continue
		}
		if strings.ToLower(channel.Username) == username {
			return cloneChannel(channel), true, nil
		}
	}
	return domain.Channel{}, false, nil
}

func (s *ChannelStore) SearchPublicChannels(_ context.Context, viewerUserID int64, query string, limit int) (domain.PublicChannelSearchResult, error) {
	if viewerUserID == 0 {
		return domain.PublicChannelSearchResult{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxPublicChannelSearchLimit {
		limit = domain.MaxPublicChannelSearchLimit
	}
	query = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(query, "@")))
	if query == "" {
		return domain.PublicChannelSearchResult{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	type item struct {
		channel domain.Channel
		joined  bool
		rank    int
	}
	items := make([]item, 0, limit)
	for channelID, channel := range s.channels {
		rank, ok := publicChannelSearchRank(channel, query)
		if !ok {
			continue
		}
		member, joined := s.members[channelID][viewerUserID]
		joined = joined && member.Status == domain.ChannelMemberActive
		items = append(items, item{
			channel: cloneChannel(channel),
			joined:  joined,
			rank:    rank,
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].rank != items[j].rank {
			return items[i].rank < items[j].rank
		}
		if items[i].joined != items[j].joined {
			return items[i].joined
		}
		if items[i].channel.ParticipantsCount != items[j].channel.ParticipantsCount {
			return items[i].channel.ParticipantsCount > items[j].channel.ParticipantsCount
		}
		if items[i].channel.Date != items[j].channel.Date {
			return items[i].channel.Date > items[j].channel.Date
		}
		return items[i].channel.ID > items[j].channel.ID
	})

	out := domain.PublicChannelSearchResult{}
	for _, item := range items {
		if len(out.MyResults)+len(out.Results) >= limit {
			break
		}
		if item.joined {
			out.MyResults = append(out.MyResults, item.channel)
		} else {
			out.Results = append(out.Results, item.channel)
		}
	}
	return out, nil
}

func (s *ChannelStore) SetChannelPhoto(_ context.Context, userID, channelID int64, photo *domain.Photo) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if photo != nil && photo.ID != 0 {
		channel.PhotoID = photo.ID
		channel.PhotoDCID = photo.DCID
		channel.PhotoStripped = domain.StrippedFromSizes(photo.Sizes)
	} else {
		channel.PhotoID = 0
		channel.PhotoDCID = 0
		channel.PhotoStripped = nil
	}
	s.channels[channelID] = channel
	return channel, nil
}

func (s *ChannelStore) SetSignatures(_ context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.Signatures
	channel.Signatures = enabled
	s.channels[channelID] = channel
	if prev != enabled {
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      int(time.Now().Unix()),
			Type:      domain.ChannelAdminLogToggleSignatures,
			PrevBool:  prev,
			NewBool:   enabled,
		})
	}
	return channel, nil
}

func (s *ChannelStore) SetPreHistoryHidden(_ context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if member.Role != domain.ChannelRoleCreator {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.PreHistoryHidden
	channel.PreHistoryHidden = enabled
	s.channels[channelID] = channel
	if prev != enabled {
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      int(time.Now().Unix()),
			Type:      domain.ChannelAdminLogTogglePreHistoryHidden,
			PrevBool:  prev,
			NewBool:   enabled,
		})
	}
	return channel, nil
}

func (s *ChannelStore) SetParticipantsHidden(_ context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !channel.Megagroup || !canBanChannelUsers(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	channel.ParticipantsHidden = enabled
	s.channels[channelID] = channel
	return channel, nil
}

func (s *ChannelStore) SetForum(_ context.Context, userID, channelID int64, enabled, tabs bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
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
	channel.Forum = enabled
	channel.ForumTabs = enabled && tabs
	s.channels[channelID] = channel
	if prevForum != channel.Forum || prevTabs != channel.ForumTabs {
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      int(time.Now().Unix()),
			Type:      domain.ChannelAdminLogToggleForum,
			PrevBool:  prevForum,
			NewBool:   enabled,
		})
	}
	return cloneChannel(channel), nil
}

func (s *ChannelStore) SetAutotranslation(_ context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.Autotranslation
	channel.Autotranslation = enabled
	s.channels[channelID] = channel
	if prev != enabled {
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      int(time.Now().Unix()),
			Type:      domain.ChannelAdminLogToggleAutotranslation,
			PrevBool:  prev,
			NewBool:   enabled,
		})
	}
	return cloneChannel(channel), nil
}

func (s *ChannelStore) SetRestrictedSponsored(_ context.Context, userID, channelID int64, restricted bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	channel.RestrictedSponsored = restricted
	s.channels[channelID] = channel
	return cloneChannel(channel), nil
}

func (s *ChannelStore) SetPaidMessagesPrice(_ context.Context, userID, channelID int64, stars int64, broadcastMessagesAllowed bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 || stars < 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	channel.SendPaidMessagesStars = stars
	channel.BroadcastMessagesAllowed = channel.Broadcast && broadcastMessagesAllowed
	s.channels[channelID] = channel
	return cloneChannel(channel), nil
}

func (s *ChannelStore) SetAntiSpam(_ context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !channel.Megagroup || !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.AntiSpam
	channel.AntiSpam = enabled
	s.channels[channelID] = channel
	if prev != enabled {
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      int(time.Now().Unix()),
			Type:      domain.ChannelAdminLogToggleAntiSpam,
			PrevBool:  prev,
			NewBool:   enabled,
		})
	}
	return cloneChannel(channel), nil
}

func (s *ChannelStore) SetSlowMode(_ context.Context, userID, channelID int64, seconds int) (domain.Channel, error) {
	if userID == 0 || channelID == 0 || !domain.ValidChannelSlowModeSeconds(seconds) {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	prev := channel.SlowmodeSeconds
	channel.SlowmodeSeconds = seconds
	s.channels[channelID] = channel
	if prev != seconds {
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: channelID,
			UserID:    userID,
			Date:      int(time.Now().Unix()),
			Type:      domain.ChannelAdminLogToggleSlowMode,
			PrevInt:   prev,
			NewInt:    seconds,
		})
	}
	return channel, nil
}

func (s *ChannelStore) SetNoForwards(_ context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	channel.NoForwards = enabled
	s.channels[channelID] = channel
	return channel, nil
}

func (s *ChannelStore) SetJoinToSend(_ context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !channel.Megagroup || !canExportChannelInvite(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	channel.JoinToSend = enabled
	s.channels[channelID] = channel
	return channel, nil
}

func (s *ChannelStore) SetJoinRequest(_ context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !channel.Megagroup || !canExportChannelInvite(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if enabled && strings.TrimSpace(channel.Username) == "" {
		return domain.Channel{}, domain.ErrChatPublicRequired
	}
	channel.JoinRequest = enabled
	s.channels[channelID] = channel
	return channel, nil
}

func (s *ChannelStore) SetAvailableReactions(_ context.Context, userID, channelID int64, policy domain.ChannelReactionPolicy) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	channel.ReactionPolicy = copyChannelReactionPolicy(policy)
	s.channels[channelID] = channel
	return cloneChannel(channel), nil
}

func (s *ChannelStore) SetColor(_ context.Context, userID, channelID int64, forProfile bool, color domain.ChannelPeerColor) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	if forProfile {
		channel.ProfileColor = color
	} else {
		channel.Color = color
	}
	s.channels[channelID] = channel
	return cloneChannel(channel), nil
}

func (s *ChannelStore) SetEmojiStatus(_ context.Context, userID, channelID int64, status domain.ChannelEmojiStatus) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}
	member := s.members[channelID][userID]
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, domain.ErrChannelAdminRequired
	}
	channel.EmojiStatus = status
	s.channels[channelID] = channel
	return cloneChannel(channel), nil
}

func (s *ChannelStore) ListAdminLog(_ context.Context, req domain.ChannelAdminLogRequest) (domain.ChannelAdminLogResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MaxID < 0 || req.MinID < 0 {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelAdminLogResult{}, err
	}
	if !isChannelAdmin(s.members[req.ChannelID][req.UserID]) {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelAdminRequired
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelAdminLogLimit {
		limit = domain.MaxChannelAdminLogLimit
	}
	admins := int64Set(req.AdminUserIDs)
	query := strings.ToLower(strings.TrimSpace(req.Query))
	out := make([]domain.ChannelAdminLogEvent, 0, limit)
	events := s.adminLogs[req.ChannelID]
	for i := len(events) - 1; i >= 0 && len(out) < limit; i-- {
		event := events[i]
		if req.MaxID > 0 && event.ID >= req.MaxID {
			continue
		}
		if req.MinID > 0 && event.ID <= req.MinID {
			continue
		}
		if len(admins) > 0 {
			if _, ok := admins[event.UserID]; !ok {
				continue
			}
		}
		if !adminLogEventMatchesFilter(event.Type, req.Filter) {
			continue
		}
		if query != "" && !adminLogEventMatchesQuery(event, query) {
			continue
		}
		out = append(out, cloneChannelAdminLogEvent(event))
	}
	return domain.ChannelAdminLogResult{Channel: channel, Events: out}, nil
}

func (s *ChannelStore) SendChannelMessage(_ context.Context, req domain.SendChannelMessageRequest) (domain.SendChannelMessageResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.SendChannelMessageResult{}, domain.ErrChannelInvalid
	}
	if strings.TrimSpace(req.Message) == "" && req.Action == nil && req.Media.IsZero() {
		return domain.SendChannelMessageResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canSendChannelMessage(channel, member) {
		return domain.SendChannelMessageResult{}, domain.ErrChannelWriteForbidden
	}
	if req.RandomID != 0 {
		if id, ok := s.randomToID[channelRandomKey{channelID: req.ChannelID, userID: req.UserID, randomID: req.RandomID}]; ok {
			msg, ok := s.findMessageLocked(req.ChannelID, id)
			if ok {
				event := s.eventForMessageLocked(req.ChannelID, id)
				if event.Message.ID != 0 {
					msg = event.Message
				}
				return domain.SendChannelMessageResult{
					Channel:   channel,
					Message:   cloneChannelMessage(msg),
					Event:     event,
					Duplicate: true,
				}, nil
			}
		}
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	if wait := channelSlowModeWait(channel, member, req.Date); wait > 0 {
		return domain.SendChannelMessageResult{}, domain.NewSlowModeWaitError(wait)
	}
	replyTo, err := s.resolveChannelReplyLocked(req, member, channel)
	if err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	var sendAs *domain.Peer
	if req.SendAs != nil {
		p := *req.SendAs
		sendAs = &p
	}
	pts := s.nextChannelPtsLocked(req.ChannelID)
	msgID := s.nextChannelMessageIDLocked(req.ChannelID)
	var discussion *domain.SendChannelDiscussionResult
	var discussionRef *domain.ChannelDiscussionRef
	if channel.Broadcast && channel.LinkedChatID != 0 {
		if linked, ok := s.channels[channel.LinkedChatID]; ok && !linked.Deleted && linked.Megagroup {
			discussionPts := s.nextChannelPtsLocked(linked.ID)
			discussionMsgID := s.nextChannelMessageIDLocked(linked.ID)
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
				Forward:      &domain.MessageForward{From: domain.Peer{Type: domain.PeerTypeChannel, ID: channel.ID}, Date: req.Date, ChannelPost: msgID, SavedFrom: domain.Peer{Type: domain.PeerTypeChannel, ID: channel.ID}, SavedFromMsgID: msgID},
				Pts:          discussionPts,
			}
			discussionEvent := domain.ChannelUpdateEvent{
				ChannelID: linked.ID,
				Type:      domain.ChannelUpdateNewMessage,
				Pts:       discussionPts,
				PtsCount:  1,
				Date:      req.Date,
				Message:   cloneChannelMessage(discussionMsg),
			}
			s.messages[linked.ID] = append(s.messages[linked.ID], discussionMsg)
			s.events[linked.ID] = append(s.events[linked.ID], discussionEvent)
			linked.TopMessageID = discussionMsgID
			linked.Pts = discussionPts
			s.channels[linked.ID] = linked
			s.addChannelUnreadMentionsLocked(linked.ID, discussionMsg, req.UserID, req.MentionUserIDs)
			for userID, member := range s.members[linked.ID] {
				if member.Status == domain.ChannelMemberActive {
					s.upsertChannelDialogLocked(userID, linked, discussionMsg, false)
				}
			}
			discussion = &domain.SendChannelDiscussionResult{
				Channel:    cloneChannel(linked),
				Message:    cloneChannelMessage(discussionMsg),
				Event:      cloneChannelEvent(discussionEvent),
				Recipients: s.activeMemberIDsLocked(linked.ID, 0, 0),
			}
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
	msg.Replies = s.channelMessageRepliesLocked(req.UserID, req.ChannelID, msg)
	event := domain.ChannelUpdateEvent{
		ChannelID:    req.ChannelID,
		Type:         domain.ChannelUpdateNewMessage,
		Pts:          pts,
		PtsCount:     1,
		Date:         req.Date,
		Message:      cloneChannelMessage(msg),
		SenderUserID: req.UserID,
	}
	s.messages[req.ChannelID] = append(s.messages[req.ChannelID], msg)
	s.events[req.ChannelID] = append(s.events[req.ChannelID], event)
	s.addChannelUnreadMentionsLocked(req.ChannelID, msg, req.UserID, req.MentionUserIDs)
	s.updateForumTopicTopMessageLocked(req.ChannelID, msg)
	if channel.Broadcast {
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: req.ChannelID,
			UserID:    req.UserID,
			Date:      req.Date,
			Type:      domain.ChannelAdminLogSendMessage,
			Message:   ptrChannelMessage(msg),
			Query:     msg.Body,
		})
	}
	if req.RandomID != 0 {
		s.randomToID[channelRandomKey{channelID: req.ChannelID, userID: req.UserID, randomID: req.RandomID}] = msg.ID
	}
	channel.TopMessageID = msg.ID
	channel.Pts = pts
	s.channels[req.ChannelID] = channel
	member.SlowmodeLastSendDate = req.Date
	s.members[req.ChannelID][req.UserID] = member
	for userID, member := range s.members[req.ChannelID] {
		if member.Status == domain.ChannelMemberActive {
			s.upsertChannelDialogLocked(userID, channel, msg, userID == req.UserID)
		}
	}
	return domain.SendChannelMessageResult{
		Channel:    channel,
		Message:    cloneChannelMessage(msg),
		Event:      cloneChannelEvent(event),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
		Discussion: discussion,
	}, nil
}

func (s *ChannelStore) EditChannelMessage(_ context.Context, req domain.EditChannelMessageRequest) (domain.EditChannelMessageResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.ID <= 0 || strings.TrimSpace(req.Message) == "" {
		return domain.EditChannelMessageResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.EditChannelMessageResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	idx, ok := s.findMessageIndexLocked(req.ChannelID, req.ID)
	if !ok || s.messages[req.ChannelID][idx].Deleted || s.messages[req.ChannelID][idx].Action != nil {
		return domain.EditChannelMessageResult{}, domain.ErrMessageIDInvalid
	}
	prevMsg := s.messages[req.ChannelID][idx]
	msg := prevMsg
	if msg.SenderUserID != req.UserID && !canEditChannelMessage(member) {
		return domain.EditChannelMessageResult{}, domain.ErrMessageAuthorRequired
	}
	if msg.Body == req.Message && sameMessageEntities(msg.Entities, req.Entities) {
		return domain.EditChannelMessageResult{}, domain.ErrMessageNotModified
	}
	pts := s.nextChannelPtsLocked(req.ChannelID)
	msg.Body = req.Message
	msg.Entities = append([]domain.MessageEntity(nil), req.Entities...)
	msg.EditDate = req.EditDate
	msg.Pts = pts
	s.messages[req.ChannelID][idx] = msg
	channel.Pts = pts
	s.channels[req.ChannelID] = channel
	event := domain.ChannelUpdateEvent{
		ChannelID:    req.ChannelID,
		Type:         domain.ChannelUpdateEditMessage,
		Pts:          pts,
		PtsCount:     1,
		Date:         req.EditDate,
		Message:      cloneChannelMessage(msg),
		SenderUserID: req.UserID,
	}
	s.events[req.ChannelID] = append(s.events[req.ChannelID], event)
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID:   req.ChannelID,
		UserID:      req.UserID,
		Date:        req.EditDate,
		Type:        domain.ChannelAdminLogEditMessage,
		PrevMessage: ptrChannelMessage(prevMsg),
		NewMessage:  ptrChannelMessage(msg),
		Query:       msg.Body,
	})
	return domain.EditChannelMessageResult{
		Channel:    channel,
		Message:    cloneChannelMessage(msg),
		Event:      cloneChannelEvent(event),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
	}, nil
}

func (s *ChannelStore) DeleteChannelMessages(_ context.Context, req domain.DeleteChannelMessagesRequest) (domain.DeleteChannelMessagesResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || len(req.IDs) == 0 {
		return domain.DeleteChannelMessagesResult{}, domain.ErrChannelInvalid
	}
	if len(req.IDs) > domain.MaxDeleteMessageIDs {
		return domain.DeleteChannelMessagesResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelMessagesResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	deleted, event, channel, err := s.deleteChannelMessagesLocked(channel, member, req.IDs, req.UserID, req.Date)
	if err != nil {
		return domain.DeleteChannelMessagesResult{}, err
	}
	return domain.DeleteChannelMessagesResult{
		Channel:    channel,
		Event:      cloneChannelEvent(event),
		DeletedIDs: append([]int(nil), deleted...),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
	}, nil
}

func (s *ChannelStore) DeleteChannelHistory(_ context.Context, req domain.DeleteChannelHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	maxID := req.MaxID
	if maxID <= 0 || maxID > channel.TopMessageID {
		maxID = channel.TopMessageID
	}
	member := s.members[req.ChannelID][req.UserID]
	if !req.ForEveryone {
		appliedMinID := maxInt(member.AvailableMinID, maxID)
		member.AvailableMinID = appliedMinID
		member.ReadInboxMaxID = maxInt(member.ReadInboxMaxID, appliedMinID)
		member.UnreadMark = false
		s.members[req.ChannelID][req.UserID] = member
		s.deleteChannelUnreadMentionsUpToLocked(req.UserID, req.ChannelID, appliedMinID)
		if s.dialogs[req.UserID] == nil {
			s.dialogs[req.UserID] = make(map[int64]domain.ChannelDialog)
		}
		s.dialogs[req.UserID][req.ChannelID] = s.dialogForUserLocked(req.UserID, channel)
		return domain.DeleteChannelHistoryResult{Channel: channel, AvailableMinID: appliedMinID}, nil
	}
	if !canDeleteAnyChannelMessage(member) {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelAdminRequired
	}
	ids := make([]int, 0, domain.MaxDeleteHistoryBatch)
	for i := len(s.messages[req.ChannelID]) - 1; i >= 0; i-- {
		msg := s.messages[req.ChannelID][i]
		if msg.Deleted || msg.ID > maxID {
			continue
		}
		ids = append(ids, msg.ID)
		if len(ids) >= domain.MaxDeleteHistoryBatch {
			break
		}
	}
	deleted, event, channel, err := s.deleteChannelMessagesLocked(channel, member, ids, req.UserID, req.Date)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	offset := 0
	if len(deleted) == domain.MaxDeleteHistoryBatch {
		offset = 1
	}
	return domain.DeleteChannelHistoryResult{
		Channel:    channel,
		Event:      cloneChannelEvent(event),
		DeletedIDs: append([]int(nil), deleted...),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
		Offset:     offset,
	}, nil
}

func (s *ChannelStore) DeleteChannelParticipantHistory(_ context.Context, req domain.DeleteChannelParticipantHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.ParticipantUserID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canDeleteAnyChannelMessage(member) {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelAdminRequired
	}
	ids := make([]int, 0, domain.MaxDeleteHistoryBatch)
	for i := len(s.messages[req.ChannelID]) - 1; i >= 0; i-- {
		msg := s.messages[req.ChannelID][i]
		if msg.Deleted || msg.SenderUserID != req.ParticipantUserID {
			continue
		}
		ids = append(ids, msg.ID)
		if len(ids) >= domain.MaxDeleteHistoryBatch {
			break
		}
	}
	deleted, event, channel, err := s.deleteChannelMessagesLocked(channel, member, ids, req.UserID, req.Date)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	offset := 0
	if len(deleted) == domain.MaxDeleteHistoryBatch {
		offset = 1
	}
	return domain.DeleteChannelHistoryResult{
		Channel:    channel,
		Event:      cloneChannelEvent(event),
		DeletedIDs: append([]int(nil), deleted...),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
		Offset:     offset,
	}, nil
}

func (s *ChannelStore) UpdatePinnedMessage(_ context.Context, req domain.UpdateChannelPinnedMessageRequest) (domain.UpdateChannelPinnedMessageResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MessageID <= 0 {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.UpdateChannelPinnedMessageResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canPinChannelMessages(channel, member) {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrChannelAdminRequired
	}
	msg, ok := s.findMessageLocked(req.ChannelID, req.MessageID)
	if !ok || msg.Deleted {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrMessageIDInvalid
	}
	pinnedID := 0
	if req.Pinned {
		pinnedID = req.MessageID
	}
	if channel.PinnedMessageID == pinnedID {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrChannelNotModified
	}
	pts := s.nextChannelPtsLocked(req.ChannelID)
	channel.PinnedMessageID = pinnedID
	channel.Pts = pts
	s.channels[req.ChannelID] = channel
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
	s.events[req.ChannelID] = append(s.events[req.ChannelID], event)
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID: req.ChannelID,
		UserID:    req.UserID,
		Date:      req.Date,
		Type:      domain.ChannelAdminLogUpdatePinned,
		Message:   ptrChannelMessage(msg),
		Query:     msg.Body,
	})
	return domain.UpdateChannelPinnedMessageResult{
		Channel:    channel,
		Event:      cloneChannelEvent(event),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
	}, nil
}

func (s *ChannelStore) ExportInvite(_ context.Context, req domain.ExportChannelInviteRequest) (domain.ExportChannelInviteResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ExportChannelInviteResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ExportChannelInviteResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.ExportChannelInviteResult{}, domain.ErrChannelAdminRequired
	}
	if req.LegacyRevokePermanent {
		for hash, invite := range s.invites {
			if invite.ChannelID == req.ChannelID && invite.AdminUserID == req.UserID && invite.Permanent {
				invite.Revoked = true
				s.invites[hash] = invite
			}
		}
	}
	inviteID, err := randomMemoryPositiveInt64()
	if err != nil {
		return domain.ExportChannelInviteResult{}, err
	}
	hash, err := randomMemoryInviteHash()
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
	s.invites[hash] = invite
	return domain.ExportChannelInviteResult{Channel: channel, Invite: invite}, nil
}

func (s *ChannelStore) CheckInvite(_ context.Context, userID int64, hash string, date int) (domain.CheckChannelInviteResult, error) {
	if userID == 0 || strings.TrimSpace(hash) == "" {
		return domain.CheckChannelInviteResult{}, domain.ErrInviteHashEmpty
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	invite, ok := s.invites[strings.TrimSpace(hash)]
	if !ok || invite.Revoked {
		return domain.CheckChannelInviteResult{}, domain.ErrInviteHashInvalid
	}
	if invite.ExpireDate > 0 && invite.ExpireDate < date {
		return domain.CheckChannelInviteResult{}, domain.ErrInviteHashExpired
	}
	channel, ok := s.channels[invite.ChannelID]
	if !ok || channel.Deleted {
		return domain.CheckChannelInviteResult{}, domain.ErrInviteHashInvalid
	}
	member := s.members[invite.ChannelID][userID]
	if member.Status == domain.ChannelMemberKicked || member.Status == domain.ChannelMemberBanned || member.BannedRights.ViewMessages {
		return domain.CheckChannelInviteResult{}, domain.ErrInviteHashInvalid
	}
	return domain.CheckChannelInviteResult{
		Channel: channel,
		Invite:  invite,
		Already: member.Status == domain.ChannelMemberActive,
		Self:    member,
	}, nil
}

func (s *ChannelStore) ImportInvite(_ context.Context, req domain.ImportChannelInviteRequest) (domain.CreateChannelResult, error) {
	if req.UserID == 0 || strings.TrimSpace(req.Hash) == "" {
		return domain.CreateChannelResult{}, domain.ErrInviteHashEmpty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	invite, ok := s.invites[strings.TrimSpace(req.Hash)]
	if !ok || invite.Revoked {
		return domain.CreateChannelResult{}, domain.ErrInviteHashInvalid
	}
	if invite.ExpireDate > 0 && invite.ExpireDate < req.Date {
		return domain.CreateChannelResult{}, domain.ErrInviteHashExpired
	}
	channel, ok := s.channels[invite.ChannelID]
	if !ok || channel.Deleted {
		return domain.CreateChannelResult{}, domain.ErrInviteHashInvalid
	}
	if invite.RequestNeeded {
		if err := s.recordPendingInviteRequestLocked(invite, req.UserID, req.Date); err != nil {
			return domain.CreateChannelResult{}, err
		}
		return domain.CreateChannelResult{Channel: channel}, domain.ErrInviteRequestSent
	}
	return s.approveInviteImporterLocked(channel, invite, req.UserID, 0, req.Date)
}

func (s *ChannelStore) ListExportedInvites(_ context.Context, req domain.ChannelInviteListRequest) (domain.ChannelInviteList, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.AdminUserID == 0 {
		return domain.ChannelInviteList{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.channelForMemberLocked(req.UserID, req.ChannelID); err != nil {
		return domain.ChannelInviteList{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.ChannelInviteList{}, domain.ErrChannelAdminRequired
	}
	all := make([]domain.ChannelInvite, 0)
	for _, invite := range s.invites {
		if invite.ChannelID == req.ChannelID && invite.AdminUserID == req.AdminUserID && invite.Revoked == req.Revoked {
			all = append(all, invite)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Date != all[j].Date {
			return all[i].Date > all[j].Date
		}
		return all[i].Hash > all[j].Hash
	})
	total := len(all)
	start := 0
	if req.OffsetDate > 0 || req.OffsetHash != "" {
		start = len(all)
		for i, invite := range all {
			if invite.Date == req.OffsetDate && invite.Hash == req.OffsetHash {
				start = i + 1
				break
			}
		}
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelInviteListLimit {
		limit = domain.MaxChannelInviteListLimit
	}
	if start > len(all) {
		start = len(all)
	}
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	return domain.ChannelInviteList{Count: total, Invites: cloneChannelInvites(all[start:end])}, nil
}

func (s *ChannelStore) GetExportedInvite(_ context.Context, req domain.GetChannelInviteRequest) (domain.ChannelInvite, error) {
	if req.UserID == 0 || req.ChannelID == 0 || strings.TrimSpace(req.Hash) == "" {
		return domain.ChannelInvite{}, domain.ErrInviteHashEmpty
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.channelForMemberLocked(req.UserID, req.ChannelID); err != nil {
		return domain.ChannelInvite{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.ChannelInvite{}, domain.ErrChannelAdminRequired
	}
	return s.inviteByChannelHashLocked(req.ChannelID, req.Hash)
}

func (s *ChannelStore) EditExportedInvite(_ context.Context, req domain.EditChannelInviteRequest) (domain.EditChannelInviteResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || strings.TrimSpace(req.Hash) == "" {
		return domain.EditChannelInviteResult{}, domain.ErrInviteHashEmpty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.channelForMemberLocked(req.UserID, req.ChannelID); err != nil {
		return domain.EditChannelInviteResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.EditChannelInviteResult{}, domain.ErrChannelAdminRequired
	}
	invite, err := s.inviteByChannelHashLocked(req.ChannelID, req.Hash)
	if err != nil {
		return domain.EditChannelInviteResult{}, err
	}
	if req.Revoked {
		if invite.Revoked {
			return domain.EditChannelInviteResult{}, domain.ErrInviteRevokedMissing
		}
		invite.Revoked = true
		s.invites[invite.Hash] = invite
		if !invite.Permanent {
			return domain.EditChannelInviteResult{Invite: invite}, nil
		}
		newInvite, err := s.newReplacementInviteLocked(invite, req.Date)
		if err != nil {
			return domain.EditChannelInviteResult{}, err
		}
		s.invites[newInvite.Hash] = newInvite
		return domain.EditChannelInviteResult{Invite: invite, NewInvite: &newInvite}, nil
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
	s.invites[invite.Hash] = invite
	return domain.EditChannelInviteResult{Invite: invite}, nil
}

func (s *ChannelStore) DeleteExportedInvite(_ context.Context, req domain.DeleteChannelInviteRequest) error {
	if req.UserID == 0 || req.ChannelID == 0 || strings.TrimSpace(req.Hash) == "" {
		return domain.ErrInviteHashEmpty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.channelForMemberLocked(req.UserID, req.ChannelID); err != nil {
		return err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.ErrChannelAdminRequired
	}
	invite, err := s.inviteByChannelHashLocked(req.ChannelID, req.Hash)
	if err != nil {
		return err
	}
	delete(s.invites, invite.Hash)
	return nil
}

func (s *ChannelStore) DeleteRevokedExportedInvites(_ context.Context, req domain.DeleteRevokedChannelInvitesRequest) error {
	if req.UserID == 0 || req.ChannelID == 0 || req.AdminUserID == 0 {
		return domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.channelForMemberLocked(req.UserID, req.ChannelID); err != nil {
		return err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.ErrChannelAdminRequired
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelHideJoinRequests {
		limit = domain.MaxChannelHideJoinRequests
	}
	deleted := 0
	for hash, invite := range s.invites {
		if invite.ChannelID == req.ChannelID && invite.AdminUserID == req.AdminUserID && invite.Revoked {
			delete(s.invites, hash)
			deleted++
			if deleted >= limit {
				break
			}
		}
	}
	return nil
}

func (s *ChannelStore) ListAdminsWithInvites(_ context.Context, userID, channelID int64) ([]domain.ChannelAdminInviteCount, error) {
	if userID == 0 || channelID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.channelForMemberLocked(userID, channelID); err != nil {
		return nil, err
	}
	member := s.members[channelID][userID]
	if !canExportChannelInvite(member) {
		return nil, domain.ErrChannelAdminRequired
	}
	byAdmin := map[int64]*domain.ChannelAdminInviteCount{}
	for _, invite := range s.invites {
		if invite.ChannelID != channelID {
			continue
		}
		count := byAdmin[invite.AdminUserID]
		if count == nil {
			count = &domain.ChannelAdminInviteCount{AdminUserID: invite.AdminUserID}
			byAdmin[invite.AdminUserID] = count
		}
		if invite.Revoked {
			count.RevokedInvitesCount++
		} else {
			count.InvitesCount++
		}
	}
	out := make([]domain.ChannelAdminInviteCount, 0, len(byAdmin))
	for _, count := range byAdmin {
		out = append(out, *count)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AdminUserID < out[j].AdminUserID })
	return out, nil
}

func (s *ChannelStore) ListInviteImporters(_ context.Context, req domain.ChannelInviteImportersRequest) (domain.ChannelInviteImporterList, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.channelForMemberLocked(req.UserID, req.ChannelID); err != nil {
		return domain.ChannelInviteImporterList{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelAdminRequired
	}
	var inviteID int64
	if req.Hash != "" {
		invite, err := s.inviteByChannelHashLocked(req.ChannelID, req.Hash)
		if err != nil {
			return domain.ChannelInviteImporterList{}, err
		}
		inviteID = invite.InviteID
	}
	if req.Query != "" {
		return domain.ChannelInviteImporterList{}, nil
	}
	all := make([]domain.ChannelInviteImporter, 0)
	for _, importer := range s.importers[req.ChannelID] {
		if importer.Requested != req.Requested {
			continue
		}
		if inviteID != 0 && importer.InviteID != inviteID {
			continue
		}
		all = append(all, importer)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Date != all[j].Date {
			return all[i].Date > all[j].Date
		}
		return all[i].UserID > all[j].UserID
	})
	total := len(all)
	start := 0
	if req.OffsetDate > 0 || req.OffsetUserID != 0 {
		start = len(all)
		for i, importer := range all {
			if importer.Date == req.OffsetDate && importer.UserID == req.OffsetUserID {
				start = i + 1
				break
			}
		}
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelInviteListLimit {
		limit = domain.MaxChannelInviteListLimit
	}
	if start > len(all) {
		start = len(all)
	}
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	return domain.ChannelInviteImporterList{Count: total, Importers: cloneChannelInviteImporters(all[start:end])}, nil
}

func (s *ChannelStore) PendingJoinRequests(_ context.Context, channelID int64, limit int) (domain.ChannelPendingJoinRequests, error) {
	if channelID == 0 {
		return domain.ChannelPendingJoinRequests{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, ok := s.channels[channelID]
	if !ok || channel.Deleted {
		return domain.ChannelPendingJoinRequests{}, domain.ErrChannelInvalid
	}
	all := make([]domain.ChannelInviteImporter, 0)
	for _, importer := range s.importers[channelID] {
		if importer.Requested {
			all = append(all, importer)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Date != all[j].Date {
			return all[i].Date > all[j].Date
		}
		return all[i].UserID > all[j].UserID
	})
	if limit <= 0 || limit > domain.MaxChannelPendingJoinRecentRequesters {
		limit = domain.MaxChannelPendingJoinRecentRequesters
	}
	if len(all) < limit {
		limit = len(all)
	}
	recent := make([]int64, 0, limit)
	for _, importer := range all[:limit] {
		recent = append(recent, importer.UserID)
	}
	return domain.ChannelPendingJoinRequests{
		ChannelID:        channelID,
		Count:            len(all),
		RecentRequesters: recent,
	}, nil
}

func (s *ChannelStore) HideChatJoinRequest(_ context.Context, req domain.HideChannelJoinRequestRequest) (domain.CreateChannelResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TargetUserID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.CreateChannelResult{}, domain.ErrChannelAdminRequired
	}
	importer, ok := s.importers[req.ChannelID][req.TargetUserID]
	if !ok || !importer.Requested {
		return domain.CreateChannelResult{}, domain.ErrHideRequesterMissing
	}
	invite := domain.ChannelInvite{ChannelID: req.ChannelID, AdminUserID: req.UserID}
	if importer.InviteID != 0 {
		var err error
		invite, err = s.inviteByIDLocked(req.ChannelID, importer.InviteID)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
	}
	if !req.Approved {
		s.deletePendingInviteImporterLocked(invite, req.TargetUserID)
		return domain.CreateChannelResult{Channel: channel, Recipients: s.activeMemberIDsLocked(req.ChannelID, req.TargetUserID, 0)}, nil
	}
	return s.approveInviteImporterLocked(channel, invite, req.TargetUserID, req.UserID, req.Date)
}

func (s *ChannelStore) HideAllChatJoinRequests(_ context.Context, req domain.HideChannelJoinRequestsRequest) (domain.CreateChannelResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !canExportChannelInvite(member) {
		return domain.CreateChannelResult{}, domain.ErrChannelAdminRequired
	}
	var inviteID int64
	if req.Hash != "" {
		invite, err := s.inviteByChannelHashLocked(req.ChannelID, req.Hash)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		inviteID = invite.InviteID
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelHideJoinRequests {
		limit = domain.MaxChannelHideJoinRequests
	}
	targets := make([]domain.ChannelInviteImporter, 0, limit)
	for _, importer := range s.importers[req.ChannelID] {
		if !importer.Requested {
			continue
		}
		if inviteID != 0 && importer.InviteID != inviteID {
			continue
		}
		targets = append(targets, importer)
		if len(targets) >= limit {
			break
		}
	}
	var result domain.CreateChannelResult
	for _, importer := range targets {
		invite := domain.ChannelInvite{ChannelID: req.ChannelID, AdminUserID: req.UserID}
		if importer.InviteID != 0 {
			var err error
			invite, err = s.inviteByIDLocked(req.ChannelID, importer.InviteID)
			if err != nil {
				return domain.CreateChannelResult{}, err
			}
		}
		if !req.Approved {
			s.deletePendingInviteImporterLocked(invite, importer.UserID)
			result = domain.CreateChannelResult{Channel: channel, Recipients: s.activeMemberIDsLocked(req.ChannelID, importer.UserID, 0)}
			continue
		}
		result, err = s.approveInviteImporterLocked(channel, invite, importer.UserID, req.UserID, req.Date)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		channel = result.Channel
	}
	if result.Channel.ID == 0 {
		result = domain.CreateChannelResult{Channel: channel, Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0)}
	}
	return result, nil
}

func (s *ChannelStore) approveInviteImporterLocked(channel domain.Channel, invite domain.ChannelInvite, userID, approvedBy int64, date int) (domain.CreateChannelResult, error) {
	if invite.InviteID != 0 && invite.UsageLimit > 0 && invite.UsageCount >= invite.UsageLimit {
		return domain.CreateChannelResult{}, domain.ErrUsersTooMuch
	}
	channelID := channel.ID
	if channelID == 0 {
		channelID = invite.ChannelID
	}
	if existing, ok := s.members[channelID][userID]; ok {
		if existing.Status == domain.ChannelMemberActive {
			return domain.CreateChannelResult{}, domain.ErrUserAlreadyParticipant
		}
		if existing.Status == domain.ChannelMemberKicked || existing.Status == domain.ChannelMemberBanned || existing.BannedRights.ViewMessages {
			return domain.CreateChannelResult{}, domain.ErrInviteHashInvalid
		}
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
	if s.members[channelID] == nil {
		s.members[channelID] = make(map[int64]domain.ChannelMember)
	}
	s.members[channelID][userID] = member
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID: channelID,
		UserID:    userID,
		Date:      date,
		Type:      domain.ChannelAdminLogParticipantJoin,
	})
	if importer, ok := s.importers[channelID][userID]; ok && importer.Requested {
		if importer.InviteID == invite.InviteID {
			if invite.InviteID != 0 && invite.RequestedCount > 0 {
				invite.RequestedCount--
			}
		} else if importer.InviteID != 0 {
			if pendingInvite, err := s.inviteByIDLocked(channelID, importer.InviteID); err == nil && pendingInvite.RequestedCount > 0 {
				pendingInvite.RequestedCount--
				s.invites[pendingInvite.Hash] = pendingInvite
			}
		}
	}
	if invite.InviteID != 0 && invite.Hash != "" {
		invite.UsageCount++
		s.invites[invite.Hash] = invite
	}
	s.refreshChannelCountsLocked(channelID)
	channel = s.channels[channelID]
	var msg domain.ChannelMessage
	var event domain.ChannelUpdateEvent
	if channel.Megagroup {
		msg, event = s.appendChannelServiceMessageLocked(channelID, userID, date, domain.ChannelMessageAction{
			Type:    domain.ChannelActionChatJoined,
			UserIDs: []int64{userID},
		})
		channel.TopMessageID = msg.ID
		channel.Pts = event.Pts
		s.channels[channelID] = channel
	}
	member.ReadInboxMaxID = maxInt(member.ReadInboxMaxID, channel.TopMessageID)
	if msg.ID != 0 {
		member.ReadOutboxMaxID = maxInt(member.ReadOutboxMaxID, msg.ID)
	}
	s.members[channelID][userID] = member
	s.upsertChannelDialogLocked(userID, channel, msg, true)
	if s.importers[channelID] == nil {
		s.importers[channelID] = make(map[int64]domain.ChannelInviteImporter)
	}
	s.importers[channelID][userID] = domain.ChannelInviteImporter{
		ChannelID:  channelID,
		InviteID:   invite.InviteID,
		UserID:     userID,
		Date:       date,
		ApprovedBy: approvedBy,
	}
	return domain.CreateChannelResult{
		Channel:    channel,
		Members:    []domain.ChannelMember{member},
		Message:    cloneChannelMessage(msg),
		Event:      cloneChannelEvent(event),
		Recipients: s.activeMemberIDsLocked(channelID, 0, 0),
	}, nil
}

func (s *ChannelStore) recordPublicJoinRequestLocked(channel domain.Channel, userID int64, date int) error {
	if existing, ok := s.members[channel.ID][userID]; ok {
		if existing.Status == domain.ChannelMemberActive {
			return domain.ErrUserAlreadyParticipant
		}
		if existing.Status == domain.ChannelMemberKicked || existing.Status == domain.ChannelMemberBanned || existing.BannedRights.ViewMessages {
			return domain.ErrInviteHashInvalid
		}
	}
	if s.importers[channel.ID] == nil {
		s.importers[channel.ID] = make(map[int64]domain.ChannelInviteImporter)
	}
	if existing, ok := s.importers[channel.ID][userID]; ok && existing.Requested {
		return domain.ErrInviteRequestSent
	}
	s.importers[channel.ID][userID] = domain.ChannelInviteImporter{
		ChannelID: channel.ID,
		UserID:    userID,
		Date:      date,
		Requested: true,
	}
	return nil
}

func (s *ChannelStore) recordPendingInviteRequestLocked(invite domain.ChannelInvite, userID int64, date int) error {
	if existing, ok := s.members[invite.ChannelID][userID]; ok {
		if existing.Status == domain.ChannelMemberActive {
			return domain.ErrUserAlreadyParticipant
		}
		if existing.Status == domain.ChannelMemberKicked || existing.Status == domain.ChannelMemberBanned || existing.BannedRights.ViewMessages {
			return domain.ErrInviteHashInvalid
		}
	}
	if s.importers[invite.ChannelID] == nil {
		s.importers[invite.ChannelID] = make(map[int64]domain.ChannelInviteImporter)
	}
	if existing, ok := s.importers[invite.ChannelID][userID]; ok && existing.Requested {
		return domain.ErrInviteRequestSent
	}
	s.importers[invite.ChannelID][userID] = domain.ChannelInviteImporter{
		ChannelID: invite.ChannelID,
		InviteID:  invite.InviteID,
		UserID:    userID,
		Date:      date,
		Requested: true,
	}
	invite.RequestedCount++
	s.invites[invite.Hash] = invite
	return nil
}

func (s *ChannelStore) deletePendingInviteImporterLocked(invite domain.ChannelInvite, userID int64) {
	if existing, ok := s.importers[invite.ChannelID][userID]; ok && existing.Requested {
		delete(s.importers[invite.ChannelID], userID)
		if invite.InviteID != 0 && invite.Hash != "" && invite.RequestedCount > 0 {
			invite.RequestedCount--
			s.invites[invite.Hash] = invite
		}
	}
}

func (s *ChannelStore) inviteByChannelHashLocked(channelID int64, hash string) (domain.ChannelInvite, error) {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return domain.ChannelInvite{}, domain.ErrInviteHashEmpty
	}
	invite, ok := s.invites[hash]
	if !ok || invite.ChannelID != channelID {
		return domain.ChannelInvite{}, domain.ErrInviteHashInvalid
	}
	return invite, nil
}

func (s *ChannelStore) inviteByIDLocked(channelID, inviteID int64) (domain.ChannelInvite, error) {
	for _, invite := range s.invites {
		if invite.ChannelID == channelID && invite.InviteID == inviteID {
			return invite, nil
		}
	}
	return domain.ChannelInvite{}, domain.ErrInviteHashInvalid
}

func (s *ChannelStore) newReplacementInviteLocked(old domain.ChannelInvite, date int) (domain.ChannelInvite, error) {
	inviteID, err := randomMemoryPositiveInt64()
	if err != nil {
		return domain.ChannelInvite{}, err
	}
	hash, err := randomMemoryInviteHash()
	if err != nil {
		return domain.ChannelInvite{}, err
	}
	if date == 0 {
		date = int(time.Now().Unix())
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

func cloneChannelInvites(in []domain.ChannelInvite) []domain.ChannelInvite {
	if len(in) == 0 {
		return nil
	}
	out := make([]domain.ChannelInvite, len(in))
	copy(out, in)
	return out
}

func cloneChannelInviteImporters(in []domain.ChannelInviteImporter) []domain.ChannelInviteImporter {
	if len(in) == 0 {
		return nil
	}
	out := make([]domain.ChannelInviteImporter, len(in))
	copy(out, in)
	return out
}

func (s *ChannelStore) ListChannelDialogs(_ context.Context, viewerUserID int64, filter domain.DialogFilter) (domain.ChannelDialogList, error) {
	if viewerUserID == 0 {
		return domain.ChannelDialogList{}, nil
	}
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	channelIDs := make([]int64, 0, len(s.dialogs[viewerUserID]))
	seen := make(map[int64]struct{}, len(s.dialogs[viewerUserID]))
	for channelID := range s.dialogs[viewerUserID] {
		channelIDs = append(channelIDs, channelID)
		seen[channelID] = struct{}{}
	}
	for channelID, members := range s.members {
		if _, ok := seen[channelID]; ok {
			continue
		}
		if member, ok := members[viewerUserID]; ok && member.Status == domain.ChannelMemberActive {
			channelIDs = append(channelIDs, channelID)
			seen[channelID] = struct{}{}
		}
	}

	items := make([]domain.Dialog, 0, len(channelIDs))
	for _, channelID := range channelIDs {
		channel, ok := s.channels[channelID]
		if !ok || channel.Deleted {
			continue
		}
		if _, err := s.channelForMemberLocked(viewerUserID, channelID); err != nil {
			continue
		}
		item := channelDialogToDialog(s.dialogForUserLocked(viewerUserID, channel))
		if !channelDialogMatchesFilter(item, channel, filter) {
			continue
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Pinned != items[j].Pinned {
			return items[i].Pinned
		}
		if items[i].PinnedOrder != items[j].PinnedOrder {
			return items[i].PinnedOrder > items[j].PinnedOrder
		}
		if items[i].TopMessageDate != items[j].TopMessageDate {
			return items[i].TopMessageDate > items[j].TopMessageDate
		}
		if items[i].TopMessage != items[j].TopMessage {
			return items[i].TopMessage > items[j].TopMessage
		}
		return items[i].Peer.ID > items[j].Peer.ID
	})
	out := domain.ChannelDialogList{Count: len(items)}
	for _, dialog := range items {
		if len(out.Dialogs) >= limit {
			break
		}
		out.Dialogs = append(out.Dialogs, dialog)
		channel := s.channels[dialog.Peer.ID]
		out.Channels = append(out.Channels, channel)
		if msg, ok := s.findMessageLocked(dialog.Peer.ID, dialog.TopMessage); ok && !msg.Deleted {
			out.Messages = append(out.Messages, cloneChannelMessage(msg))
		}
	}
	return out, nil
}

func (s *ChannelStore) GetChannelDialogs(_ context.Context, viewerUserID int64, channelIDs []int64) (domain.ChannelDialogList, error) {
	if viewerUserID == 0 || len(channelIDs) == 0 {
		return domain.ChannelDialogList{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
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
		channel, err := s.channelForMemberLocked(viewerUserID, channelID)
		if err != nil {
			continue
		}
		dialog := channelDialogToDialog(s.dialogForUserLocked(viewerUserID, channel))
		out.Dialogs = append(out.Dialogs, dialog)
		out.Channels = append(out.Channels, channel)
		if msg, ok := s.findMessageLocked(channelID, dialog.TopMessage); ok && !msg.Deleted {
			out.Messages = append(out.Messages, cloneChannelMessage(msg))
		}
	}
	out.Count = len(out.Dialogs)
	return out, nil
}

func (s *ChannelStore) ListCommonChannels(_ context.Context, req domain.CommonChannelsRequest) (domain.CommonChannelsResult, error) {
	if req.UserID == 0 || req.TargetUserID == 0 || req.UserID == req.TargetUserID || req.MaxID < 0 {
		return domain.CommonChannelsResult{}, domain.ErrChannelInvalid
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxCommonChannelsLimit {
		limit = domain.MaxCommonChannelsLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]int64, 0)
	for channelID, members := range s.members {
		self, selfOK := members[req.UserID]
		target, targetOK := members[req.TargetUserID]
		if !selfOK || !targetOK || self.Status != domain.ChannelMemberActive || target.Status != domain.ChannelMemberActive {
			continue
		}
		channel, ok := s.channels[channelID]
		if !ok || channel.Deleted || !channel.Megagroup || channel.Broadcast {
			continue
		}
		ids = append(ids, channelID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := domain.CommonChannelsResult{Count: len(ids)}
	if req.CountOnly {
		return out, nil
	}
	for _, channelID := range ids {
		if req.MaxID > 0 && channelID <= req.MaxID {
			continue
		}
		out.Channels = append(out.Channels, cloneChannel(s.channels[channelID]))
		if len(out.Channels) >= limit {
			break
		}
	}
	return out, nil
}

func (s *ChannelStore) ListLeftChannels(_ context.Context, userID int64, offset, limit int) (domain.LeftChannelsResult, error) {
	if userID == 0 || offset < 0 || offset > domain.MaxLeftChannelsOffset {
		return domain.LeftChannelsResult{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxLeftChannelsLimit {
		limit = domain.MaxLeftChannelsLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	all := make([]domain.LeftChannel, 0)
	for channelID, members := range s.members {
		member, ok := members[userID]
		if !ok || member.Status != domain.ChannelMemberLeft {
			continue
		}
		channel, ok := s.channels[channelID]
		if !ok || channel.Deleted || (!channel.Broadcast && !channel.Megagroup) {
			continue
		}
		all = append(all, domain.LeftChannel{
			Channel: cloneChannel(channel),
			Self:    member,
		})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Self.LeftAt != all[j].Self.LeftAt {
			return all[i].Self.LeftAt > all[j].Self.LeftAt
		}
		return all[i].Channel.ID > all[j].Channel.ID
	})

	out := domain.LeftChannelsResult{Count: len(all)}
	if offset >= len(all) {
		return out, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	out.Channels = append(out.Channels, all[offset:end]...)
	return out, nil
}

func (s *ChannelStore) ListInactiveChannels(_ context.Context, userID int64, limit int) (domain.ChannelDialogList, error) {
	if userID == 0 {
		return domain.ChannelDialogList{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxInactiveChannelsLimit {
		limit = domain.MaxInactiveChannelsLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	type item struct {
		channel domain.Channel
		dialog  domain.Dialog
	}
	items := make([]item, 0, limit)
	for channelID, members := range s.members {
		member, ok := members[userID]
		if !ok || member.Status != domain.ChannelMemberActive {
			continue
		}
		channel, ok := s.channels[channelID]
		if !ok || channel.Deleted || (!channel.Broadcast && !channel.Megagroup) {
			continue
		}
		if _, err := s.channelForMemberLocked(userID, channelID); err != nil {
			continue
		}
		dialog := channelDialogToDialog(s.dialogForUserLocked(userID, channel))
		dialog.TopMessageDate = inactiveChannelDate(dialog, channel, member)
		items = append(items, item{channel: cloneChannel(channel), dialog: dialog})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].dialog.TopMessageDate != items[j].dialog.TopMessageDate {
			return items[i].dialog.TopMessageDate < items[j].dialog.TopMessageDate
		}
		if items[i].dialog.TopMessage != items[j].dialog.TopMessage {
			return items[i].dialog.TopMessage < items[j].dialog.TopMessage
		}
		return items[i].channel.ID < items[j].channel.ID
	})
	if len(items) > limit {
		items = items[:limit]
	}
	out := domain.ChannelDialogList{Count: len(items)}
	for _, item := range items {
		out.Dialogs = append(out.Dialogs, item.dialog)
		out.Channels = append(out.Channels, item.channel)
	}
	return out, nil
}

func (s *ChannelStore) ListChannelRecommendations(_ context.Context, req domain.ChannelRecommendationsRequest) (domain.ChannelRecommendationsResult, error) {
	if req.UserID == 0 || req.SourceChannelID < 0 {
		return domain.ChannelRecommendationsResult{}, domain.ErrChannelInvalid
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelRecommendationsLimit {
		limit = domain.DefaultChannelRecommendationsLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]domain.Channel, 0, limit)
	for channelID, channel := range s.channels {
		if !recommendableChannel(channel) || channelID == req.SourceChannelID {
			continue
		}
		if req.SourceChannelID == 0 {
			if member, ok := s.members[channelID][req.UserID]; ok && member.Status == domain.ChannelMemberActive {
				continue
			}
		}
		items = append(items, cloneChannel(channel))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ParticipantsCount != items[j].ParticipantsCount {
			return items[i].ParticipantsCount > items[j].ParticipantsCount
		}
		if items[i].Date != items[j].Date {
			return items[i].Date > items[j].Date
		}
		return items[i].ID > items[j].ID
	})
	out := domain.ChannelRecommendationsResult{Count: len(items)}
	if len(items) > limit {
		items = items[:limit]
	}
	out.Channels = append(out.Channels, items...)
	return out, nil
}

func (s *ChannelStore) ListDiscussionGroups(_ context.Context, userID int64, limit int) ([]domain.Channel, error) {
	if userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxDiscussionGroupsLimit {
		limit = domain.MaxDiscussionGroupsLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]domain.Channel, 0, limit)
	for channelID, channel := range s.channels {
		if !validDiscussionGroup(channel) || channel.Deleted {
			continue
		}
		member := s.members[channelID][userID]
		if member.Status != domain.ChannelMemberActive || !canManageDiscussionGroup(member) {
			continue
		}
		items = append(items, cloneChannel(channel))
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ID > items[j].ID
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *ChannelStore) SetDiscussionGroup(_ context.Context, userID, broadcastID, groupID int64) (domain.DiscussionGroupUpdateResult, error) {
	if userID == 0 {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrChannelInvalid
	}
	if broadcastID == 0 && groupID == 0 {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrLinkNotModified
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := make(map[int64]domain.Channel)
	markChanged := func(channel domain.Channel) {
		if channel.ID != 0 {
			changed[channel.ID] = cloneChannel(channel)
		}
	}
	setLinked := func(channelID, linkedID int64) (domain.Channel, bool) {
		channel, ok := s.channels[channelID]
		if !ok || channel.Deleted {
			return domain.Channel{}, false
		}
		if channel.LinkedChatID == linkedID {
			return channel, true
		}
		channel.LinkedChatID = linkedID
		s.channels[channelID] = channel
		markChanged(channel)
		return channel, true
	}

	if broadcastID == 0 {
		group, groupMember, err := s.channelAndMemberLocked(userID, groupID)
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
		if oldBroadcast, ok := s.channels[oldBroadcastID]; ok && oldBroadcast.LinkedChatID == groupID {
			if updated, ok := setLinked(oldBroadcastID, 0); ok {
				s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
					ChannelID: updated.ID,
					UserID:    userID,
					Date:      int(time.Now().Unix()),
					Type:      domain.ChannelAdminLogChangeLinkedChat,
					PrevInt:   int(groupID),
					NewInt:    0,
				})
			}
		}
		setLinked(groupID, 0)
		return discussionGroupUpdateResult(changed), nil
	}

	broadcast, broadcastMember, err := s.channelAndMemberLocked(userID, broadcastID)
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
		updated, _ := setLinked(broadcastID, 0)
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: updated.ID,
			UserID:    userID,
			Date:      int(time.Now().Unix()),
			Type:      domain.ChannelAdminLogChangeLinkedChat,
			PrevInt:   int(oldGroupID),
			NewInt:    0,
		})
		if oldGroup, ok := s.channels[oldGroupID]; ok && oldGroup.LinkedChatID == broadcastID {
			setLinked(oldGroupID, 0)
		}
		return discussionGroupUpdateResult(changed), nil
	}

	group, groupMember, err := s.channelAndMemberLocked(userID, groupID)
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
		if oldGroup, ok := s.channels[oldGroupID]; ok && oldGroup.LinkedChatID == broadcastID {
			setLinked(oldGroupID, 0)
		}
	}
	if oldBroadcastID != 0 && oldBroadcastID != broadcastID {
		if oldBroadcast, ok := s.channels[oldBroadcastID]; ok && oldBroadcast.LinkedChatID == groupID {
			if updated, ok := setLinked(oldBroadcastID, 0); ok {
				s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
					ChannelID: updated.ID,
					UserID:    userID,
					Date:      int(time.Now().Unix()),
					Type:      domain.ChannelAdminLogChangeLinkedChat,
					PrevInt:   int(groupID),
					NewInt:    0,
				})
			}
		}
	}
	updatedBroadcast, _ := setLinked(broadcastID, groupID)
	setLinked(groupID, broadcastID)
	s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
		ChannelID: updatedBroadcast.ID,
		UserID:    userID,
		Date:      int(time.Now().Unix()),
		Type:      domain.ChannelAdminLogChangeLinkedChat,
		PrevInt:   int(oldGroupID),
		NewInt:    int(groupID),
	})
	return discussionGroupUpdateResult(changed), nil
}

func (s *ChannelStore) SetChannelDialogPinned(_ context.Context, userID, channelID int64, pinned bool) (bool, error) {
	if userID == 0 || channelID == 0 {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return false, nil
	}
	nextOrder := 1
	for _, dialog := range s.dialogs[userID] {
		if dialog.Pinned && dialog.PinnedOrder >= nextOrder {
			nextOrder = dialog.PinnedOrder + 1
		}
	}
	dialog := s.dialogForUserLocked(userID, channel)
	changed := dialog.Pinned != pinned || (pinned && dialog.PinnedOrder == 0)
	dialog.Pinned = pinned
	if pinned {
		if dialog.PinnedOrder == 0 {
			dialog.PinnedOrder = nextOrder
		}
	} else {
		dialog.PinnedOrder = 0
	}
	if s.dialogs[userID] == nil {
		s.dialogs[userID] = make(map[int64]domain.ChannelDialog)
	}
	s.dialogs[userID][channelID] = dialog
	return changed, nil
}

func (s *ChannelStore) ReorderChannelPinnedDialogs(_ context.Context, userID int64, order []domain.Peer, force bool) error {
	if userID == 0 {
		return nil
	}
	positions := make(map[int64]int, len(order))
	for i, peer := range order {
		if peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
			continue
		}
		if _, ok := positions[peer.ID]; ok {
			continue
		}
		positions[peer.ID] = len(order) - i
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for channelID, dialog := range s.dialogs[userID] {
		if pos, ok := positions[channelID]; ok {
			dialog.Pinned = true
			dialog.PinnedOrder = pos
			s.dialogs[userID][channelID] = dialog
			continue
		}
		if force && dialog.Pinned {
			dialog.Pinned = false
			dialog.PinnedOrder = 0
			s.dialogs[userID][channelID] = dialog
		}
	}
	return nil
}

func (s *ChannelStore) SetChannelDialogUnreadMark(_ context.Context, userID, channelID int64, unread bool) (bool, error) {
	if userID == 0 || channelID == 0 {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return false, nil
	}
	dialog := s.dialogForUserLocked(userID, channel)
	changed := dialog.UnreadMark != unread
	dialog.UnreadMark = unread
	member := s.members[channelID][userID]
	member.UnreadMark = unread
	s.members[channelID][userID] = member
	if s.dialogs[userID] == nil {
		s.dialogs[userID] = make(map[int64]domain.ChannelDialog)
	}
	s.dialogs[userID][channelID] = dialog
	return changed, nil
}

func (s *ChannelStore) SetChannelViewForumAsMessages(_ context.Context, userID, channelID int64, enabled bool) (bool, error) {
	if userID == 0 || channelID == 0 {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(userID, channelID)
	if err != nil {
		return false, nil
	}
	dialog := s.dialogForUserLocked(userID, channel)
	changed := dialog.ViewForumAsMessages != enabled
	dialog.ViewForumAsMessages = enabled
	if s.dialogs[userID] == nil {
		s.dialogs[userID] = make(map[int64]domain.ChannelDialog)
	}
	s.dialogs[userID][channelID] = dialog
	return changed, nil
}

func (s *ChannelStore) ListChannelUnreadMarked(_ context.Context, userID int64) ([]domain.Peer, error) {
	if userID == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Peer, 0, len(s.dialogs[userID]))
	for channelID, dialog := range s.dialogs[userID] {
		if !dialog.UnreadMark {
			continue
		}
		if _, err := s.channelForMemberLocked(userID, channelID); err != nil {
			continue
		}
		out = append(out, domain.Peer{Type: domain.PeerTypeChannel, ID: channelID})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *ChannelStore) EditChannelPeerFolders(_ context.Context, userID int64, peers []domain.FolderPeerUpdate) error {
	if userID == 0 || len(peers) == 0 {
		return nil
	}
	updates := make(map[int64]int, len(peers))
	for _, item := range peers {
		if item.Peer.Type != domain.PeerTypeChannel || item.Peer.ID == 0 {
			continue
		}
		if item.FolderID != domain.DialogMainFolderID && item.FolderID != domain.DialogArchiveFolderID {
			continue
		}
		updates[item.Peer.ID] = item.FolderID
	}
	if len(updates) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for channelID, folderID := range updates {
		channel, err := s.channelForMemberLocked(userID, channelID)
		if err != nil {
			continue
		}
		dialog := s.dialogForUserLocked(userID, channel)
		dialog.FolderID = folderID
		if s.dialogs[userID] == nil {
			s.dialogs[userID] = make(map[int64]domain.ChannelDialog)
		}
		s.dialogs[userID][channelID] = dialog
	}
	return nil
}

func (s *ChannelStore) ListChannelHistory(_ context.Context, viewerUserID int64, filter domain.ChannelHistoryFilter) (domain.ChannelHistory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, _, err := s.channelForViewerLocked(viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	items := append([]domain.ChannelMessage(nil), s.messages[filter.ChannelID]...)
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	out := make([]domain.ChannelMessage, 0, limit)
	query := strings.ToLower(strings.TrimSpace(filter.Query))
	matched := 0
	for _, msg := range items {
		if msg.Deleted {
			continue
		}
		if msg.ID <= member.AvailableMinID {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(msg.Body), query) {
			continue
		}
		if filter.SenderUserID != 0 && msg.SenderUserID != filter.SenderUserID {
			continue
		}
		if filter.MinDate > 0 && msg.Date <= filter.MinDate {
			continue
		}
		if filter.MaxDate > 0 && msg.Date >= filter.MaxDate {
			continue
		}
		if filter.OffsetID > 0 && msg.ID >= filter.OffsetID {
			continue
		}
		if filter.OffsetID <= 0 && filter.OffsetDate > 0 && msg.Date >= filter.OffsetDate {
			continue
		}
		if filter.MaxID > 0 && msg.ID > filter.MaxID {
			continue
		}
		if filter.MinID > 0 && msg.ID <= filter.MinID {
			continue
		}
		matched++
		if len(out) < limit {
			out = append(out, cloneChannelMessage(msg))
		}
	}
	s.populateChannelMessageRepliesLocked(viewerUserID, filter.ChannelID, out)
	s.populateChannelMessageReactionsLocked(viewerUserID, channel, out)
	return domain.ChannelHistory{
		Channel:  channel,
		Self:     member,
		Messages: out,
		Count:    matched,
	}, nil
}

func (s *ChannelStore) SearchPublicPosts(_ context.Context, viewerUserID int64, req domain.ChannelSearchPostsRequest) (domain.ChannelHistory, error) {
	query := strings.ToLower(strings.TrimSpace(req.Query))
	hashtag := strings.ToLower(strings.TrimSpace(req.Hashtag))
	if (query == "") == (hashtag == "") {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if req.Limit <= 0 || req.Limit > domain.MaxChannelSearchPostsLimit {
		req.Limit = domain.MaxChannelSearchPostsLimit
	}
	type hit struct {
		channel domain.Channel
		message domain.ChannelMessage
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	hits := make([]hit, 0, req.Limit+1)
	for channelID, channel := range s.channels {
		if channel.Deleted || strings.TrimSpace(channel.Username) == "" {
			continue
		}
		for _, msg := range s.messages[channelID] {
			if msg.Deleted || strings.TrimSpace(msg.Body) == "" {
				continue
			}
			if !channelSearchPostAfterCursor(msg, req) {
				continue
			}
			body := strings.ToLower(msg.Body)
			if query != "" && !strings.Contains(body, query) {
				continue
			}
			if hashtag != "" && !strings.Contains(body, "#"+hashtag) {
				continue
			}
			hits = append(hits, hit{channel: channel, message: cloneChannelMessage(msg)})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		a, b := hits[i].message, hits[j].message
		if a.Date != b.Date {
			return a.Date > b.Date
		}
		if a.ChannelID != b.ChannelID {
			return a.ChannelID > b.ChannelID
		}
		return a.ID > b.ID
	})
	out := domain.ChannelHistory{Count: len(hits)}
	if out.Count > req.Limit {
		out.Count = req.Limit + 1
		hits = hits[:req.Limit]
	}
	channelSeen := make(map[int64]struct{}, len(hits))
	for _, h := range hits {
		out.Messages = append(out.Messages, h.message)
		if _, ok := channelSeen[h.channel.ID]; ok {
			continue
		}
		channelSeen[h.channel.ID] = struct{}{}
		out.Channels = append(out.Channels, h.channel)
	}
	s.populateChannelMessagesReactionsLocked(viewerUserID, out.Channels, out.Messages)
	return out, nil
}

func (s *ChannelStore) SearchJoinedMessages(_ context.Context, viewerUserID int64, req domain.ChannelGlobalSearchRequest) (domain.ChannelHistory, error) {
	query := strings.ToLower(strings.TrimSpace(req.Query))
	if viewerUserID == 0 || query == "" {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if req.Limit <= 0 || req.Limit > domain.MaxChannelGlobalSearchLimit {
		req.Limit = domain.MaxChannelGlobalSearchLimit
	}
	type hit struct {
		channel domain.Channel
		message domain.ChannelMessage
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	hits := make([]hit, 0, req.Limit+1)
	for channelID, channel := range s.channels {
		if channel.Deleted {
			continue
		}
		if req.BroadcastsOnly && (!channel.Broadcast || channel.Megagroup) {
			continue
		}
		if req.GroupsOnly && !channel.Megagroup {
			continue
		}
		member, ok := s.members[channelID][viewerUserID]
		if !ok || member.Status != domain.ChannelMemberActive || member.BannedRights.ViewMessages {
			continue
		}
		if req.HasFolderID {
			dialog, ok := s.dialogs[viewerUserID][channelID]
			if !ok || dialog.FolderID != req.FolderID {
				continue
			}
		}
		for _, msg := range s.messages[channelID] {
			if msg.Deleted || strings.TrimSpace(msg.Body) == "" {
				continue
			}
			if member.AvailableMinID > 0 && msg.ID <= member.AvailableMinID {
				continue
			}
			if req.MinDate > 0 && msg.Date <= req.MinDate {
				continue
			}
			if req.MaxDate > 0 && msg.Date >= req.MaxDate {
				continue
			}
			if !channelGlobalSearchAfterCursor(msg, req) {
				continue
			}
			if !strings.Contains(strings.ToLower(msg.Body), query) {
				continue
			}
			hits = append(hits, hit{channel: channel, message: cloneChannelMessage(msg)})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		a, b := hits[i].message, hits[j].message
		if a.Date != b.Date {
			return a.Date > b.Date
		}
		if a.ChannelID != b.ChannelID {
			return a.ChannelID > b.ChannelID
		}
		return a.ID > b.ID
	})
	out := domain.ChannelHistory{Count: len(hits)}
	if out.Count > req.Limit {
		out.Count = req.Limit + 1
		hits = hits[:req.Limit]
	}
	channelSeen := make(map[int64]struct{}, len(hits))
	for _, h := range hits {
		out.Messages = append(out.Messages, h.message)
		if _, ok := channelSeen[h.channel.ID]; ok {
			continue
		}
		channelSeen[h.channel.ID] = struct{}{}
		out.Channels = append(out.Channels, h.channel)
	}
	s.populateChannelMessagesReactionsLocked(viewerUserID, out.Channels, out.Messages)
	return out, nil
}

func channelSearchPostAfterCursor(msg domain.ChannelMessage, req domain.ChannelSearchPostsRequest) bool {
	if req.OffsetRate <= 0 && req.OffsetChannelID <= 0 && req.OffsetID <= 0 {
		return true
	}
	if req.OffsetRate > 0 {
		if msg.Date < req.OffsetRate {
			return true
		}
		if msg.Date > req.OffsetRate {
			return false
		}
	}
	if req.OffsetChannelID > 0 {
		if msg.ChannelID < req.OffsetChannelID {
			return true
		}
		if msg.ChannelID > req.OffsetChannelID {
			return false
		}
	}
	if req.OffsetID > 0 {
		return msg.ID < req.OffsetID
	}
	return false
}

func channelGlobalSearchAfterCursor(msg domain.ChannelMessage, req domain.ChannelGlobalSearchRequest) bool {
	if req.OffsetRate <= 0 && req.OffsetChannelID <= 0 && req.OffsetID <= 0 {
		return true
	}
	if req.OffsetRate > 0 {
		if msg.Date < req.OffsetRate {
			return true
		}
		if msg.Date > req.OffsetRate {
			return false
		}
	}
	if req.OffsetChannelID > 0 {
		if msg.ChannelID < req.OffsetChannelID {
			return true
		}
		if msg.ChannelID > req.OffsetChannelID {
			return false
		}
	}
	if req.OffsetID > 0 {
		return msg.ID < req.OffsetID
	}
	return false
}

func (s *ChannelStore) GetChannelMessages(_ context.Context, viewerUserID, channelID int64, ids []int) (domain.ChannelHistory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, err := s.channelAndMemberLocked(viewerUserID, channelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	if len(ids) == 0 {
		return domain.ChannelHistory{Channel: channel, Self: member}, nil
	}
	if len(ids) > domain.MaxGetMessageIDs {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	wanted := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
		}
		wanted[id] = struct{}{}
	}
	messages := make([]domain.ChannelMessage, 0, len(wanted))
	for _, msg := range s.messages[channelID] {
		if _, ok := wanted[msg.ID]; !ok {
			continue
		}
		if msg.Deleted || msg.ID <= member.AvailableMinID {
			continue
		}
		messages = append(messages, cloneChannelMessage(msg))
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID > messages[j].ID })
	s.populateChannelMessageRepliesLocked(viewerUserID, channelID, messages)
	s.populateChannelMessageReactionsLocked(viewerUserID, channel, messages)
	return domain.ChannelHistory{Channel: channel, Self: member, Messages: messages, Count: len(messages)}, nil
}

func (s *ChannelStore) ReadChannelMessageContents(_ context.Context, req domain.ReadChannelMessageContentsRequest) (domain.ReadChannelMessageContentsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ReadChannelMessageContentsResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, member, err := s.channelAndMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelMessageContentsResult{}, err
	}
	if len(req.IDs) == 0 {
		return domain.ReadChannelMessageContentsResult{Channel: channel}, nil
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ReadChannelMessageContentsResult{}, domain.ErrChannelInvalid
	}
	wanted := make(map[int]struct{}, len(req.IDs))
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ReadChannelMessageContentsResult{}, domain.ErrMessageIDInvalid
		}
		wanted[id] = struct{}{}
	}
	messages := make([]domain.ChannelMessage, 0, len(wanted))
	for _, msg := range s.messages[req.ChannelID] {
		if _, ok := wanted[msg.ID]; !ok {
			continue
		}
		if msg.Deleted || msg.ID <= member.AvailableMinID {
			continue
		}
		messages = append(messages, cloneChannelMessage(msg))
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID > messages[j].ID })
	clearedSet := make(map[int]struct{})
	for _, msg := range messages {
		byUser := s.reactions[req.ChannelID][msg.ID]
		if len(byUser) == 0 {
			continue
		}
		for reactedUserID, rows := range byUser {
			changed := false
			for i := range rows {
				if rows[i].SenderUserID == req.UserID && rows[i].UserID != req.UserID && rows[i].Unread {
					rows[i].Unread = false
					changed = true
					clearedSet[msg.ID] = struct{}{}
				}
			}
			if changed {
				byUser[reactedUserID] = rows
			}
		}
	}
	cleared := make([]int, 0, len(clearedSet))
	for id := range clearedSet {
		cleared = append(cleared, id)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(cleared)))
	if len(cleared) > 0 {
		s.refreshChannelUnreadReactionsDialogLocked(req.UserID, req.ChannelID)
	}
	s.populateChannelMessageRepliesLocked(req.UserID, req.ChannelID, messages)
	s.populateChannelMessageReactionsLocked(req.UserID, channel, messages)
	return domain.ReadChannelMessageContentsResult{
		Channel:                         channel,
		Messages:                        messages,
		ClearedUnreadReactionMessageIDs: cleared,
	}, nil
}

func (s *ChannelStore) GetChannelMessageViews(_ context.Context, req domain.ChannelMessageViewsRequest) (domain.ChannelMessageViewsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelMessageViewsResult{}, domain.ErrChannelInvalid
	}
	if len(req.IDs) == 0 {
		return domain.ChannelMessageViewsResult{Views: map[int]int{}}, nil
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ChannelMessageViewsResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, member, err := s.channelAndMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageViewsResult{}, err
	}
	wanted := make(map[int]struct{}, len(req.IDs))
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ChannelMessageViewsResult{}, domain.ErrMessageIDInvalid
		}
		wanted[id] = struct{}{}
	}
	visible := make(map[int]struct{}, len(wanted))
	for _, msg := range s.messages[req.ChannelID] {
		if _, ok := wanted[msg.ID]; !ok {
			continue
		}
		if msg.Deleted || msg.ID <= member.AvailableMinID {
			continue
		}
		visible[msg.ID] = struct{}{}
	}
	if s.msgViews[req.ChannelID] == nil {
		s.msgViews[req.ChannelID] = make(map[int]int)
	}
	if s.msgViewers[req.ChannelID] == nil {
		s.msgViewers[req.ChannelID] = make(map[int]map[int64]struct{})
	}
	for id := range visible {
		if req.Increment {
			if s.msgViewers[req.ChannelID][id] == nil {
				s.msgViewers[req.ChannelID][id] = make(map[int64]struct{})
			}
			if _, seen := s.msgViewers[req.ChannelID][id][req.UserID]; !seen {
				s.msgViewers[req.ChannelID][id][req.UserID] = struct{}{}
				s.msgViews[req.ChannelID][id]++
			}
		}
	}
	out := make(map[int]int, len(visible))
	for id := range visible {
		out[id] = s.msgViews[req.ChannelID][id]
	}
	return domain.ChannelMessageViewsResult{Views: out}, nil
}

func (s *ChannelStore) SetChannelMessageReactions(_ context.Context, req domain.SetChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	if len(req.Reactions) > domain.MaxChannelMessageReactionsPerUser {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, member, err := s.channelAndMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	idx, ok := s.findMessageIndexLocked(req.ChannelID, req.MessageID)
	if !ok {
		return domain.ChannelMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	msg := s.messages[req.ChannelID][idx]
	if msg.Deleted || msg.Action != nil || msg.ID <= member.AvailableMinID {
		return domain.ChannelMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if s.reactions[req.ChannelID] == nil {
		s.reactions[req.ChannelID] = make(map[int]map[int64][]domain.ChannelMessagePeerReaction)
	}
	if s.reactions[req.ChannelID][req.MessageID] == nil {
		s.reactions[req.ChannelID][req.MessageID] = make(map[int64][]domain.ChannelMessagePeerReaction)
	}
	if len(req.Reactions) == 0 {
		delete(s.reactions[req.ChannelID][req.MessageID], req.UserID)
	} else {
		rows := make([]domain.ChannelMessagePeerReaction, 0, len(req.Reactions))
		for i, reaction := range req.Reactions {
			rows = append(rows, domain.ChannelMessagePeerReaction{
				ChannelID:    req.ChannelID,
				MessageID:    req.MessageID,
				SenderUserID: msg.SenderUserID,
				UserID:       req.UserID,
				Reaction:     reaction,
				Big:          req.Big,
				Unread:       msg.SenderUserID != 0 && msg.SenderUserID != req.UserID,
				ChosenOrder:  i + 1,
				Date:         req.Date,
			})
		}
		s.reactions[req.ChannelID][req.MessageID][req.UserID] = rows
		if s.top[req.UserID] == nil {
			s.top[req.UserID] = make(map[string]domain.TopMessageReaction)
		}
		for _, reaction := range req.Reactions {
			key := messageReactionKey(reaction)
			row := s.top[req.UserID][key]
			row.UserID = req.UserID
			row.Reaction = reaction
			row.Count++
			row.Date = req.Date
			s.top[req.UserID][key] = row
		}
		if req.AddToRecent {
			if s.recent[req.UserID] == nil {
				s.recent[req.UserID] = make(map[string]domain.RecentMessageReaction)
			}
			for _, reaction := range req.Reactions {
				s.recent[req.UserID][messageReactionKey(reaction)] = domain.RecentMessageReaction{
					UserID:   req.UserID,
					Reaction: reaction,
					Date:     req.Date,
				}
			}
		}
	}
	s.refreshChannelUnreadReactionsDialogLocked(msg.SenderUserID, req.ChannelID)
	reactions := s.channelMessageReactionsLocked(req.UserID, channel, req.MessageID)
	msg = cloneChannelMessage(msg)
	msg.Reactions = cloneChannelMessageReactionsPtr(&reactions)
	return domain.ChannelMessageReactionsResult{
		Channel:    cloneChannel(channel),
		Message:    msg,
		Messages:   []domain.ChannelMessage{msg},
		Reactions:  cloneChannelMessageReactions(reactions),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
	}, nil
}

func (s *ChannelStore) DeleteChannelParticipantReaction(_ context.Context, req domain.DeleteChannelParticipantReactionRequest) (domain.ChannelMessageReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID || req.ParticipantUserID == 0 {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, member, err := s.channelAndMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	if !canDeleteAnyChannelMessage(member) {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelAdminRequired
	}
	msg, ok := s.findMessageLocked(req.ChannelID, req.MessageID)
	if !ok || msg.Deleted || msg.ID <= member.AvailableMinID {
		return domain.ChannelMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if s.reactions[req.ChannelID] != nil && s.reactions[req.ChannelID][req.MessageID] != nil {
		delete(s.reactions[req.ChannelID][req.MessageID], req.ParticipantUserID)
	}
	s.refreshChannelUnreadReactionsDialogLocked(msg.SenderUserID, req.ChannelID)
	reactions := s.channelMessageReactionsLocked(req.UserID, channel, req.MessageID)
	outMsg := cloneChannelMessage(msg)
	outMsg.Reactions = cloneChannelMessageReactionsPtr(&reactions)
	return domain.ChannelMessageReactionsResult{
		Channel:    cloneChannel(channel),
		Message:    outMsg,
		Messages:   []domain.ChannelMessage{outMsg},
		Reactions:  cloneChannelMessageReactions(reactions),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
	}, nil
}

func (s *ChannelStore) DeleteChannelParticipantReactions(_ context.Context, req domain.DeleteChannelParticipantReactionsRequest) (domain.DeleteChannelParticipantReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.ParticipantUserID == 0 {
		return domain.DeleteChannelParticipantReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.Limit <= 0 || req.Limit > domain.MaxDeleteParticipantReactionsBatch {
		req.Limit = domain.MaxDeleteParticipantReactionsBatch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, member, err := s.channelAndMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelParticipantReactionsResult{}, err
	}
	if !canDeleteAnyChannelMessage(member) {
		return domain.DeleteChannelParticipantReactionsResult{}, domain.ErrChannelAdminRequired
	}
	type reactionMsg struct {
		id     int
		sender int64
		date   int
	}
	candidates := make([]reactionMsg, 0)
	for msgID, byUser := range s.reactions[req.ChannelID] {
		rows := byUser[req.ParticipantUserID]
		if len(rows) == 0 {
			continue
		}
		msg, ok := s.findMessageLocked(req.ChannelID, msgID)
		if !ok || msg.Deleted || msg.ID <= member.AvailableMinID {
			continue
		}
		item := reactionMsg{id: msgID, sender: msg.SenderUserID}
		for _, row := range rows {
			if row.Date > item.date {
				item.date = row.Date
			}
		}
		candidates = append(candidates, item)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].date != candidates[j].date {
			return candidates[i].date > candidates[j].date
		}
		return candidates[i].id > candidates[j].id
	})
	if len(candidates) > req.Limit {
		candidates = candidates[:req.Limit]
	}
	owners := make(map[int64]struct{})
	ids := make([]int, 0, len(candidates))
	for _, item := range candidates {
		if s.reactions[req.ChannelID] != nil && s.reactions[req.ChannelID][item.id] != nil {
			delete(s.reactions[req.ChannelID][item.id], req.ParticipantUserID)
		}
		if item.sender != 0 {
			owners[item.sender] = struct{}{}
		}
		ids = append(ids, item.id)
	}
	for ownerID := range owners {
		s.refreshChannelUnreadReactionsDialogLocked(ownerID, req.ChannelID)
	}
	messages := make([]domain.ChannelMessage, 0, len(ids))
	for _, id := range ids {
		msg, ok := s.findMessageLocked(req.ChannelID, id)
		if !ok || msg.Deleted || msg.ID <= member.AvailableMinID {
			continue
		}
		reactions := s.channelMessageReactionsLocked(req.UserID, channel, id)
		outMsg := cloneChannelMessage(msg)
		outMsg.Reactions = cloneChannelMessageReactionsPtr(&reactions)
		messages = append(messages, outMsg)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID > messages[j].ID })
	return domain.DeleteChannelParticipantReactionsResult{
		Channel:    cloneChannel(channel),
		Messages:   messages,
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
		Deleted:    len(ids),
	}, nil
}

func (s *ChannelStore) GetChannelMessageReactions(_ context.Context, req domain.ChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, err := s.channelAndMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageReactionsResult{}, err
	}
	wanted := make(map[int]struct{}, len(req.IDs))
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ChannelMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
		wanted[id] = struct{}{}
	}
	messages := make([]domain.ChannelMessage, 0, len(wanted))
	for _, msg := range s.messages[req.ChannelID] {
		if _, ok := wanted[msg.ID]; !ok {
			continue
		}
		if msg.Deleted || msg.ID <= member.AvailableMinID {
			continue
		}
		item := cloneChannelMessage(msg)
		reactions := s.channelMessageReactionsLocked(req.UserID, channel, msg.ID)
		item.Reactions = cloneChannelMessageReactionsPtr(&reactions)
		messages = append(messages, item)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID > messages[j].ID })
	res := domain.ChannelMessageReactionsResult{
		Channel:  cloneChannel(channel),
		Messages: messages,
	}
	if len(messages) == 1 {
		res.Message = messages[0]
		if messages[0].Reactions != nil {
			res.Reactions = cloneChannelMessageReactions(*messages[0].Reactions)
		}
	}
	return res, nil
}

func (s *ChannelStore) ListChannelMessageReactions(_ context.Context, req domain.ChannelMessageReactionsListRequest) (domain.ChannelMessageReactionsList, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.ChannelMessageReactionsList{}, domain.ErrChannelInvalid
	}
	if req.Limit <= 0 || req.Limit > domain.MaxChannelMessageReactionListLimit {
		req.Limit = domain.MaxChannelMessageReactionListLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, err := s.channelAndMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelMessageReactionsList{}, err
	}
	if channel.Broadcast && !channel.Megagroup {
		return domain.ChannelMessageReactionsList{}, domain.ErrChannelRightForbidden
	}
	msg, ok := s.findMessageLocked(req.ChannelID, req.MessageID)
	if !ok || msg.Deleted || msg.ID <= member.AvailableMinID {
		return domain.ChannelMessageReactionsList{}, domain.ErrMessageIDInvalid
	}
	rows := s.channelMessageReactionRowsLocked(req.ChannelID, req.MessageID, req.UserID, req.Reaction)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Date != rows[j].Date {
			return rows[i].Date > rows[j].Date
		}
		if rows[i].UserID != rows[j].UserID {
			return rows[i].UserID > rows[j].UserID
		}
		return rows[i].Reaction.Emoticon < rows[j].Reaction.Emoticon
	})
	total := len(rows)
	if req.Offset != "" {
		if offset, ok := parseMemoryReactionOffset(req.Offset); ok {
			filtered := rows[:0]
			for _, row := range rows {
				if memoryReactionAfterOffset(row, offset) {
					filtered = append(filtered, row)
				}
			}
			rows = filtered
		}
	}
	next := ""
	if len(rows) > req.Limit {
		rows = rows[:req.Limit]
		next = memoryReactionOffset(rows[len(rows)-1])
	}
	return domain.ChannelMessageReactionsList{
		Channel:    cloneChannel(channel),
		Message:    cloneChannelMessage(msg),
		Count:      total,
		Reactions:  cloneChannelPeerReactions(rows),
		NextOffset: next,
	}, nil
}

func (s *ChannelStore) RecordMessageReactionUse(_ context.Context, userID int64, reactions []domain.MessageReaction, addToRecent bool, date int) error {
	if userID == 0 || len(reactions) == 0 {
		return nil
	}
	if date == 0 {
		date = int(time.Now().Unix())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.top[userID] == nil {
		s.top[userID] = make(map[string]domain.TopMessageReaction)
	}
	if addToRecent && s.recent[userID] == nil {
		s.recent[userID] = make(map[string]domain.RecentMessageReaction)
	}
	for _, reaction := range reactions {
		if reaction.Type != domain.MessageReactionEmoji || strings.TrimSpace(reaction.Emoticon) == "" {
			continue
		}
		key := messageReactionKey(reaction)
		row := s.top[userID][key]
		row.UserID = userID
		row.Reaction = reaction
		row.Count++
		row.Date = date
		s.top[userID][key] = row
		if addToRecent {
			s.recent[userID][key] = domain.RecentMessageReaction{
				UserID:   userID,
				Reaction: reaction,
				Date:     date,
			}
		}
	}
	return nil
}

func (s *ChannelStore) ListTopMessageReactions(_ context.Context, userID int64, limit int) ([]domain.MessageReaction, error) {
	if userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.MessageReaction{}, nil
	}
	if limit > domain.MaxTopMessageReactions {
		limit = domain.MaxTopMessageReactions
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := make([]domain.TopMessageReaction, 0, len(s.top[userID]))
	for _, row := range s.top[userID] {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count != rows[j].Count {
			return rows[i].Count > rows[j].Count
		}
		if rows[i].Date != rows[j].Date {
			return rows[i].Date > rows[j].Date
		}
		if rows[i].Reaction.Type != rows[j].Reaction.Type {
			return rows[i].Reaction.Type < rows[j].Reaction.Type
		}
		return rows[i].Reaction.Emoticon < rows[j].Reaction.Emoticon
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]domain.MessageReaction, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Reaction)
	}
	return out, nil
}

func (s *ChannelStore) ListRecentMessageReactions(_ context.Context, userID int64, limit int) ([]domain.MessageReaction, error) {
	if userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.MessageReaction{}, nil
	}
	if limit > domain.MaxRecentMessageReactions {
		limit = domain.MaxRecentMessageReactions
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := make([]domain.RecentMessageReaction, 0, len(s.recent[userID]))
	for _, row := range s.recent[userID] {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Date != rows[j].Date {
			return rows[i].Date > rows[j].Date
		}
		if rows[i].Reaction.Type != rows[j].Reaction.Type {
			return rows[i].Reaction.Type < rows[j].Reaction.Type
		}
		return rows[i].Reaction.Emoticon < rows[j].Reaction.Emoticon
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]domain.MessageReaction, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Reaction)
	}
	return out, nil
}

func (s *ChannelStore) ClearRecentMessageReactions(_ context.Context, userID int64) error {
	if userID == 0 {
		return domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.recent, userID)
	return nil
}

func (s *ChannelStore) ListSavedReactionTags(_ context.Context, userID int64, limit int) ([]domain.SavedReactionTag, error) {
	if userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.SavedReactionTag{}, nil
	}
	if limit > domain.MaxSavedReactionTags {
		limit = domain.MaxSavedReactionTags
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := make([]domain.SavedReactionTag, 0, len(s.savedTags[userID]))
	for _, row := range s.savedTags[userID] {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count != rows[j].Count {
			return rows[i].Count > rows[j].Count
		}
		if rows[i].Reaction.Type != rows[j].Reaction.Type {
			return rows[i].Reaction.Type < rows[j].Reaction.Type
		}
		return rows[i].Reaction.Emoticon < rows[j].Reaction.Emoticon
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (s *ChannelStore) UpsertSavedReactionTag(_ context.Context, tag domain.SavedReactionTag) error {
	if tag.UserID == 0 || tag.Reaction.Type != domain.MessageReactionEmoji || strings.TrimSpace(tag.Reaction.Emoticon) == "" {
		return domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.savedTags[tag.UserID] == nil {
		s.savedTags[tag.UserID] = make(map[string]domain.SavedReactionTag)
	}
	tag.Reaction.Emoticon = strings.TrimSpace(tag.Reaction.Emoticon)
	if tag.Count < 0 {
		tag.Count = 0
	}
	s.savedTags[tag.UserID][messageReactionKey(tag.Reaction)] = tag
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
	if req.IconColor == 0 {
		req.IconColor = domain.DefaultForumTopicIconColor
	}
	s.mu.Lock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		s.mu.Unlock()
		return domain.CreateChannelForumTopicResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	if !channel.Forum || channel.Broadcast || !channel.Megagroup {
		s.mu.Unlock()
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelForumMissing
	}
	if !canSendChannelMessage(channel, member) {
		s.mu.Unlock()
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelWriteForbidden
	}
	if id, ok := s.randomToID[channelRandomKey{channelID: req.ChannelID, userID: req.UserID, randomID: req.RandomID}]; ok {
		if topic, ok := s.topics[req.ChannelID][id]; ok {
			msg, _ := s.findMessageLocked(req.ChannelID, id)
			event := s.eventForMessageLocked(req.ChannelID, id)
			recipients := s.activeMemberIDsLocked(req.ChannelID, 0, 0)
			s.mu.Unlock()
			return domain.CreateChannelForumTopicResult{
				Channel:    cloneChannel(channel),
				Topic:      cloneChannelForumTopic(topic),
				Message:    cloneChannelMessage(msg),
				Event:      cloneChannelEvent(event),
				Recipients: recipients,
				Duplicate:  true,
			}, nil
		}
	}
	s.mu.Unlock()

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

	s.mu.Lock()
	defer s.mu.Unlock()
	channel = s.channels[req.ChannelID]
	if s.topics[req.ChannelID] == nil {
		s.topics[req.ChannelID] = make(map[int]domain.ChannelForumTopic)
	}
	topic, ok := s.topics[req.ChannelID][res.Message.ID]
	if !ok {
		topic = domain.ChannelForumTopic{
			ChannelID:       req.ChannelID,
			TopicID:         res.Message.ID,
			CreatorUserID:   req.UserID,
			Title:           title,
			IconColor:       req.IconColor,
			IconEmojiID:     req.IconEmojiID,
			TitleMissing:    req.TitleMissing,
			Date:            res.Message.Date,
			TopMessageID:    res.Message.ID,
			ReadInboxMaxID:  res.Message.ID,
			ReadOutboxMaxID: res.Message.ID,
		}
		s.topics[req.ChannelID][topic.TopicID] = topic
	}
	return domain.CreateChannelForumTopicResult{
		Channel:    cloneChannel(channel),
		Topic:      cloneChannelForumTopic(topic),
		Message:    cloneChannelMessage(res.Message),
		Event:      cloneChannelEvent(res.Event),
		Recipients: append([]int64(nil), res.Recipients...),
		Duplicate:  res.Duplicate,
	}, nil
}

func (s *ChannelStore) EditForumTopic(ctx context.Context, req domain.EditChannelForumTopicRequest) (domain.EditChannelForumTopicResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TopicID <= 0 {
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		s.mu.Unlock()
		return domain.EditChannelForumTopicResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	topic, ok := s.topics[req.ChannelID][req.TopicID]
	if !channel.Forum {
		s.mu.Unlock()
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelForumMissing
	}
	if !ok {
		s.mu.Unlock()
		return domain.EditChannelForumTopicResult{}, domain.ErrMessageIDInvalid
	}
	if !canManageForumTopic(channel, member, topic, req.UserID) {
		s.mu.Unlock()
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelAdminRequired
	}
	next := topic
	action := domain.ChannelMessageAction{Type: domain.ChannelActionTopicEdit}
	changed := false
	if req.Title != nil {
		title := strings.TrimSpace(*req.Title)
		if title == "" {
			s.mu.Unlock()
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
	s.mu.Unlock()
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

	s.mu.Lock()
	defer s.mu.Unlock()
	channel = s.channels[req.ChannelID]
	if _, ok := s.topics[req.ChannelID][req.TopicID]; !ok {
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelForumMissing
	}
	next.TopMessageID = maxInt(next.TopMessageID, res.Message.ID)
	s.topics[req.ChannelID][req.TopicID] = next
	return domain.EditChannelForumTopicResult{
		Channel:    cloneChannel(channel),
		Topic:      cloneChannelForumTopic(next),
		Message:    cloneChannelMessage(res.Message),
		Event:      cloneChannelEvent(res.Event),
		Recipients: append([]int64(nil), res.Recipients...),
	}, nil
}

func (s *ChannelStore) UpdatePinnedForumTopic(_ context.Context, req domain.UpdateChannelForumTopicPinnedRequest) (domain.UpdateChannelForumTopicPinnedResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TopicID <= 0 {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.UpdateChannelForumTopicPinnedResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	topic, ok := s.topics[req.ChannelID][req.TopicID]
	if !channel.Forum {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelForumMissing
	}
	if !ok {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrMessageIDInvalid
	}
	if !canPinChannelMessages(channel, member) {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelAdminRequired
	}
	if topic.Pinned == req.Pinned {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelNotModified
	}
	topic.Pinned = req.Pinned
	if req.Pinned && topic.PinnedOrder == 0 {
		topic.PinnedOrder = s.nextForumTopicPinnedOrderLocked(req.ChannelID)
	}
	if !req.Pinned {
		topic.PinnedOrder = 0
	}
	s.topics[req.ChannelID][req.TopicID] = topic
	return domain.UpdateChannelForumTopicPinnedResult{
		Channel:    cloneChannel(channel),
		Topic:      cloneChannelForumTopic(topic),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
	}, nil
}

func (s *ChannelStore) ReorderPinnedForumTopics(_ context.Context, req domain.ReorderChannelPinnedForumTopicsRequest) (domain.ReorderChannelPinnedForumTopicsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || len(req.Order) > domain.MaxChannelForumTopicIDs {
		return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReorderChannelPinnedForumTopicsResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
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
		topic, ok := s.topics[req.ChannelID][id]
		if !ok || !topic.Pinned {
			if req.Force {
				continue
			}
			return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrMessageIDInvalid
		}
		seen[id] = struct{}{}
		order = append(order, id)
	}
	for i, id := range order {
		topic := s.topics[req.ChannelID][id]
		topic.PinnedOrder = len(order) - i
		s.topics[req.ChannelID][id] = topic
	}
	return domain.ReorderChannelPinnedForumTopicsResult{
		Channel:    cloneChannel(channel),
		Order:      append([]int(nil), order...),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
	}, nil
}

func (s *ChannelStore) DeleteForumTopicHistory(_ context.Context, req domain.DeleteChannelForumTopicHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TopicID <= 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	topic, ok := s.topics[req.ChannelID][req.TopicID]
	if !channel.Forum {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelForumMissing
	}
	if !ok {
		return domain.DeleteChannelHistoryResult{}, domain.ErrMessageIDInvalid
	}
	if !canManageForumTopic(channel, member, topic, req.UserID) && !canDeleteAnyChannelMessage(member) {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelAdminRequired
	}
	ids := make([]int, 0, domain.MaxDeleteHistoryBatch)
	for i := len(s.messages[req.ChannelID]) - 1; i >= 0; i-- {
		msg := s.messages[req.ChannelID][i]
		if msg.Deleted {
			continue
		}
		if msg.ID != req.TopicID && (msg.ReplyTo == nil || msg.ReplyTo.TopMessageID != req.TopicID) {
			continue
		}
		ids = append(ids, msg.ID)
		if len(ids) >= domain.MaxDeleteHistoryBatch {
			break
		}
	}
	deleted, event, channel, err := s.deleteChannelMessagesLocked(channel, member, ids, req.UserID, req.Date)
	if err != nil {
		return domain.DeleteChannelHistoryResult{}, err
	}
	offset := 0
	if s.topicHasVisibleMessagesLocked(req.ChannelID, req.TopicID) {
		offset = 1
	} else {
		delete(s.topics[req.ChannelID], req.TopicID)
	}
	return domain.DeleteChannelHistoryResult{
		Channel:    cloneChannel(channel),
		Event:      cloneChannelEvent(event),
		DeletedIDs: append([]int(nil), deleted...),
		Recipients: s.activeMemberIDsLocked(req.ChannelID, 0, 0),
		Offset:     offset,
	}, nil
}

func (s *ChannelStore) ListForumTopics(_ context.Context, viewerUserID int64, filter domain.ChannelForumTopicFilter) (domain.ChannelForumTopicList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, err := s.channelAndMemberLocked(viewerUserID, filter.ChannelID)
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
	all := make([]domain.ChannelForumTopic, 0, len(s.topics[filter.ChannelID]))
	for _, topic := range s.topics[filter.ChannelID] {
		if topic.TopicID <= member.AvailableMinID {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(topic.Title), query) {
			continue
		}
		if forumTopicBeforeOrAtOffset(topic, filter) {
			continue
		}
		all = append(all, s.topicWithViewerCountersLocked(viewerUserID, filter.ChannelID, topic, member))
	}
	sortForumTopics(all)
	count := len(all)
	if len(all) > limit {
		all = all[:limit]
	}
	messages := s.forumTopicRootMessagesLocked(filter.ChannelID, all, member.AvailableMinID)
	return domain.ChannelForumTopicList{
		Channel:  cloneChannel(channel),
		Dialog:   s.dialogForUserLocked(viewerUserID, channel),
		Topics:   all,
		Messages: messages,
		Count:    count,
	}, nil
}

func (s *ChannelStore) GetForumTopicsByID(_ context.Context, viewerUserID, channelID int64, ids []int) (domain.ChannelForumTopicList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, err := s.channelAndMemberLocked(viewerUserID, channelID)
	if err != nil {
		return domain.ChannelForumTopicList{}, err
	}
	if !channel.Forum {
		return domain.ChannelForumTopicList{}, domain.ErrChannelForumMissing
	}
	wanted := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ChannelForumTopicList{}, domain.ErrMessageIDInvalid
		}
		wanted[id] = struct{}{}
	}
	topics := make([]domain.ChannelForumTopic, 0, len(wanted))
	for id := range wanted {
		topic, ok := s.topics[channelID][id]
		if !ok || topic.TopicID <= member.AvailableMinID {
			continue
		}
		topics = append(topics, s.topicWithViewerCountersLocked(viewerUserID, channelID, topic, member))
	}
	sortForumTopics(topics)
	messages := s.forumTopicRootMessagesLocked(channelID, topics, member.AvailableMinID)
	return domain.ChannelForumTopicList{
		Channel:  cloneChannel(channel),
		Dialog:   s.dialogForUserLocked(viewerUserID, channel),
		Topics:   topics,
		Messages: messages,
		Count:    len(topics),
	}, nil
}

func (s *ChannelStore) ListChannelReplies(_ context.Context, viewerUserID int64, filter domain.ChannelRepliesFilter) (domain.ChannelHistory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	source, member, err := s.channelAndMemberLocked(viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	root, ok := s.findMessageLocked(filter.ChannelID, filter.RootMessageID)
	if !ok || root.Deleted || root.ID <= member.AvailableMinID {
		return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
	}
	targetChannel := source
	targetMember := member
	rootID := root.ID
	extraChannels := []domain.Channel(nil)
	if source.Broadcast {
		if root.Discussion == nil || root.Discussion.ChannelID == 0 || root.Discussion.MessageID == 0 {
			return domain.ChannelHistory{Channel: source, Count: 0}, nil
		}
		linked, ok := s.channels[root.Discussion.ChannelID]
		if !ok || linked.Deleted {
			return domain.ChannelHistory{Channel: source, Count: 0}, nil
		}
		targetChannel = linked
		rootID = root.Discussion.MessageID
		if linkedMember, ok := s.members[linked.ID][viewerUserID]; ok {
			targetMember = linkedMember
		} else {
			targetMember = domain.ChannelMember{}
		}
		extraChannels = append(extraChannels, source)
	}
	if targetRoot, ok := s.findMessageLocked(targetChannel.ID, rootID); !ok || targetRoot.Deleted {
		return domain.ChannelHistory{Channel: targetChannel, Channels: extraChannels, Count: 0}, nil
	}
	limit := filter.Limit
	if limit <= 0 || limit > domain.MaxChannelRepliesLimit {
		limit = domain.MaxChannelRepliesLimit
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	base := make([]domain.ChannelMessage, 0, limit)
	for _, msg := range s.messages[targetChannel.ID] {
		if msg.Deleted || msg.ID <= targetMember.AvailableMinID {
			continue
		}
		if !channelReplyBelongsToRoot(msg, targetChannel.ID, rootID) {
			continue
		}
		if filter.MaxID > 0 && msg.ID >= filter.MaxID {
			continue
		}
		if filter.MinID > 0 && msg.ID <= filter.MinID {
			continue
		}
		if filter.OffsetDate > 0 && msg.Date == 0 {
			continue
		}
		base = append(base, msg)
	}
	sort.SliceStable(base, func(i, j int) bool { return channelMessageLess(base[i], base[j]) })
	page := pageChannelMessageHistory(base, filter, limit)
	out := make([]domain.ChannelMessage, 0, len(page))
	for _, msg := range page {
		out = append(out, cloneChannelMessage(msg))
	}
	s.populateChannelMessageRepliesLocked(viewerUserID, targetChannel.ID, out)
	s.populateChannelMessageReactionsLocked(viewerUserID, targetChannel, out)
	topics := []domain.ChannelForumTopic(nil)
	if targetChannel.Forum {
		if topic, ok := s.topics[targetChannel.ID][rootID]; ok && !topic.Hidden {
			topic = s.topicWithViewerCountersLocked(viewerUserID, targetChannel.ID, topic, targetMember)
			topics = append(topics, cloneChannelForumTopic(topic))
		}
	}
	return domain.ChannelHistory{Channel: targetChannel, Channels: extraChannels, Topics: topics, Messages: out, Count: len(base)}, nil
}

func (s *ChannelStore) ListChannelUnreadMentions(_ context.Context, viewerUserID int64, filter domain.ChannelUnreadMentionsFilter) (domain.ChannelHistory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, err := s.channelAndMemberLocked(viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > domain.MaxChannelUnreadMentionsLimit {
		limit = domain.MaxChannelUnreadMentionsLimit
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	base := make([]domain.ChannelMessage, 0, limit)
	for msgID, topID := range s.mentions[viewerUserID][filter.ChannelID] {
		if filter.TopMsgID > 0 && topID != filter.TopMsgID {
			continue
		}
		msg, ok := s.findMessageLocked(filter.ChannelID, msgID)
		if !ok || msg.Deleted || msg.ID <= member.AvailableMinID {
			continue
		}
		if filter.MaxID > 0 && msg.ID >= filter.MaxID {
			continue
		}
		if filter.MinID > 0 && msg.ID <= filter.MinID {
			continue
		}
		base = append(base, msg)
	}
	sort.SliceStable(base, func(i, j int) bool { return channelMessageLess(base[i], base[j]) })
	page := pageChannelMessageHistory(base, domain.ChannelRepliesFilter{
		OffsetID:   filter.OffsetID,
		OffsetDate: filter.OffsetDate,
		AddOffset:  filter.AddOffset,
		Limit:      limit,
		MaxID:      filter.MaxID,
		MinID:      filter.MinID,
	}, limit)
	out := make([]domain.ChannelMessage, 0, len(page))
	for _, msg := range page {
		out = append(out, cloneChannelMessage(msg))
	}
	s.populateChannelMessageRepliesLocked(viewerUserID, filter.ChannelID, out)
	s.populateChannelMessageReactionsLocked(viewerUserID, channel, out)
	return domain.ChannelHistory{Channel: channel, Messages: out, Count: len(base)}, nil
}

func (s *ChannelStore) ReadChannelMentions(_ context.Context, req domain.ReadChannelMentionsRequest) (domain.ReadChannelMentionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ReadChannelMentionsResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelMentionsResult{}, err
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelReadMentionsBatch {
		limit = domain.MaxChannelReadMentionsBatch
	}
	msgIDs := make([]int, 0, limit)
	for msgID, topID := range s.mentions[req.UserID][req.ChannelID] {
		if req.TopMsgID > 0 && topID != req.TopMsgID {
			continue
		}
		msgIDs = append(msgIDs, msgID)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(msgIDs)))
	if len(msgIDs) > limit {
		msgIDs = msgIDs[:limit]
	}
	for _, msgID := range msgIDs {
		delete(s.mentions[req.UserID][req.ChannelID], msgID)
	}
	remaining := s.countChannelUnreadMentionsLocked(req.UserID, req.ChannelID, req.TopMsgID)
	if dialogs := s.dialogs[req.UserID]; dialogs != nil {
		dialog := dialogs[req.ChannelID]
		dialog.UnreadMentions = s.countChannelUnreadMentionsLocked(req.UserID, req.ChannelID, 0)
		dialog.UserID = req.UserID
		dialog.ChannelID = req.ChannelID
		dialogs[req.ChannelID] = dialog
	}
	offset := 0
	if remaining > 0 {
		offset = 1
	}
	return domain.ReadChannelMentionsResult{
		Channel:    channel,
		Cleared:    len(msgIDs),
		Remaining:  remaining,
		Offset:     offset,
		ChannelPts: channel.Pts,
	}, nil
}

func (s *ChannelStore) ListChannelUnreadReactions(_ context.Context, viewerUserID int64, filter domain.ChannelUnreadReactionsFilter) (domain.ChannelHistory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, err := s.channelAndMemberLocked(viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > domain.MaxChannelUnreadReactionsLimit {
		limit = domain.MaxChannelUnreadReactionsLimit
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	base := make([]domain.ChannelMessage, 0, limit)
	for msgID, byUser := range s.reactions[filter.ChannelID] {
		msg, ok := s.findMessageLocked(filter.ChannelID, msgID)
		if !ok || msg.Deleted || msg.ID <= member.AvailableMinID {
			continue
		}
		if filter.TopMsgID > 0 && msg.ID != filter.TopMsgID && channelMentionTopID(msg) != filter.TopMsgID {
			continue
		}
		if filter.MaxID > 0 && msg.ID >= filter.MaxID {
			continue
		}
		if filter.MinID > 0 && msg.ID <= filter.MinID {
			continue
		}
		if !channelMessageHasUnreadReactionForUser(byUser, viewerUserID) {
			continue
		}
		base = append(base, msg)
	}
	sort.SliceStable(base, func(i, j int) bool { return channelMessageLess(base[i], base[j]) })
	page := pageChannelMessageHistory(base, domain.ChannelRepliesFilter{
		OffsetID:  filter.OffsetID,
		AddOffset: filter.AddOffset,
		Limit:     limit,
		MaxID:     filter.MaxID,
		MinID:     filter.MinID,
	}, limit)
	out := make([]domain.ChannelMessage, 0, len(page))
	for _, msg := range page {
		out = append(out, cloneChannelMessage(msg))
	}
	s.populateChannelMessageRepliesLocked(viewerUserID, filter.ChannelID, out)
	s.populateChannelMessageReactionsLocked(viewerUserID, channel, out)
	return domain.ChannelHistory{Channel: channel, Messages: out, Count: len(base)}, nil
}

func (s *ChannelStore) ReadChannelReactions(_ context.Context, req domain.ReadChannelReactionsRequest) (domain.ReadChannelReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ReadChannelReactionsResult{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelReactionsResult{}, err
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelReadReactionsBatch {
		limit = domain.MaxChannelReadReactionsBatch
	}
	msgIDs := make([]int, 0, limit)
	for msgID, byUser := range s.reactions[req.ChannelID] {
		msg, ok := s.findMessageLocked(req.ChannelID, msgID)
		if !ok || msg.Deleted {
			continue
		}
		if req.TopMsgID > 0 && msg.ID != req.TopMsgID && channelMentionTopID(msg) != req.TopMsgID {
			continue
		}
		if channelMessageHasUnreadReactionForUser(byUser, req.UserID) {
			msgIDs = append(msgIDs, msgID)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(msgIDs)))
	if len(msgIDs) > limit {
		msgIDs = msgIDs[:limit]
	}
	for _, msgID := range msgIDs {
		for reactedUserID, rows := range s.reactions[req.ChannelID][msgID] {
			changed := false
			for i := range rows {
				if rows[i].SenderUserID == req.UserID && rows[i].Unread {
					rows[i].Unread = false
					changed = true
				}
			}
			if changed {
				s.reactions[req.ChannelID][msgID][reactedUserID] = rows
			}
		}
	}
	remaining := s.countChannelUnreadReactionsLocked(req.UserID, req.ChannelID, req.TopMsgID)
	s.refreshChannelUnreadReactionsDialogLocked(req.UserID, req.ChannelID)
	offset := 0
	if remaining > 0 {
		offset = 1
	}
	return domain.ReadChannelReactionsResult{
		Channel:    channel,
		Cleared:    len(msgIDs),
		Remaining:  remaining,
		Offset:     offset,
		ChannelPts: channel.Pts,
	}, nil
}

func (s *ChannelStore) GetDiscussionMessage(_ context.Context, viewerUserID, channelID int64, msgID int) (domain.ChannelDiscussionMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	source, member, err := s.channelAndMemberLocked(viewerUserID, channelID)
	if err != nil {
		return domain.ChannelDiscussionMessage{}, err
	}
	msg, ok := s.findMessageLocked(channelID, msgID)
	if !ok || msg.Deleted || msg.ID <= member.AvailableMinID {
		return domain.ChannelDiscussionMessage{}, domain.ErrMessageIDInvalid
	}
	result := domain.ChannelDiscussionMessage{PostChannel: source, DiscussionChannel: source, Channels: []domain.Channel{source}}
	targetChannel := source
	targetMsg := msg
	targetMember := member
	if source.Broadcast {
		if msg.Discussion == nil || msg.Discussion.ChannelID == 0 || msg.Discussion.MessageID == 0 {
			return result, nil
		}
		linked, ok := s.channels[msg.Discussion.ChannelID]
		if !ok || linked.Deleted {
			return result, nil
		}
		linkedMsg, ok := s.findMessageLocked(linked.ID, msg.Discussion.MessageID)
		if !ok || linkedMsg.Deleted {
			return result, nil
		}
		targetChannel = linked
		targetMsg = linkedMsg
		if linkedMember, ok := s.members[linked.ID][viewerUserID]; ok {
			targetMember = linkedMember
		} else {
			targetMember = domain.ChannelMember{}
		}
		result.DiscussionChannel = linked
		result.Channels = []domain.Channel{source, linked}
	}
	items := []domain.ChannelMessage{cloneChannelMessage(targetMsg)}
	s.populateChannelMessageRepliesLocked(viewerUserID, targetChannel.ID, items)
	s.populateChannelMessageReactionsLocked(viewerUserID, targetChannel, items)
	if stats := s.channelMessageRepliesLocked(viewerUserID, targetChannel.ID, targetMsg); stats != nil {
		result.MaxID = stats.MaxID
	}
	result.ReadInboxMaxID = targetMember.ReadInboxMaxID
	result.ReadOutboxMaxID = targetMember.ReadOutboxMaxID
	result.UnreadCount = s.channelThreadUnreadCountLocked(viewerUserID, targetChannel.ID, targetMsg.ID, targetMember.ReadInboxMaxID)
	result.Messages = items
	return result, nil
}

func (s *ChannelStore) ReadChannelHistory(_ context.Context, req domain.ReadChannelHistoryRequest) (domain.ReadChannelHistoryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelHistoryResult{}, err
	}
	maxID := req.MaxID
	if maxID <= 0 || maxID > channel.TopMessageID {
		maxID = channel.TopMessageID
	}
	member := s.members[req.ChannelID][req.UserID]
	previous := member.ReadInboxMaxID
	changed := maxID > member.ReadInboxMaxID
	var outboxUpdates []domain.ChannelReadOutboxUpdate
	if changed {
		member.ReadInboxMaxID = maxID
		member.ReadInboxDate = req.Date
		member.UnreadMark = false
		s.members[req.ChannelID][req.UserID] = member
		outboxUpdates = s.advanceChannelReadOutboxLocked(req.ChannelID, req.UserID, previous, maxID)
	}
	dialog := s.dialogForUserLocked(req.UserID, channel)
	dialog.ReadInboxMaxID = member.ReadInboxMaxID
	dialog.UnreadCount = s.channelUnreadCountLocked(req.UserID, channel.ID, member.ReadInboxMaxID, dialog.TopMessageID)
	dialog.UnreadMark = false
	if s.dialogs[req.UserID] == nil {
		s.dialogs[req.UserID] = make(map[int64]domain.ChannelDialog)
	}
	s.dialogs[req.UserID][req.ChannelID] = dialog
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

func (s *ChannelStore) advanceChannelReadOutboxLocked(channelID, readerUserID int64, previous, maxID int) []domain.ChannelReadOutboxUpdate {
	if maxID <= previous {
		return nil
	}
	lowerID := previous
	if maxID-lowerID > domain.MaxChannelReadOutboxScanMessages {
		lowerID = maxID - domain.MaxChannelReadOutboxScanMessages
	}
	bySender := make(map[int64]int, domain.MaxChannelReadOutboxFanout)
	messages := s.messages[channelID]
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		if msg.ID <= lowerID {
			break
		}
		if msg.ID > maxID || msg.Deleted || msg.SenderUserID == 0 || msg.SenderUserID == readerUserID {
			continue
		}
		if _, ok := bySender[msg.SenderUserID]; ok {
			continue
		}
		bySender[msg.SenderUserID] = msg.ID
		if len(bySender) >= domain.MaxChannelReadOutboxFanout {
			break
		}
	}
	if len(bySender) == 0 {
		return nil
	}
	senderIDs := make([]int64, 0, len(bySender))
	for userID := range bySender {
		senderIDs = append(senderIDs, userID)
	}
	sort.Slice(senderIDs, func(i, j int) bool { return senderIDs[i] < senderIDs[j] })
	channel := s.channels[channelID]
	out := make([]domain.ChannelReadOutboxUpdate, 0, len(senderIDs))
	for _, userID := range senderIDs {
		maxForSender := bySender[userID]
		member, ok := s.members[channelID][userID]
		if !ok || member.Status != domain.ChannelMemberActive || maxForSender <= member.ReadOutboxMaxID {
			continue
		}
		member.ReadOutboxMaxID = maxForSender
		s.members[channelID][userID] = member
		dialog := s.dialogForUserLocked(userID, channel)
		if dialog.ReadOutboxMaxID < maxForSender {
			dialog.ReadOutboxMaxID = maxForSender
		}
		if s.dialogs[userID] == nil {
			s.dialogs[userID] = make(map[int64]domain.ChannelDialog)
		}
		s.dialogs[userID][channelID] = dialog
		out = append(out, domain.ChannelReadOutboxUpdate{UserID: userID, MaxID: maxForSender})
	}
	return out
}

func (s *ChannelStore) ListMessageReadParticipants(_ context.Context, req domain.ChannelReadParticipantsRequest) (domain.ChannelReadParticipantsResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, err := s.channelForMemberLocked(req.UserID, req.ChannelID)
	if err != nil {
		return domain.ChannelReadParticipantsResult{}, err
	}
	member := s.members[req.ChannelID][req.UserID]
	msg, found := s.findMessageLocked(req.ChannelID, req.MessageID)
	if !found || msg.Deleted || msg.ID <= member.AvailableMinID {
		return domain.ChannelReadParticipantsResult{}, domain.ErrMessageIDInvalid
	}
	result := domain.ChannelReadParticipantsResult{
		Channel: channel,
		Message: cloneChannelMessage(msg),
	}
	if !channel.Megagroup || channel.ParticipantsHidden || channel.ParticipantsCount > domain.MaxChannelReadParticipants {
		return result, nil
	}
	now := req.Date
	if now > 0 && msg.Date+domain.ChannelReadMarkExpirePeriod <= now {
		return result, nil
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelReadParticipants {
		limit = domain.MaxChannelReadParticipants
	}
	for _, reader := range s.members[req.ChannelID] {
		if reader.UserID == req.UserID || reader.Status != domain.ChannelMemberActive || reader.BannedRights.ViewMessages {
			continue
		}
		if reader.ReadInboxDate <= 0 {
			continue
		}
		if reader.AvailableMinID >= req.MessageID || reader.ReadInboxMaxID < req.MessageID {
			continue
		}
		result.Participants = append(result.Participants, domain.ChannelReadParticipant{
			UserID: reader.UserID,
			Date:   reader.ReadInboxDate,
		})
		if len(result.Participants) >= limit {
			break
		}
	}
	sort.Slice(result.Participants, func(i, j int) bool {
		if result.Participants[i].Date == result.Participants[j].Date {
			return result.Participants[i].UserID < result.Participants[j].UserID
		}
		return result.Participants[i].Date < result.Participants[j].Date
	})
	return result, nil
}

func (s *ChannelStore) ListChannelDifference(_ context.Context, req domain.ChannelDifferenceRequest) (domain.ChannelDifference, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, member, preview, err := s.channelForViewerLocked(req.UserID, req.ChannelID)
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
	dialog := s.dialogForUserLocked(req.UserID, channel)
	if preview {
		dialog = previewChannelDialog(req.UserID, channel, member)
	}
	if channel.Pts-req.Pts > limit {
		messages := make([]domain.ChannelMessage, 0, domain.MaxChannelDifferenceTooLongMessages)
		for i := len(s.messages[req.ChannelID]) - 1; i >= 0 && len(messages) < domain.MaxChannelDifferenceTooLongMessages; i-- {
			msg := s.messages[req.ChannelID][i]
			if msg.Deleted {
				continue
			}
			if msg.ID <= member.AvailableMinID {
				continue
			}
			messages = append(messages, cloneChannelMessage(msg))
		}
		s.populateChannelMessageUnreadFlagsLocked(req.UserID, messages)
		return domain.ChannelDifference{
			Channel:     channel,
			Self:        member,
			NewMessages: messages,
			Pts:         channel.Pts,
			Final:       true,
			TooLong:     true,
			Timeout:     30,
			Dialog:      dialog,
		}, nil
	}
	events := make([]domain.ChannelUpdateEvent, 0, limit)
	lastPts := req.Pts
	for _, event := range s.events[req.ChannelID] {
		if event.Pts <= req.Pts {
			continue
		}
		lastPts = event.Pts
		visible, ok := domain.FilterChannelUpdateEventForAvailableMinID(cloneChannelEvent(event), member.AvailableMinID)
		if !ok {
			continue
		}
		if preview && visible.Type == domain.ChannelUpdateParticipant {
			continue
		}
		events = append(events, visible)
	}
	if len(events) == 0 {
		return domain.ChannelDifference{
			Channel: channel,
			Self:    member,
			Pts:     maxInt(lastPts, req.Pts),
			Final:   true,
			Timeout: 30,
			Dialog:  dialog,
		}, nil
	}
	diff := domain.ChannelDifference{
		Channel: channel,
		Self:    member,
		Events:  events,
		Pts:     lastPts,
		Final:   lastPts >= channel.Pts,
		Timeout: 30,
		Dialog:  dialog,
	}
	for _, event := range events {
		switch event.Type {
		case domain.ChannelUpdateNewMessage:
			diff.NewMessages = append(diff.NewMessages, cloneChannelMessage(event.Message))
		default:
			diff.OtherUpdates = append(diff.OtherUpdates, cloneChannelEvent(event))
		}
	}
	s.populateChannelMessageUnreadFlagsLocked(req.UserID, diff.NewMessages)
	for i := range diff.OtherUpdates {
		if diff.OtherUpdates[i].Message.ID == 0 {
			continue
		}
		messages := []domain.ChannelMessage{diff.OtherUpdates[i].Message}
		s.populateChannelMessageUnreadFlagsLocked(req.UserID, messages)
		diff.OtherUpdates[i].Message = messages[0]
	}
	return diff, nil
}

func (s *ChannelStore) ListActiveChannelIDsForUser(_ context.Context, userID, afterChannelID int64, limit int) ([]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if userID == 0 || afterChannelID < 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxSynchronousChannelDialogFanout {
		limit = domain.MaxSynchronousChannelDialogFanout
	}
	out := make([]int64, 0, limit)
	for channelID, members := range s.members {
		if channelID <= afterChannelID {
			continue
		}
		channel, ok := s.channels[channelID]
		if !ok || channel.Deleted {
			continue
		}
		member, ok := members[userID]
		if !ok || member.Status != domain.ChannelMemberActive {
			continue
		}
		out = append(out, channelID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *ChannelStore) ListDirtyActiveChannelsForUser(_ context.Context, userID int64, sinceDate int, afterChannelID int64, limit int) ([]domain.DirtyChannel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if userID == 0 || sinceDate <= 0 || afterChannelID < 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxChannelDifferenceLimit {
		limit = domain.MaxChannelDifferenceLimit
	}
	out := make([]domain.DirtyChannel, 0, limit)
	for channelID, members := range s.members {
		if channelID <= afterChannelID {
			continue
		}
		channel, ok := s.channels[channelID]
		if !ok || channel.Deleted {
			continue
		}
		member, ok := members[userID]
		if !ok || member.Status != domain.ChannelMemberActive {
			continue
		}
		dirty := false
		for _, event := range s.events[channelID] {
			if event.Date > sinceDate {
				dirty = true
				break
			}
		}
		if dirty {
			out = append(out, domain.DirtyChannel{ChannelID: channelID, Pts: channel.Pts})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ChannelID < out[j].ChannelID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *ChannelStore) ListActiveChannelMemberIDs(_ context.Context, viewerUserID, channelID int64, limit int) ([]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.channelForMemberLocked(viewerUserID, channelID); err != nil {
		return nil, err
	}
	return s.activeMemberIDsLocked(channelID, 0, limit), nil
}

func (s *ChannelStore) ListChannelInviteAdminMemberIDs(_ context.Context, channelID int64, limit int) ([]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	channel, ok := s.channels[channelID]
	if channelID == 0 || !ok || channel.Deleted {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxChannelRealtimeFanout {
		limit = domain.MaxChannelRealtimeFanout
	}
	members := s.members[channelID]
	out := make([]int64, 0, minInt(len(members), limit))
	for _, member := range members {
		if member.Status != domain.ChannelMemberActive {
			continue
		}
		if member.Role == domain.ChannelRoleCreator {
			out = append(out, member.UserID)
			if len(out) >= limit {
				break
			}
			continue
		}
		if member.Role == domain.ChannelRoleAdmin && (member.AdminRights.InviteUsers || member.AdminRights.ChangeInfo) {
			out = append(out, member.UserID)
			if len(out) >= limit {
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (s *ChannelStore) FilterActiveChannelMemberIDs(_ context.Context, channelID int64, userIDs []int64) ([]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if channelID == 0 || len(userIDs) == 0 {
		return nil, nil
	}
	members := s.members[channelID]
	if len(members) == 0 {
		return nil, nil
	}
	out := make([]int64, 0, len(userIDs))
	seen := make(map[int64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		member, ok := members[userID]
		if !ok || member.Status != domain.ChannelMemberActive {
			continue
		}
		out = append(out, userID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (s *ChannelStore) MaxChannelPts(_ context.Context, channelID int64) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ptsSeq[channelID], nil
}

func (s *ChannelStore) MaxChannelMessageID(_ context.Context, channelID int64) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.msgSeq[channelID], nil
}

func (s *ChannelStore) nextChannelIDLocked() int64 {
	id := s.nextID
	s.nextID++
	return id
}

func (s *ChannelStore) nextAccessHashLocked() int64 {
	hash := s.nextHash
	s.nextHash += 17
	return hash
}

func (s *ChannelStore) nextChannelMessageIDLocked(channelID int64) int {
	s.msgSeq[channelID]++
	return s.msgSeq[channelID]
}

func (s *ChannelStore) nextChannelPtsLocked(channelID int64) int {
	s.ptsSeq[channelID]++
	return s.ptsSeq[channelID]
}

func (s *ChannelStore) nextChannelPtsNLocked(channelID int64, count int) int {
	if count <= 0 {
		return s.ptsSeq[channelID]
	}
	s.ptsSeq[channelID] += count
	return s.ptsSeq[channelID]
}

func (s *ChannelStore) appendChannelServiceMessageLocked(channelID, senderUserID int64, date int, action domain.ChannelMessageAction) (domain.ChannelMessage, domain.ChannelUpdateEvent) {
	channel := s.channels[channelID]
	pts := s.nextChannelPtsLocked(channelID)
	msg := domain.ChannelMessage{
		ChannelID:    channelID,
		ID:           s.nextChannelMessageIDLocked(channelID),
		SenderUserID: senderUserID,
		From:         domain.Peer{Type: domain.PeerTypeUser, ID: senderUserID},
		Date:         date,
		Post:         channel.Broadcast,
		Action:       &action,
		Pts:          pts,
	}
	event := domain.ChannelUpdateEvent{
		ChannelID:    channelID,
		Type:         domain.ChannelUpdateNewMessage,
		Pts:          pts,
		PtsCount:     1,
		Date:         date,
		Message:      cloneChannelMessage(msg),
		SenderUserID: senderUserID,
		UserIDs:      append([]int64(nil), action.UserIDs...),
	}
	s.messages[channelID] = append(s.messages[channelID], msg)
	s.events[channelID] = append(s.events[channelID], event)
	return msg, event
}

func transientChannelParticipantEvent(channelID, actorUserID int64, previous, participant domain.ChannelMember, date int) domain.ChannelUpdateEvent {
	return domain.ChannelUpdateEvent{
		ChannelID:    channelID,
		Type:         domain.ChannelUpdateParticipant,
		Date:         date,
		SenderUserID: actorUserID,
		UserIDs:      uniqueNonZeroInt64s(actorUserID, previous.UserID, previous.InviterUserID, participant.UserID, participant.InviterUserID),
		Previous:     previous,
		Participant:  participant,
	}
}

func (s *ChannelStore) channelForMemberLocked(userID, channelID int64) (domain.Channel, error) {
	channel, _, err := s.channelAndMemberLocked(userID, channelID)
	return channel, err
}

func (s *ChannelStore) channelForViewerLocked(userID, channelID int64) (domain.Channel, domain.ChannelMember, bool, error) {
	channel, member, err := s.channelAndMemberLocked(userID, channelID)
	if err == nil {
		return channel, member, false, nil
	}
	if !errors.Is(err, domain.ErrChannelPrivate) {
		return domain.Channel{}, domain.ChannelMember{}, false, err
	}
	channel, ok := s.channels[channelID]
	if !ok || channel.Deleted {
		return domain.Channel{}, domain.ChannelMember{}, false, domain.ErrChannelInvalid
	}
	existing, found := s.members[channelID][userID]
	if found && (existing.Status == domain.ChannelMemberBanned || existing.Status == domain.ChannelMemberKicked || existing.BannedRights.ViewMessages) {
		return domain.Channel{}, domain.ChannelMember{}, false, domain.ErrChannelUserBanned
	}
	if !publicPreviewableChannel(channel) {
		return domain.Channel{}, domain.ChannelMember{}, false, domain.ErrChannelPrivate
	}
	return channel, publicPreviewMember(channel, userID, existing, found), true, nil
}

func (s *ChannelStore) channelAndMemberLocked(userID, channelID int64) (domain.Channel, domain.ChannelMember, error) {
	channel, ok := s.channels[channelID]
	if !ok || channel.Deleted {
		return domain.Channel{}, domain.ChannelMember{}, domain.ErrChannelInvalid
	}
	member, ok := s.members[channelID][userID]
	if !ok || member.Status == domain.ChannelMemberLeft {
		return domain.Channel{}, domain.ChannelMember{}, domain.ErrChannelPrivate
	}
	if member.Status == domain.ChannelMemberBanned || member.Status == domain.ChannelMemberKicked || member.BannedRights.ViewMessages {
		return domain.Channel{}, domain.ChannelMember{}, domain.ErrChannelUserBanned
	}
	return channel, member, nil
}

func (s *ChannelStore) upsertChannelDialogLocked(userID int64, channel domain.Channel, top domain.ChannelMessage, selfAction bool) {
	if s.dialogs[userID] == nil {
		s.dialogs[userID] = make(map[int64]domain.ChannelDialog)
	}
	dialog := s.dialogs[userID][channel.ID]
	dialog.UserID = userID
	dialog.ChannelID = channel.ID
	dialog.TopMessageID = s.visibleTopMessageIDLocked(userID, channel)
	if top.ID != 0 {
		dialog.TopMessageDate = top.Date
	}
	member := s.members[channel.ID][userID]
	if member.ReadInboxMaxID > dialog.ReadInboxMaxID {
		dialog.ReadInboxMaxID = member.ReadInboxMaxID
	}
	if selfAction {
		if channel.TopMessageID > dialog.ReadInboxMaxID {
			dialog.ReadInboxMaxID = channel.TopMessageID
		}
		dialog.ReadOutboxMaxID = channel.TopMessageID
	}
	dialog.UnreadCount = s.channelUnreadCountLocked(userID, channel.ID, dialog.ReadInboxMaxID, dialog.TopMessageID)
	dialog.UnreadMentions = s.countChannelUnreadMentionsLocked(userID, channel.ID, 0)
	dialog.UnreadReactions = s.countChannelUnreadReactionsLocked(userID, channel.ID, 0)
	s.dialogs[userID][channel.ID] = dialog
}

func (s *ChannelStore) dialogForUserLocked(userID int64, channel domain.Channel) domain.ChannelDialog {
	dialog := s.dialogs[userID][channel.ID]
	dialog.UserID = userID
	dialog.ChannelID = channel.ID
	member := s.members[channel.ID][userID]
	dialog.TopMessageID = s.visibleTopMessageIDForMemberLocked(channel, member)
	if member.ReadInboxMaxID > dialog.ReadInboxMaxID {
		dialog.ReadInboxMaxID = member.ReadInboxMaxID
	}
	dialog.UnreadCount = s.channelUnreadCountLocked(userID, channel.ID, dialog.ReadInboxMaxID, dialog.TopMessageID)
	dialog.UnreadMentions = s.countChannelUnreadMentionsLocked(userID, channel.ID, 0)
	dialog.UnreadReactions = s.countChannelUnreadReactionsLocked(userID, channel.ID, 0)
	return dialog
}

func (s *ChannelStore) findMessageLocked(channelID int64, id int) (domain.ChannelMessage, bool) {
	for _, msg := range s.messages[channelID] {
		if msg.ID == id {
			return msg, true
		}
	}
	return domain.ChannelMessage{}, false
}

func (s *ChannelStore) findMessageIndexLocked(channelID int64, id int) (int, bool) {
	for i, msg := range s.messages[channelID] {
		if msg.ID == id {
			return i, true
		}
	}
	return 0, false
}

func (s *ChannelStore) populateChannelMessageRepliesLocked(viewerUserID, channelID int64, messages []domain.ChannelMessage) {
	for i := range messages {
		messages[i].Replies = s.channelMessageRepliesLocked(viewerUserID, channelID, messages[i])
	}
}

func (s *ChannelStore) channelMessageRepliesLocked(viewerUserID, channelID int64, msg domain.ChannelMessage) *domain.ChannelMessageReplies {
	targetChannelID := channelID
	rootID := msg.ID
	stats := domain.ChannelMessageReplies{}
	if msg.Discussion != nil && msg.Discussion.ChannelID != 0 && msg.Discussion.MessageID != 0 {
		targetChannelID = msg.Discussion.ChannelID
		rootID = msg.Discussion.MessageID
		stats.Comments = true
		stats.ChannelID = msg.Discussion.ChannelID
	} else if channel, ok := s.channels[channelID]; ok && channel.Broadcast && channel.LinkedChatID != 0 && msg.Post {
		stats.Comments = true
		stats.ChannelID = channel.LinkedChatID
	}
	if rootID <= 0 {
		return nil
	}
	if member, ok := s.members[targetChannelID][viewerUserID]; ok {
		stats.ReadMaxID = member.ReadInboxMaxID
	}
	seenRecent := map[domain.Peer]struct{}{}
	for i := len(s.messages[targetChannelID]) - 1; i >= 0; i-- {
		reply := s.messages[targetChannelID][i]
		if reply.Deleted || !channelReplyBelongsToRoot(reply, targetChannelID, rootID) {
			continue
		}
		stats.Replies++
		if stats.MaxID == 0 || reply.ID > stats.MaxID {
			stats.MaxID = reply.ID
			stats.RepliesPts = reply.Pts
		}
		if len(stats.RecentRepliers) < 3 {
			peer := reply.From
			if peer.ID == 0 && reply.SenderUserID != 0 {
				peer = domain.Peer{Type: domain.PeerTypeUser, ID: reply.SenderUserID}
			}
			if peer.ID != 0 {
				if _, ok := seenRecent[peer]; !ok {
					seenRecent[peer] = struct{}{}
					stats.RecentRepliers = append(stats.RecentRepliers, peer)
				}
			}
		}
	}
	if stats.Comments && stats.RepliesPts == 0 {
		if root, ok := s.findMessageLocked(targetChannelID, rootID); ok {
			stats.RepliesPts = root.Pts
		}
	}
	if !stats.Comments && stats.Replies == 0 {
		return nil
	}
	return &stats
}

func (s *ChannelStore) channelThreadUnreadCountLocked(viewerUserID, channelID int64, rootID, readMaxID int) int {
	unread := 0
	for _, msg := range s.messages[channelID] {
		if msg.Deleted || msg.ID <= readMaxID || msg.SenderUserID == viewerUserID {
			continue
		}
		if channelReplyBelongsToRoot(msg, channelID, rootID) {
			unread++
		}
	}
	return unread
}

func (s *ChannelStore) channelUnreadCountLocked(viewerUserID, channelID int64, readMaxID, topID int) int {
	if viewerUserID == 0 || channelID == 0 || topID <= readMaxID {
		return 0
	}
	unread := 0
	for _, msg := range s.messages[channelID] {
		if msg.Deleted || msg.ID <= readMaxID || msg.ID > topID || msg.SenderUserID == viewerUserID {
			continue
		}
		unread++
	}
	return unread
}

func (s *ChannelStore) addChannelUnreadMentionsLocked(channelID int64, msg domain.ChannelMessage, senderUserID int64, userIDs []int64) {
	if len(userIDs) == 0 || msg.ID == 0 {
		return
	}
	seen := make(map[int64]struct{}, len(userIDs))
	written := 0
	topID := channelMentionTopID(msg)
	for _, userID := range userIDs {
		if userID == 0 || userID == senderUserID {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		member, ok := s.members[channelID][userID]
		if !ok || member.Status != domain.ChannelMemberActive || member.BannedRights.ViewMessages {
			continue
		}
		if msg.ID <= member.AvailableMinID || msg.ID <= member.ReadInboxMaxID {
			continue
		}
		if s.mentions[userID] == nil {
			s.mentions[userID] = make(map[int64]map[int]int)
		}
		if s.mentions[userID][channelID] == nil {
			s.mentions[userID][channelID] = make(map[int]int)
		}
		s.mentions[userID][channelID][msg.ID] = topID
		written++
		if written == domain.MaxChannelMentionRecipients {
			return
		}
	}
}

func (s *ChannelStore) countChannelUnreadMentionsLocked(userID, channelID int64, topMsgID int) int {
	count := 0
	for _, mentionTopID := range s.mentions[userID][channelID] {
		if topMsgID == 0 || mentionTopID == topMsgID {
			count++
		}
	}
	return count
}

func (s *ChannelStore) countChannelUnreadReactionsLocked(userID, channelID int64, topMsgID int) int {
	count := 0
	availableMinID := 0
	if member, ok := s.members[channelID][userID]; ok {
		availableMinID = member.AvailableMinID
	}
	for msgID, byUser := range s.reactions[channelID] {
		msg, ok := s.findMessageLocked(channelID, msgID)
		if !ok || msg.Deleted || msg.ID <= availableMinID {
			continue
		}
		if topMsgID > 0 && msg.ID != topMsgID && channelMentionTopID(msg) != topMsgID {
			continue
		}
		if channelMessageHasUnreadReactionForUser(byUser, userID) {
			count++
		}
	}
	return count
}

func channelMessageHasUnreadReactionForUser(byUser map[int64][]domain.ChannelMessagePeerReaction, userID int64) bool {
	for _, rows := range byUser {
		for _, row := range rows {
			if row.SenderUserID == userID && row.UserID != userID && row.Unread {
				return true
			}
		}
	}
	return false
}

func (s *ChannelStore) refreshChannelUnreadReactionsDialogLocked(userID, channelID int64) {
	if userID == 0 || channelID == 0 {
		return
	}
	channel, ok := s.channels[channelID]
	if !ok {
		return
	}
	member, ok := s.members[channelID][userID]
	if !ok || member.Status != domain.ChannelMemberActive || member.BannedRights.ViewMessages {
		return
	}
	dialog := s.dialogForUserLocked(userID, channel)
	dialog.UnreadReactions = s.countChannelUnreadReactionsLocked(userID, channelID, 0)
	if s.dialogs[userID] == nil {
		s.dialogs[userID] = make(map[int64]domain.ChannelDialog)
	}
	s.dialogs[userID][channelID] = dialog
}

func (s *ChannelStore) deleteChannelUnreadMentionsLocked(channelID int64, ids []int) {
	if len(ids) == 0 {
		return
	}
	set := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	for userID, byChannel := range s.mentions {
		mentions := byChannel[channelID]
		if len(mentions) == 0 {
			continue
		}
		for id := range set {
			delete(mentions, id)
		}
		if len(mentions) == 0 {
			delete(byChannel, channelID)
		}
		if len(byChannel) == 0 {
			delete(s.mentions, userID)
		}
	}
}

func (s *ChannelStore) deleteChannelUnreadMentionsUpToLocked(userID, channelID int64, maxID int) {
	if maxID <= 0 || len(s.mentions[userID][channelID]) == 0 {
		return
	}
	for id := range s.mentions[userID][channelID] {
		if id <= maxID {
			delete(s.mentions[userID][channelID], id)
		}
	}
	if len(s.mentions[userID][channelID]) == 0 {
		delete(s.mentions[userID], channelID)
	}
	if len(s.mentions[userID]) == 0 {
		delete(s.mentions, userID)
	}
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

func channelReplyBelongsToRoot(msg domain.ChannelMessage, channelID int64, rootID int) bool {
	if msg.ReplyTo == nil || rootID <= 0 {
		return false
	}
	if msg.ReplyTo.Peer.ID != 0 && msg.ReplyTo.Peer != (domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}) {
		return false
	}
	return msg.ReplyTo.TopMessageID == rootID || (msg.ReplyTo.TopMessageID == 0 && msg.ReplyTo.MessageID == rootID)
}

func pageChannelMessageHistory(base []domain.ChannelMessage, filter domain.ChannelRepliesFilter, limit int) []domain.ChannelMessage {
	if limit <= 0 || len(base) == 0 {
		return nil
	}
	switch messageHistoryLoadType(filter.AddOffset, limit) {
	case messageHistoryLoadForward:
		return forwardChannelMessageHistory(base, filter, limit)
	case messageHistoryLoadAround:
		forwardLimit := -filter.AddOffset
		if forwardLimit > limit {
			forwardLimit = limit
		}
		backwardLimit := limit + filter.AddOffset
		if backwardLimit < 0 {
			backwardLimit = 0
		}
		page := make([]domain.ChannelMessage, 0, limit)
		page = append(page, forwardChannelMessageHistory(base, filter, forwardLimit)...)
		page = append(page, backwardChannelMessageHistory(base, filter, backwardLimit, true)...)
		sort.SliceStable(page, func(i, j int) bool { return channelMessageLess(page[i], page[j]) })
		return page
	default:
		start := filter.AddOffset
		if start < 0 {
			start = 0
		}
		candidates := backwardChannelMessageHistory(base, filter, limit+start, false)
		if start >= len(candidates) {
			return nil
		}
		return candidates[start:]
	}
}

func backwardChannelMessageHistory(base []domain.ChannelMessage, filter domain.ChannelRepliesFilter, limit int, includeOffset bool) []domain.ChannelMessage {
	if limit <= 0 {
		return nil
	}
	out := make([]domain.ChannelMessage, 0, limit)
	for _, msg := range base {
		if !channelMessageBeforeHistoryOffset(msg, filter, includeOffset) {
			continue
		}
		out = append(out, msg)
		if len(out) == limit {
			break
		}
	}
	return out
}

func forwardChannelMessageHistory(base []domain.ChannelMessage, filter domain.ChannelRepliesFilter, limit int) []domain.ChannelMessage {
	if limit <= 0 {
		return nil
	}
	out := make([]domain.ChannelMessage, 0, limit)
	for i := len(base) - 1; i >= 0; i-- {
		msg := base[i]
		if !channelMessageAfterHistoryOffset(msg, filter) {
			continue
		}
		out = append(out, msg)
		if len(out) == limit {
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
	return out
}

func channelMessageBeforeHistoryOffset(msg domain.ChannelMessage, filter domain.ChannelRepliesFilter, includeOffset bool) bool {
	if filter.OffsetDate > 0 {
		if includeOffset {
			return msg.Date <= filter.OffsetDate
		}
		return msg.Date < filter.OffsetDate
	}
	if filter.OffsetID <= 0 {
		return true
	}
	if includeOffset {
		return msg.ID <= filter.OffsetID
	}
	return msg.ID < filter.OffsetID
}

func channelMessageAfterHistoryOffset(msg domain.ChannelMessage, filter domain.ChannelRepliesFilter) bool {
	if filter.OffsetDate > 0 {
		return msg.Date >= filter.OffsetDate
	}
	if filter.OffsetID <= 0 {
		return false
	}
	return msg.ID > filter.OffsetID
}

func channelMessageLess(a, b domain.ChannelMessage) bool {
	if a.Date != b.Date {
		return a.Date > b.Date
	}
	return a.ID > b.ID
}

func (s *ChannelStore) resolveChannelReplyLocked(req domain.SendChannelMessageRequest, member domain.ChannelMember, channel domain.Channel) (*domain.MessageReply, error) {
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
		topic, ok := s.topics[req.ChannelID][req.ReplyTo.TopMessageID]
		if !ok || topic.Hidden {
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
	target, ok := s.findMessageLocked(req.ChannelID, req.ReplyTo.MessageID)
	if !ok || target.Deleted || target.ID <= member.AvailableMinID {
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
		if topic, ok := s.topics[req.ChannelID][reply.TopMessageID]; ok && !topic.Hidden {
			if topic.Closed && !canManageForumTopic(channel, member, topic, req.UserID) {
				return nil, domain.ErrChannelWriteForbidden
			}
			reply.ForumTopic = true
		}
	}
	return reply, nil
}

func (s *ChannelStore) visibleTopMessageIDLocked(userID int64, channel domain.Channel) int {
	return s.visibleTopMessageIDForMemberLocked(channel, s.members[channel.ID][userID])
}

func (s *ChannelStore) visibleTopMessageIDForMemberLocked(channel domain.Channel, member domain.ChannelMember) int {
	for i := len(s.messages[channel.ID]) - 1; i >= 0; i-- {
		msg := s.messages[channel.ID][i]
		if !msg.Deleted && msg.ID > member.AvailableMinID {
			return msg.ID
		}
	}
	return 0
}

func (s *ChannelStore) deleteChannelMessagesLocked(channel domain.Channel, member domain.ChannelMember, ids []int, actorUserID int64, date int) ([]int, domain.ChannelUpdateEvent, domain.Channel, error) {
	if len(ids) == 0 {
		return nil, domain.ChannelUpdateEvent{}, channel, nil
	}
	seen := make(map[int]struct{}, len(ids))
	deleted := make([]int, 0, len(ids))
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return nil, domain.ChannelUpdateEvent{}, channel, domain.ErrMessageIDInvalid
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		idx, ok := s.findMessageIndexLocked(channel.ID, id)
		if !ok || s.messages[channel.ID][idx].Deleted {
			continue
		}
		msg := s.messages[channel.ID][idx]
		if msg.SenderUserID != actorUserID && !canDeleteAnyChannelMessage(member) {
			return nil, domain.ChannelUpdateEvent{}, channel, domain.ErrChannelAdminRequired
		}
		msg.Deleted = true
		s.messages[channel.ID][idx] = msg
		deleted = append(deleted, id)
		s.appendChannelAdminLogLocked(domain.ChannelAdminLogEvent{
			ChannelID: channel.ID,
			UserID:    actorUserID,
			Date:      date,
			Type:      domain.ChannelAdminLogDeleteMessage,
			Message:   ptrChannelMessage(msg),
			Query:     msg.Body,
		})
	}
	if len(deleted) == 0 {
		return nil, domain.ChannelUpdateEvent{}, channel, nil
	}
	s.deleteChannelUnreadMentionsLocked(channel.ID, deleted)
	pts := s.nextChannelPtsNLocked(channel.ID, len(deleted))
	channel.Pts = pts
	channel.TopMessageID = s.topNonDeletedMessageIDLocked(channel.ID)
	s.channels[channel.ID] = channel
	for userID, member := range s.members[channel.ID] {
		if member.Status != domain.ChannelMemberActive {
			continue
		}
		if s.dialogs[userID] == nil {
			s.dialogs[userID] = make(map[int64]domain.ChannelDialog)
		}
		dialog := s.dialogForUserLocked(userID, channel)
		s.dialogs[userID][channel.ID] = dialog
	}
	event := domain.ChannelUpdateEvent{
		ChannelID:    channel.ID,
		Type:         domain.ChannelUpdateDeleteMessages,
		Pts:          pts,
		PtsCount:     len(deleted),
		Date:         date,
		MessageIDs:   append([]int(nil), deleted...),
		SenderUserID: actorUserID,
	}
	s.events[channel.ID] = append(s.events[channel.ID], event)
	return deleted, event, channel, nil
}

func (s *ChannelStore) topNonDeletedMessageIDLocked(channelID int64) int {
	for i := len(s.messages[channelID]) - 1; i >= 0; i-- {
		if !s.messages[channelID][i].Deleted {
			return s.messages[channelID][i].ID
		}
	}
	return 0
}

func (s *ChannelStore) eventForMessageLocked(channelID int64, id int) domain.ChannelUpdateEvent {
	for _, event := range s.events[channelID] {
		if event.Message.ID == id {
			return cloneChannelEvent(event)
		}
	}
	return domain.ChannelUpdateEvent{}
}

func (s *ChannelStore) appendChannelAdminLogLocked(event domain.ChannelAdminLogEvent) {
	if event.ChannelID == 0 || event.UserID == 0 || event.Type == "" {
		return
	}
	s.logSeq[event.ChannelID]++
	event.ID = s.logSeq[event.ChannelID]
	event.Query = adminLogSearchText(event)
	s.adminLogs[event.ChannelID] = append(s.adminLogs[event.ChannelID], cloneChannelAdminLogEvent(event))
}

func (s *ChannelStore) activeMemberIDsLocked(channelID, excludeUserID int64, limit int) []int64 {
	members := s.members[channelID]
	if limit <= 0 || limit > domain.MaxChannelRealtimeFanout {
		limit = domain.MaxChannelRealtimeFanout
	}
	capacity := limit
	if len(members) < capacity {
		capacity = len(members)
	}
	out := make([]int64, 0, capacity)
	for userID, member := range members {
		if userID == excludeUserID || member.Status != domain.ChannelMemberActive {
			continue
		}
		out = append(out, userID)
		if len(out) >= limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
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

func inactiveChannelDate(dialog domain.Dialog, channel domain.Channel, member domain.ChannelMember) int {
	if dialog.TopMessageDate > 0 {
		return dialog.TopMessageDate
	}
	date := channel.Date
	if member.JoinedAt > date {
		date = member.JoinedAt
	}
	return date
}

func recommendableChannel(channel domain.Channel) bool {
	return !channel.Deleted &&
		channel.Broadcast &&
		!channel.Megagroup &&
		strings.TrimSpace(channel.Username) != ""
}

func publicSearchableChannel(channel domain.Channel) bool {
	return !channel.Deleted &&
		(channel.Broadcast || channel.Megagroup) &&
		strings.TrimSpace(channel.Username) != ""
}

func publicPreviewableChannel(channel domain.Channel) bool {
	return publicSearchableChannel(channel)
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

func publicChannelSearchRank(channel domain.Channel, queryLower string) (int, bool) {
	if !publicSearchableChannel(channel) {
		return 0, false
	}
	username := strings.ToLower(strings.TrimSpace(channel.Username))
	title := strings.ToLower(strings.TrimSpace(channel.Title))
	switch {
	case username == queryLower:
		return 0, true
	case strings.HasPrefix(username, queryLower):
		return 1, true
	case strings.Contains(username, queryLower):
		return 2, true
	case strings.HasPrefix(title, queryLower):
		return 3, true
	case strings.Contains(title, queryLower):
		return 4, true
	default:
		return 0, false
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
	if peerInFolderList(dialog.Peer, folder.ExcludePeers) {
		return false
	}
	if folder.ExcludeRead && dialog.UnreadCount == 0 && !dialog.UnreadMark {
		return false
	}
	if folder.ExcludeArchived && dialog.FolderID == domain.DialogArchiveFolderID {
		return false
	}
	if peerInFolderList(dialog.Peer, folder.PinnedPeers) || peerInFolderList(dialog.Peer, folder.IncludePeers) {
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

func peerInFolderList(peer domain.Peer, items []domain.DialogFolderPeer) bool {
	for _, item := range items {
		if item.Peer == peer {
			return true
		}
	}
	return false
}

func canPostToBroadcast(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || (member.Role == domain.ChannelRoleAdmin && member.AdminRights.PostMessages)
}

func canSendChannelMessage(channel domain.Channel, member domain.ChannelMember) bool {
	if channel.Broadcast {
		return canPostToBroadcast(member)
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

func channelParticipantMatchesFilter(member domain.ChannelMember, kind domain.ChannelParticipantsFilterKind, query string) bool {
	if query != "" && !strings.Contains(strconv.FormatInt(member.UserID, 10), query) {
		return false
	}
	switch kind {
	case "", domain.ChannelParticipantsRecent, domain.ChannelParticipantsContacts, domain.ChannelParticipantsMentions, domain.ChannelParticipantsSearch:
		return member.Status == domain.ChannelMemberActive
	case domain.ChannelParticipantsAdmins:
		return member.Status == domain.ChannelMemberActive && isChannelAdmin(member)
	case domain.ChannelParticipantsKicked:
		return member.Status == domain.ChannelMemberKicked || member.BannedRights.ViewMessages
	case domain.ChannelParticipantsBanned:
		return member.Status != domain.ChannelMemberKicked && !zeroChannelBannedRights(member.BannedRights)
	case domain.ChannelParticipantsBots:
		return false
	default:
		return member.Status == domain.ChannelMemberActive
	}
}

func channelRoleOrder(role domain.ChannelMemberRole) int {
	switch role {
	case domain.ChannelRoleCreator:
		return 0
	case domain.ChannelRoleAdmin:
		return 1
	default:
		return 2
	}
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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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

func adminLogEventMatchesFilter(typ domain.ChannelAdminLogEventType, filter domain.ChannelAdminLogFilter) bool {
	if filter.Empty() {
		return true
	}
	switch typ {
	case domain.ChannelAdminLogParticipantJoin:
		return filter.Join
	case domain.ChannelAdminLogParticipantLeave:
		return filter.Leave
	case domain.ChannelAdminLogParticipantInvite:
		return filter.Invite || filter.Invites
	case domain.ChannelAdminLogParticipantBan:
		return filter.Ban
	case domain.ChannelAdminLogParticipantUnban:
		return filter.Unban
	case domain.ChannelAdminLogParticipantKick:
		return filter.Kick
	case domain.ChannelAdminLogParticipantUnkick:
		return filter.Unkick
	case domain.ChannelAdminLogParticipantPromote:
		return filter.Promote
	case domain.ChannelAdminLogParticipantDemote:
		return filter.Demote
	case domain.ChannelAdminLogChangeTitle, domain.ChannelAdminLogChangeUsername, domain.ChannelAdminLogChangeLinkedChat, domain.ChannelAdminLogToggleSlowMode:
		return filter.Info
	case domain.ChannelAdminLogToggleSignatures, domain.ChannelAdminLogTogglePreHistoryHidden, domain.ChannelAdminLogToggleAntiSpam, domain.ChannelAdminLogToggleAutotranslation:
		return filter.Settings
	case domain.ChannelAdminLogToggleForum:
		return filter.Settings || filter.Forums
	case domain.ChannelAdminLogUpdatePinned:
		return filter.Pinned
	case domain.ChannelAdminLogEditMessage:
		return filter.Edit
	case domain.ChannelAdminLogDeleteMessage:
		return filter.Delete
	case domain.ChannelAdminLogSendMessage:
		return filter.Send
	default:
		return false
	}
}

func adminLogEventMatchesQuery(event domain.ChannelAdminLogEvent, query string) bool {
	if strings.Contains(strings.ToLower(event.PrevString), query) ||
		strings.Contains(strings.ToLower(event.NewString), query) ||
		strings.Contains(event.Query, query) {
		return true
	}
	for _, msg := range []*domain.ChannelMessage{event.Message, event.PrevMessage, event.NewMessage} {
		if msg != nil && strings.Contains(strings.ToLower(msg.Body), query) {
			return true
		}
	}
	return false
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

func int64Set(items []int64) map[int64]struct{} {
	if len(items) == 0 {
		return nil
	}
	out := make(map[int64]struct{}, len(items))
	for _, item := range items {
		if item != 0 {
			out[item] = struct{}{}
		}
	}
	return out
}

func (s *ChannelStore) refreshChannelCountsLocked(channelID int64) {
	channel := s.channels[channelID]
	var participants, admins, kicked, banned int
	for _, member := range s.members[channelID] {
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
	channel.ParticipantsCount = participants
	channel.AdminsCount = admins
	channel.KickedCount = kicked
	channel.BannedCount = banned
	s.channels[channelID] = channel
}

func sameMessageEntities(a, b []domain.MessageEntity) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func diffFinal(returned, all []domain.ChannelUpdateEvent) bool {
	if len(returned) == 0 {
		return true
	}
	return returned[len(returned)-1].Pts >= all[len(all)-1].Pts
}

func uniqueNonZero(ids []int64, exclude int64) []int64 {
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

func randomMemoryPositiveInt64() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:]) & ((1 << 63) - 1)), nil
}

func randomMemoryInviteHash() (string, error) {
	var b [18]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
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

func cloneChannelMembers(in []domain.ChannelMember) []domain.ChannelMember {
	return append([]domain.ChannelMember(nil), in...)
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
		out.Channels = append(out.Channels, cloneChannel(changed[id]))
	}
	return out
}

func cloneChannel(in domain.Channel) domain.Channel {
	in.ReactionPolicy = copyChannelReactionPolicy(in.ReactionPolicy)
	return in
}

func copyChannelReactionPolicy(in domain.ChannelReactionPolicy) domain.ChannelReactionPolicy {
	in.Emoticons = append([]string(nil), in.Emoticons...)
	in.CustomEmojiIDs = append([]int64(nil), in.CustomEmojiIDs...)
	return in
}

func cloneChannelEvent(in domain.ChannelUpdateEvent) domain.ChannelUpdateEvent {
	in.Message = cloneChannelMessage(in.Message)
	in.MessageIDs = append([]int(nil), in.MessageIDs...)
	in.UserIDs = append([]int64(nil), in.UserIDs...)
	return in
}

func cloneChannelForumTopic(in domain.ChannelForumTopic) domain.ChannelForumTopic {
	return in
}

func (s *ChannelStore) topicWithViewerCountersLocked(viewerUserID, channelID int64, topic domain.ChannelForumTopic, member domain.ChannelMember) domain.ChannelForumTopic {
	out := cloneChannelForumTopic(topic)
	out.UnreadCount = s.channelThreadUnreadCountLocked(viewerUserID, channelID, topic.TopicID, member.ReadInboxMaxID)
	out.UnreadMentionsCount = s.countChannelUnreadMentionsLocked(viewerUserID, channelID, topic.TopicID)
	out.UnreadReactionsCount = s.countChannelUnreadReactionsLocked(viewerUserID, channelID, topic.TopicID)
	return out
}

func (s *ChannelStore) updateForumTopicTopMessageLocked(channelID int64, msg domain.ChannelMessage) {
	if msg.ReplyTo == nil || !msg.ReplyTo.ForumTopic || msg.ReplyTo.TopMessageID <= 0 {
		return
	}
	topic, ok := s.topics[channelID][msg.ReplyTo.TopMessageID]
	if !ok {
		return
	}
	topic.TopMessageID = msg.ID
	topic.Date = msg.Date
	s.topics[channelID][topic.TopicID] = topic
}

func sortForumTopics(topics []domain.ChannelForumTopic) {
	sort.Slice(topics, func(i, j int) bool {
		a, b := topics[i], topics[j]
		if a.Pinned != b.Pinned {
			return a.Pinned
		}
		if a.PinnedOrder != b.PinnedOrder {
			return a.PinnedOrder > b.PinnedOrder
		}
		if a.Date != b.Date {
			return a.Date > b.Date
		}
		return a.TopicID > b.TopicID
	})
}

func forumTopicBeforeOrAtOffset(topic domain.ChannelForumTopic, filter domain.ChannelForumTopicFilter) bool {
	if filter.OffsetDate == 0 && filter.OffsetID == 0 && filter.OffsetTopic == 0 {
		return false
	}
	offsetID := filter.OffsetTopic
	if offsetID == 0 {
		offsetID = filter.OffsetID
	}
	if filter.OffsetDate != 0 {
		if topic.Date < filter.OffsetDate {
			return false
		}
		if topic.Date > filter.OffsetDate {
			return true
		}
	}
	if offsetID == 0 {
		return false
	}
	return topic.TopicID >= offsetID
}

func (s *ChannelStore) forumTopicRootMessagesLocked(channelID int64, topics []domain.ChannelForumTopic, availableMinID int) []domain.ChannelMessage {
	if len(topics) == 0 {
		return nil
	}
	wanted := make(map[int]struct{}, len(topics))
	for _, topic := range topics {
		if topic.TopMessageID > 0 {
			wanted[topic.TopMessageID] = struct{}{}
		}
	}
	messages := make([]domain.ChannelMessage, 0, len(wanted))
	for _, msg := range s.messages[channelID] {
		if _, ok := wanted[msg.ID]; !ok {
			continue
		}
		if msg.Deleted || msg.ID <= availableMinID {
			continue
		}
		messages = append(messages, cloneChannelMessage(msg))
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID > messages[j].ID })
	return messages
}

func (s *ChannelStore) populateChannelMessageReactionsLocked(viewerUserID int64, channel domain.Channel, messages []domain.ChannelMessage) {
	if len(messages) == 0 || channel.ID == 0 {
		return
	}
	s.populateChannelMessageUnreadFlagsLocked(viewerUserID, messages)
	for i := range messages {
		if messages[i].ChannelID != channel.ID || messages[i].ID <= 0 {
			continue
		}
		reactions := s.channelMessageReactionsLocked(viewerUserID, channel, messages[i].ID)
		if len(reactions.Results) == 0 && len(reactions.Recent) == 0 {
			continue
		}
		messages[i].Reactions = cloneChannelMessageReactionsPtr(&reactions)
	}
}

func (s *ChannelStore) populateChannelMessagesReactionsLocked(viewerUserID int64, channels []domain.Channel, messages []domain.ChannelMessage) {
	if len(messages) == 0 {
		return
	}
	s.populateChannelMessageUnreadFlagsLocked(viewerUserID, messages)
	channelsByID := make(map[int64]domain.Channel, len(channels))
	for _, ch := range channels {
		if ch.ID != 0 {
			channelsByID[ch.ID] = ch
		}
	}
	for i := range messages {
		ch := channelsByID[messages[i].ChannelID]
		if ch.ID == 0 {
			ch = s.channels[messages[i].ChannelID]
		}
		if ch.ID == 0 {
			continue
		}
		reactions := s.channelMessageReactionsLocked(viewerUserID, ch, messages[i].ID)
		if len(reactions.Results) == 0 && len(reactions.Recent) == 0 {
			continue
		}
		messages[i].Reactions = cloneChannelMessageReactionsPtr(&reactions)
	}
}

func (s *ChannelStore) populateChannelMessageUnreadFlagsLocked(viewerUserID int64, messages []domain.ChannelMessage) {
	if viewerUserID == 0 || len(messages) == 0 {
		return
	}
	for i := range messages {
		if messages[i].ChannelID == 0 || messages[i].ID <= 0 {
			continue
		}
		if _, ok := s.mentions[viewerUserID][messages[i].ChannelID][messages[i].ID]; !ok {
			continue
		}
		messages[i].Mentioned = true
		messages[i].MediaUnread = !messages[i].Media.IsZero()
	}
}

type memoryReactionCursor struct {
	date     int
	userID   int64
	emoticon string
}

func (s *ChannelStore) channelMessageReactionsLocked(viewerUserID int64, channel domain.Channel, messageID int) domain.ChannelMessageReactions {
	rows := s.channelMessageReactionRowsLocked(channel.ID, messageID, viewerUserID, nil)
	out := domain.ChannelMessageReactions{
		CanSeeList: !channel.Broadcast || channel.Megagroup,
		Results:    []domain.ChannelMessageReactionCount{},
		Recent:     []domain.ChannelMessagePeerReaction{},
	}
	if len(rows) == 0 {
		return out
	}
	type aggregate struct {
		reaction    domain.MessageReaction
		count       int
		chosenOrder int
		latestDate  int
	}
	aggregates := make(map[string]*aggregate)
	for _, row := range rows {
		key := string(row.Reaction.Type) + "\x00" + row.Reaction.Emoticon
		item := aggregates[key]
		if item == nil {
			item = &aggregate{reaction: row.Reaction}
			aggregates[key] = item
		}
		item.count++
		if row.My && row.ChosenOrder > 0 {
			item.chosenOrder = row.ChosenOrder
		}
		if row.Date > item.latestDate {
			item.latestDate = row.Date
		}
	}
	items := make([]aggregate, 0, len(aggregates))
	for _, item := range aggregates {
		items = append(items, *item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count != items[j].count {
			return items[i].count > items[j].count
		}
		if items[i].latestDate != items[j].latestDate {
			return items[i].latestDate > items[j].latestDate
		}
		return items[i].reaction.Emoticon < items[j].reaction.Emoticon
	})
	for _, item := range items {
		out.Results = append(out.Results, domain.ChannelMessageReactionCount{
			Reaction:    item.reaction,
			Count:       item.count,
			ChosenOrder: item.chosenOrder,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Date != rows[j].Date {
			return rows[i].Date > rows[j].Date
		}
		if rows[i].UserID != rows[j].UserID {
			return rows[i].UserID > rows[j].UserID
		}
		return rows[i].Reaction.Emoticon < rows[j].Reaction.Emoticon
	})
	if len(rows) > domain.MaxChannelMessageReactionRecent {
		rows = rows[:domain.MaxChannelMessageReactionRecent]
	}
	out.Recent = cloneChannelPeerReactions(rows)
	return out
}

func (s *ChannelStore) channelMessageReactionRowsLocked(channelID int64, messageID int, viewerUserID int64, filter *domain.MessageReaction) []domain.ChannelMessagePeerReaction {
	byMessage := s.reactions[channelID]
	if byMessage == nil {
		return nil
	}
	byUser := byMessage[messageID]
	if byUser == nil {
		return nil
	}
	rows := make([]domain.ChannelMessagePeerReaction, 0, len(byUser))
	for _, userRows := range byUser {
		for _, row := range userRows {
			if filter != nil && (row.Reaction.Type != filter.Type || row.Reaction.Emoticon != filter.Emoticon) {
				continue
			}
			row.My = row.UserID == viewerUserID
			rows = append(rows, row)
		}
	}
	return rows
}

func memoryReactionOffset(row domain.ChannelMessagePeerReaction) string {
	return strconv.Itoa(row.Date) + ":" + strconv.FormatInt(row.UserID, 10) + ":" + row.Reaction.Emoticon
}

func messageReactionKey(reaction domain.MessageReaction) string {
	return string(reaction.Type) + "\x00" + reaction.Emoticon
}

func parseMemoryReactionOffset(offset string) (memoryReactionCursor, bool) {
	parts := strings.SplitN(offset, ":", 3)
	if len(parts) != 3 {
		return memoryReactionCursor{}, false
	}
	date, err := strconv.Atoi(parts[0])
	if err != nil || date < 0 {
		return memoryReactionCursor{}, false
	}
	userID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || userID < 0 {
		return memoryReactionCursor{}, false
	}
	return memoryReactionCursor{date: date, userID: userID, emoticon: parts[2]}, true
}

func memoryReactionAfterOffset(row domain.ChannelMessagePeerReaction, cursor memoryReactionCursor) bool {
	if row.Date != cursor.date {
		return row.Date < cursor.date
	}
	if row.UserID != cursor.userID {
		return row.UserID < cursor.userID
	}
	return row.Reaction.Emoticon > cursor.emoticon
}

func (s *ChannelStore) nextForumTopicPinnedOrderLocked(channelID int64) int {
	next := 1
	for _, topic := range s.topics[channelID] {
		if topic.PinnedOrder >= next {
			next = topic.PinnedOrder + 1
		}
	}
	return next
}

func (s *ChannelStore) topicHasVisibleMessagesLocked(channelID int64, topicID int) bool {
	for _, msg := range s.messages[channelID] {
		if msg.Deleted {
			continue
		}
		if msg.ID == topicID || (msg.ReplyTo != nil && msg.ReplyTo.TopMessageID == topicID) {
			return true
		}
	}
	return false
}

func cloneChannelMessage(in domain.ChannelMessage) domain.ChannelMessage {
	in.Entities = append([]domain.MessageEntity(nil), in.Entities...)
	in.ReplyTo = cloneMessageReply(in.ReplyTo)
	in.Forward = cloneMessageForward(in.Forward)
	in.Discussion = cloneChannelDiscussionRef(in.Discussion)
	in.Replies = cloneChannelMessageReplies(in.Replies)
	in.Reactions = cloneChannelMessageReactionsPtr(in.Reactions)
	if in.SendAs != nil {
		p := *in.SendAs
		in.SendAs = &p
	}
	if in.Action != nil {
		in.Action = cloneChannelMessageAction(in.Action)
	}
	return in
}

func cloneChannelMessageAction(in *domain.ChannelMessageAction) *domain.ChannelMessageAction {
	if in == nil {
		return nil
	}
	out := *in
	out.UserIDs = append([]int64(nil), in.UserIDs...)
	if in.Closed != nil {
		v := *in.Closed
		out.Closed = &v
	}
	if in.Hidden != nil {
		v := *in.Hidden
		out.Hidden = &v
	}
	return &out
}

func cloneChannelDiscussionRef(in *domain.ChannelDiscussionRef) *domain.ChannelDiscussionRef {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneChannelMessageReplies(in *domain.ChannelMessageReplies) *domain.ChannelMessageReplies {
	if in == nil {
		return nil
	}
	out := *in
	out.RecentRepliers = append([]domain.Peer(nil), in.RecentRepliers...)
	return &out
}

func cloneChannelMessageReactionsPtr(in *domain.ChannelMessageReactions) *domain.ChannelMessageReactions {
	if in == nil {
		return nil
	}
	out := cloneChannelMessageReactions(*in)
	return &out
}

func cloneChannelMessageReactions(in domain.ChannelMessageReactions) domain.ChannelMessageReactions {
	in.Results = append([]domain.ChannelMessageReactionCount(nil), in.Results...)
	in.Recent = cloneChannelPeerReactions(in.Recent)
	return in
}

func cloneChannelPeerReactions(in []domain.ChannelMessagePeerReaction) []domain.ChannelMessagePeerReaction {
	if len(in) == 0 {
		return nil
	}
	return append([]domain.ChannelMessagePeerReaction(nil), in...)
}

func ptrChannelMember(in domain.ChannelMember) *domain.ChannelMember {
	out := in
	return &out
}

func ptrChannelMessage(in domain.ChannelMessage) *domain.ChannelMessage {
	out := cloneChannelMessage(in)
	return &out
}

func cloneChannelAdminLogEvent(in domain.ChannelAdminLogEvent) domain.ChannelAdminLogEvent {
	if in.PrevParticipant != nil {
		in.PrevParticipant = ptrChannelMember(*in.PrevParticipant)
	}
	if in.NewParticipant != nil {
		in.NewParticipant = ptrChannelMember(*in.NewParticipant)
	}
	if in.Participant != nil {
		in.Participant = ptrChannelMember(*in.Participant)
	}
	if in.Message != nil {
		in.Message = ptrChannelMessage(*in.Message)
	}
	if in.PrevMessage != nil {
		in.PrevMessage = ptrChannelMessage(*in.PrevMessage)
	}
	if in.NewMessage != nil {
		in.NewMessage = ptrChannelMessage(*in.NewMessage)
	}
	return in
}
