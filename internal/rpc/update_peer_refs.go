package rpc

import (
	"context"

	"telesrv/internal/domain"
)

func (r *Router) enrichUpdateEvents(ctx context.Context, viewerUserID int64, events []domain.UpdateEvent) []domain.UpdateEvent {
	if len(events) == 0 {
		return events
	}
	out := append([]domain.UpdateEvent(nil), events...)
	for i := range out {
		if out[i].Type == domain.UpdateEventMessageReactions {
			out[i] = r.enrichMessageReactionEvent(ctx, viewerUserID, out[i])
		}
		userIDs := make(map[int64]struct{})
		channelIDs := make(map[int64]struct{})
		addDomainPeerRef(out[i].Peer, 0, userIDs, channelIDs)
		for _, peer := range out[i].Peers {
			addDomainPeerRef(peer, 0, userIDs, channelIDs)
		}
		collectMessagePeerRefs(out[i].Message, 0, userIDs, channelIDs)
		out[i].Users = r.withUsersPresence(mergeDomainUsers(out[i].Users, r.domainUsersForIDs(ctx, viewerUserID, mapKeys(userIDs))...))
		out[i].Channels = mergeDomainChannels(out[i].Channels, r.domainChannelsForIDs(ctx, viewerUserID, mapKeys(channelIDs))...)
	}
	return out
}

func (r *Router) enrichMessageReactionEvent(ctx context.Context, viewerUserID int64, event domain.UpdateEvent) domain.UpdateEvent {
	if r.deps.Messages == nil || event.Message.ID <= 0 {
		return event
	}
	peer := event.Message.Peer
	if peer.Type == "" || peer.ID == 0 {
		peer = event.Peer
	}
	if peer.Type != domain.PeerTypeUser || peer.ID == 0 {
		return event
	}
	res, err := r.deps.Messages.GetMessageReactions(ctx, viewerUserID, domain.PrivateMessageReactionsRequest{
		OwnerUserID: viewerUserID,
		Peer:        peer,
		IDs:         []int{event.Message.ID},
	})
	if err != nil {
		return event
	}
	for _, msg := range res.Messages {
		if msg.OwnerUserID == viewerUserID && msg.ID == event.Message.ID {
			msg.Pts = event.Pts
			event.Message = msg
			event.Peer = msg.Peer
			return event
		}
	}
	return event
}

func (r *Router) enrichChannelDifference(ctx context.Context, viewerUserID int64, diff domain.ChannelDifference) domain.ChannelDifference {
	userIDs := make(map[int64]struct{})
	channelIDs := make(map[int64]struct{})
	for _, event := range diff.Events {
		collectChannelUpdatePeerRefs(event, diff.Channel.ID, userIDs, channelIDs)
	}
	for _, msg := range diff.NewMessages {
		collectChannelMessagePeerRefs(msg, diff.Channel.ID, userIDs, channelIDs)
	}
	for _, event := range diff.OtherUpdates {
		collectChannelUpdatePeerRefs(event, diff.Channel.ID, userIDs, channelIDs)
	}
	diff.Users = r.withUsersPresence(mergeDomainUsers(diff.Users, r.domainUsersForIDs(ctx, viewerUserID, mapKeys(userIDs))...))
	diff.Channels = mergeDomainChannels(diff.Channels, r.domainChannelsForIDs(ctx, viewerUserID, mapKeys(channelIDs))...)
	return diff
}

func (r *Router) enrichChannelHistory(ctx context.Context, viewerUserID int64, history domain.ChannelHistory) domain.ChannelHistory {
	userIDs := make(map[int64]struct{})
	channelIDs := make(map[int64]struct{})
	for _, msg := range history.Messages {
		collectChannelMessagePeerRefs(msg, history.Channel.ID, userIDs, channelIDs)
	}
	for _, topic := range history.Topics {
		if topic.CreatorUserID != 0 {
			userIDs[topic.CreatorUserID] = struct{}{}
		}
	}
	history.Users = r.withUsersPresence(mergeDomainUsers(history.Users, r.domainUsersForIDs(ctx, viewerUserID, mapKeys(userIDs))...))
	history.Channels = mergeDomainChannels(history.Channels, r.domainChannelsForIDs(ctx, viewerUserID, mapKeys(channelIDs))...)
	return history
}

func collectMessagePeerRefs(msg domain.Message, currentChannelID int64, userIDs, channelIDs map[int64]struct{}) {
	addDomainPeerRef(msg.From, currentChannelID, userIDs, channelIDs)
	addDomainPeerRef(msg.Peer, currentChannelID, userIDs, channelIDs)
	if msg.Forward != nil {
		addDomainPeerRef(msg.Forward.From, currentChannelID, userIDs, channelIDs)
	}
	if msg.ReplyTo != nil {
		addDomainPeerRef(msg.ReplyTo.Peer, currentChannelID, userIDs, channelIDs)
	}
	if msg.Reactions != nil {
		for _, reaction := range msg.Reactions.Recent {
			if reaction.UserID != 0 {
				userIDs[reaction.UserID] = struct{}{}
			}
		}
	}
}

func collectChannelUpdatePeerRefs(event domain.ChannelUpdateEvent, currentChannelID int64, userIDs, channelIDs map[int64]struct{}) {
	if event.SenderUserID != 0 {
		userIDs[event.SenderUserID] = struct{}{}
	}
	for _, id := range event.UserIDs {
		if id != 0 {
			userIDs[id] = struct{}{}
		}
	}
	for _, member := range []domain.ChannelMember{event.Previous, event.Participant} {
		if member.UserID != 0 {
			userIDs[member.UserID] = struct{}{}
		}
		if member.InviterUserID != 0 {
			userIDs[member.InviterUserID] = struct{}{}
		}
	}
	collectChannelMessagePeerRefs(event.Message, currentChannelID, userIDs, channelIDs)
}

func collectChannelMessagePeerRefs(msg domain.ChannelMessage, currentChannelID int64, userIDs, channelIDs map[int64]struct{}) {
	if msg.SenderUserID != 0 {
		userIDs[msg.SenderUserID] = struct{}{}
	}
	addDomainPeerRef(msg.From, currentChannelID, userIDs, channelIDs)
	if msg.SendAs != nil {
		addDomainPeerRef(*msg.SendAs, currentChannelID, userIDs, channelIDs)
	}
	if msg.Forward != nil {
		addDomainPeerRef(msg.Forward.From, currentChannelID, userIDs, channelIDs)
	}
	if msg.ReplyTo != nil {
		addDomainPeerRef(msg.ReplyTo.Peer, currentChannelID, userIDs, channelIDs)
	}
	if msg.Action != nil {
		for _, id := range msg.Action.UserIDs {
			if id != 0 {
				userIDs[id] = struct{}{}
			}
		}
	}
	if msg.Reactions != nil {
		for _, reaction := range msg.Reactions.Recent {
			if reaction.UserID != 0 {
				userIDs[reaction.UserID] = struct{}{}
			}
		}
	}
}

func addDomainPeerRef(peer domain.Peer, currentChannelID int64, userIDs, channelIDs map[int64]struct{}) {
	switch peer.Type {
	case domain.PeerTypeUser:
		if peer.ID != 0 {
			userIDs[peer.ID] = struct{}{}
		}
	case domain.PeerTypeChannel:
		if peer.ID != 0 && peer.ID != currentChannelID {
			channelIDs[peer.ID] = struct{}{}
		}
	}
}

func (r *Router) domainUsersForIDs(ctx context.Context, currentUserID int64, ids []int64) []domain.User {
	if len(ids) == 0 {
		return nil
	}
	out := make([]domain.User, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		switch {
		case id == domain.OfficialSystemUserID:
			out = append(out, r.withUserPresence(domain.OfficialSystemUser()))
		case r.deps.Users == nil:
			continue
		case id == currentUserID:
			if u, err := r.deps.Users.Self(ctx, currentUserID); err == nil && u.ID != 0 {
				out = append(out, r.withUserPresence(u))
			}
		default:
			if u, found, err := r.deps.Users.ByID(ctx, currentUserID, id); err == nil && found {
				out = append(out, r.withUserPresence(u))
			}
		}
	}
	return out
}

func (r *Router) domainChannelsForIDs(ctx context.Context, currentUserID int64, ids []int64) []domain.Channel {
	if r.deps.Channels == nil || len(ids) == 0 {
		return nil
	}
	out := make([]domain.Channel, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		view, err := r.deps.Channels.GetChannel(ctx, currentUserID, id)
		if err != nil || view.Channel.ID == 0 {
			continue
		}
		out = append(out, view.Channel)
	}
	return out
}

func mergeDomainUsers(base []domain.User, extra ...domain.User) []domain.User {
	out := append([]domain.User(nil), base...)
	seen := make(map[int64]struct{}, len(out)+len(extra))
	for _, u := range out {
		if u.ID != 0 {
			seen[u.ID] = struct{}{}
		}
	}
	for _, u := range extra {
		if u.ID == 0 {
			continue
		}
		if _, ok := seen[u.ID]; ok {
			continue
		}
		seen[u.ID] = struct{}{}
		out = append(out, u)
	}
	return out
}

func mergeDomainChannels(base []domain.Channel, extra ...domain.Channel) []domain.Channel {
	out := append([]domain.Channel(nil), base...)
	seen := make(map[int64]struct{}, len(out)+len(extra))
	for _, ch := range out {
		if ch.ID != 0 {
			seen[ch.ID] = struct{}{}
		}
	}
	for _, ch := range extra {
		if ch.ID == 0 {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		out = append(out, ch)
	}
	return out
}

func mapKeys(items map[int64]struct{}) []int64 {
	if len(items) == 0 {
		return nil
	}
	out := make([]int64, 0, len(items))
	for id := range items {
		if id != 0 {
			out = append(out, id)
		}
	}
	return out
}
