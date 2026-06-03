package postgres

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// TestSendPrivateMediaSurvivesUpdateEvent 验证带 media 的私聊消息：
//   - 发送后 message_boxes 持久化 media 快照；
//   - 接收方经 UpdateEventStore.ListAfter（在线 outbox / 离线 getDifference 共用的重建路径）
//     能拿回 media（曾因 update event 查询漏选 m.media 导致收件人/离线丢媒体，本测试守护该修复）；
//   - history（ListByUser）读取也带 media。
func TestSendPrivateMediaSurvivesUpdateEvent(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender, err := users.Create(ctx, domain.User{AccessHash: 61, Phone: "+1998" + suffix + "01", FirstName: "MediaSender"})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(ctx, domain.User{AccessHash: 62, Phone: "+1998" + suffix + "02", FirstName: "MediaRecipient"})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	ids := []int64{sender.ID, recipient.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM user_update_events WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM message_boxes WHERE owner_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM private_messages WHERE sender_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM dialogs WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", ids)
	})

	messages := NewMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}, &perUserCounterAllocator{}))

	media := &domain.MessageMedia{
		Kind: domain.MessageMediaKindDocument,
		Document: &domain.Document{
			ID:         9200000000000000001,
			AccessHash: 9,
			DCID:       2,
			MimeType:   "application/x-tgsticker",
			Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrSticker, Alt: "\U0001f600", StickerSetID: 5, StickerSetAccessHash: 7}},
		},
	}
	res, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        time.Now().UnixNano(),
		Message:         "", // 仅媒体（无 caption）
		Media:           media,
		Date:            int(time.Now().Unix()),
	})
	if err != nil {
		t.Fatalf("send private media: %v", err)
	}

	// 发送结果双端均带 media。
	for name, msg := range map[string]domain.Message{"sender": res.SenderMessage, "recipient": res.RecipientMessage} {
		if msg.Media == nil || msg.Media.Kind != domain.MessageMediaKindDocument || msg.Media.Document == nil || msg.Media.Document.ID != media.Document.ID {
			t.Fatalf("%s message media lost: %+v", name, msg.Media)
		}
	}

	// 关键：接收方经更新事件重建（在线推送 / 离线 difference 共用路径）仍带 media。
	events := NewUpdateEventStore(pool)
	got, err := events.ListAfter(ctx, recipient.ID, 0, 10)
	if err != nil {
		t.Fatalf("list recipient events: %v", err)
	}
	var found bool
	for _, ev := range got {
		if ev.Type == domain.UpdateEventNewMessage && ev.Message.ID != 0 {
			found = true
			if ev.Message.Media == nil || ev.Message.Media.Document == nil || ev.Message.Media.Document.ID != media.Document.ID {
				t.Fatalf("recipient update-event message lost media: %+v", ev.Message.Media)
			}
		}
	}
	if !found {
		t.Fatal("no new_message event for recipient")
	}

	// history 读取也带 media。
	list, err := messages.ListByUser(ctx, recipient.ID, domain.MessageFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list by user: %v", err)
	}
	if len(list.Messages) == 0 || list.Messages[0].Media == nil || list.Messages[0].Media.Document == nil {
		t.Fatalf("history message lost media: %+v", list.Messages)
	}
}

func TestSendChannelMediaSurvivesDifference(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 63, Phone: "+1998" + suffix + "03", FirstName: "MediaChannelOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{AccessHash: 64, Phone: "+1998" + suffix + "04", FirstName: "MediaChannelMember"})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Media Difference " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          int(time.Now().Unix()),
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	media := &domain.MessageMedia{
		Kind: domain.MessageMediaKindDocument,
		Document: &domain.Document{
			ID:         9200000000000000002,
			AccessHash: 10,
			DCID:       2,
			MimeType:   "application/octet-stream",
			Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrFilename, FileName: "telesrv-media.bin"}},
		},
	}
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  time.Now().UnixNano(),
		Message:   "",
		Media:     media,
		Date:      int(time.Now().Unix()),
	})
	if err != nil {
		t.Fatalf("send channel media: %v", err)
	}
	if sent.Message.Media == nil || sent.Message.Media.Document == nil || sent.Message.Media.Document.ID != media.Document.ID {
		t.Fatalf("send result lost media: %+v", sent.Message.Media)
	}

	diff, err := channels.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		Pts:       created.Event.Pts,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list channel difference: %v", err)
	}
	var found bool
	for _, msg := range diff.NewMessages {
		if msg.ID == sent.Message.ID {
			found = true
			if msg.Media == nil || msg.Media.Document == nil || msg.Media.Document.ID != media.Document.ID {
				t.Fatalf("channel difference message lost media: %+v", msg.Media)
			}
		}
	}
	if !found {
		t.Fatalf("sent media message %d not found in channel difference: %+v", sent.Message.ID, diff.NewMessages)
	}
}
