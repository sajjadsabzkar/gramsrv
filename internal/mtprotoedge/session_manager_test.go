package mtprotoedge

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"
)

// TestSessionManagerRegistry 验证注册表的注册/注销/查找语义（不涉及网络发送）。
func TestSessionManagerRegistry(t *testing.T) {
	sm := NewSessionManager(zaptest.NewLogger(t))
	c := &Conn{sessionID: 42, authKeyID: [8]byte{1, 2, 3}}
	c.receivesUpdates.Store(true)

	sm.Register(c)
	if got := sm.Online(); got != 1 {
		t.Fatalf("online = %d, want 1", got)
	}
	sm.BindAuthKey(42, [8]byte{1, 2, 3})
	sm.BindUser(42, 100)
	if userID, ok := sm.UserID(42); !ok || userID != 100 {
		t.Fatalf("cached user = %d ok %v, want 100/true", userID, ok)
	}
	sm.BindAuthKey(42, [8]byte{9})
	if userID, ok := sm.UserID(42); ok || userID != 0 {
		t.Fatalf("cached user after auth key switch = %d ok %v, want 0/false", userID, ok)
	}
	if userID, resolved := sm.UserIDResolved(42); resolved || userID != 0 {
		t.Fatalf("resolved user after auth key switch = %d resolved %v, want unresolved", userID, resolved)
	}
	sm.BindUser(42, 0)
	if userID, resolved := sm.UserIDResolved(42); !resolved || userID != 0 {
		t.Fatalf("negative user cache = %d resolved %v, want 0/true", userID, resolved)
	}

	sm.Unregister(c)
	if got := sm.Online(); got != 0 {
		t.Fatalf("online after unregister = %d, want 0", got)
	}

	err := sm.PushToSession(context.Background(), 42, proto.MessageFromServer, &tg.UpdatesTooLong{})
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("push to missing session err = %v, want ErrSessionNotFound", err)
	}
}

func TestSessionManagerScopesSameSessionIDByAuthKey(t *testing.T) {
	sm := NewSessionManager(zaptest.NewLogger(t))
	raw1 := [8]byte{1}
	raw2 := [8]byte{2}
	perm1 := [8]byte{9}
	c1 := &Conn{sessionID: 42, authKeyID: raw1}
	c2 := &Conn{sessionID: 42, authKeyID: raw2}

	sm.Register(c1)
	sm.Register(c2)
	if got := sm.Online(); got != 2 {
		t.Fatalf("online = %d, want 2", got)
	}

	sm.BindAuthKeyForSession(raw1, 42, perm1)
	sm.BindUserForAuthKey(raw1, 42, 100)
	sm.BindUserForAuthKey(raw2, 42, 200)

	if userID, ok := sm.UserIDForAuthKey(raw1, 42); !ok || userID != 100 {
		t.Fatalf("scoped user raw1 = %d ok %v, want 100/true", userID, ok)
	}
	if userID, ok := sm.UserIDForAuthKey(raw2, 42); !ok || userID != 200 {
		t.Fatalf("scoped user raw2 = %d ok %v, want 200/true", userID, ok)
	}
	if _, ok := sm.UserID(42); ok {
		t.Fatal("legacy UserID unexpectedly resolved ambiguous session_id")
	}
	if err := sm.PushToSession(context.Background(), 42, proto.MessageFromServer, &tg.UpdatesTooLong{}); !errors.Is(err, ErrSessionAmbiguous) {
		t.Fatalf("ambiguous push err = %v, want ErrSessionAmbiguous", err)
	}

	sm.BindUserForAuthKey(raw1, 42, 300)
	sm.BindUserForAuthKey(raw2, 42, 300)
	sent, err := sm.PushToUserExceptAuthKeySession(context.Background(), 300, perm1, 42, proto.MessageFromServer, &tg.UpdatesTooLong{})
	if err != nil {
		t.Fatalf("push except scoped session: %v", err)
	}
	if sent != 1 {
		t.Fatalf("pushed to %d sessions, want 1", sent)
	}
	if _, ok := sm.pending[sessionKey{authKeyID: raw1, sessionID: 42}]; ok {
		t.Fatal("excluded session received pending push")
	}
	if got := len(sm.pending[sessionKey{authKeyID: raw2, sessionID: 42}]); got != 1 {
		t.Fatalf("raw2 pending pushes = %d, want 1", got)
	}
}

func TestSessionManagerChannelInterestIndex(t *testing.T) {
	sm := NewSessionManager(zaptest.NewLogger(t))
	raw := [8]byte{1, 2, 3}
	c := &Conn{sessionID: 42, authKeyID: raw}
	sm.Register(c)
	sm.BindUserForAuthKey(raw, 42, 100)

	sm.TrackChannelInterest(raw, 42, 100, []int64{10, 10, 20})
	if got := sm.OnlineChannelUserIDs(10, 10); len(got) != 1 || got[0] != 100 {
		t.Fatalf("channel 10 online users = %v, want [100]", got)
	}
	sm.TrackChannelInterest(raw, 42, 100, []int64{20})
	if got := sm.OnlineChannelUserIDs(10, 10); len(got) != 0 {
		t.Fatalf("channel 10 after viewer switch = %v, want empty", got)
	}
	if got := sm.OnlineChannelUserIDs(20, 10); len(got) != 1 || got[0] != 100 {
		t.Fatalf("channel 20 after viewer switch = %v, want [100]", got)
	}
	sm.TrackChannelInterest(raw, 42, 100, []int64{10})
	if got := sm.OnlineChannelMemberUserIDs(10, 10); len(got) != 0 {
		t.Fatalf("channel 10 online members before membership sync = %v, want empty", got)
	}
	sm.SetSessionChannelMemberships(raw, 42, 100, []int64{10, 30})
	if got := sm.OnlineChannelMemberUserIDs(10, 10); len(got) != 1 || got[0] != 100 {
		t.Fatalf("channel 10 online members = %v, want [100]", got)
	}
	if got := sm.OnlineChannelUserIDs(30, 10); len(got) != 0 {
		t.Fatalf("channel 30 viewers = %v, want empty", got)
	}
	if got := sm.OnlineUserIDsForCandidates([]int64{0, 200, 100, 100}, 10); len(got) != 1 || got[0] != 100 {
		t.Fatalf("candidate online users = %v, want [100]", got)
	}

	sm.BindUserForAuthKey(raw, 42, 200)
	if got := sm.OnlineChannelUserIDs(10, 10); len(got) != 0 {
		t.Fatalf("channel interest after user switch = %v, want empty", got)
	}
	if got := sm.OnlineChannelMemberUserIDs(10, 10); len(got) != 0 {
		t.Fatalf("channel membership after user switch = %v, want empty", got)
	}
	sm.TrackChannelInterest(raw, 42, 200, []int64{10})
	if got := sm.OnlineChannelUserIDs(10, 10); len(got) != 1 || got[0] != 200 {
		t.Fatalf("channel 10 after re-track = %v, want [200]", got)
	}
	sm.AddUserChannelMembership(200, 10)
	if got := sm.OnlineChannelMemberUserIDs(10, 10); len(got) != 1 || got[0] != 200 {
		t.Fatalf("channel 10 membership after add = %v, want [200]", got)
	}
	sm.RemoveUserChannelMembership(200, 10)
	if got := sm.OnlineChannelMemberUserIDs(10, 10); len(got) != 0 {
		t.Fatalf("channel membership after remove = %v, want empty", got)
	}
	sm.ClearChannelInterest(raw, 42, 200)
	if got := sm.OnlineChannelUserIDs(10, 10); len(got) != 0 {
		t.Fatalf("channel interest after explicit clear = %v, want empty", got)
	}

	sm.Unregister(c)
	if got := sm.OnlineChannelUserIDs(10, 10); len(got) != 0 {
		t.Fatalf("channel interest after unregister = %v, want empty", got)
	}
	if got := sm.OnlineChannelMemberUserIDs(10, 10); len(got) != 0 {
		t.Fatalf("channel membership after unregister = %v, want empty", got)
	}
}

func TestSessionManagerClearsChannelIndexesOnAuthAndReadinessChanges(t *testing.T) {
	sm := NewSessionManager(zaptest.NewLogger(t))
	raw := [8]byte{1, 2, 3}
	business := [8]byte{8}
	c := &Conn{sessionID: 42, authKeyID: raw}
	sm.Register(c)
	sm.BindAuthKeyForSession(raw, 42, business)
	sm.BindUserForAuthKey(raw, 42, 100)

	track := func() {
		sm.TrackChannelInterest(raw, 42, 100, []int64{10})
		sm.SetSessionChannelMemberships(raw, 42, 100, []int64{10})
		if got := sm.OnlineChannelUserIDs(10, 10); len(got) != 1 || got[0] != 100 {
			t.Fatalf("channel viewers before cleanup = %v, want [100]", got)
		}
		if got := sm.OnlineChannelMemberUserIDs(10, 10); len(got) != 1 || got[0] != 100 {
			t.Fatalf("channel members before cleanup = %v, want [100]", got)
		}
	}
	assertCleared := func(label string) {
		if got := sm.OnlineChannelUserIDs(10, 10); len(got) != 0 {
			t.Fatalf("%s viewers = %v, want empty", label, got)
		}
		if got := sm.OnlineChannelMemberUserIDs(10, 10); len(got) != 0 {
			t.Fatalf("%s members = %v, want empty", label, got)
		}
	}

	track()
	sm.SetReceivesUpdatesForAuthKey(raw, 42, false)
	assertCleared("after receivesUpdates=false")

	track()
	sm.BindAuthKeyForSession(raw, 42, [8]byte{9})
	assertCleared("after business auth key change")

	sm.BindAuthKeyForSession(raw, 42, business)
	sm.BindUserForAuthKey(raw, 42, 100)
	track()
	if n := sm.UnbindAuthKey(business); n != 1 {
		t.Fatalf("UnbindAuthKey count = %d, want 1", n)
	}
	assertCleared("after unbind auth key")
}

// TestSessionManagerPush 验证主动推送端到端：两个 client 连接握手并建立 session 后，
// server 经 PushToSession / PushToUser 主动向其推送，client 收到。
func TestSessionManagerPush(t *testing.T) {
	const dc = 2
	addr, pub, srv := startTestServer(t, Options{DC: dc})

	conn1, auth1, cipher1 := dialHandshake(t, addr, dc, pub)
	conn2, auth2, cipher2 := dialHandshake(t, addr, dc, pub)

	// 各发一个 ping 建立 session，触发注册（并清掉 new_session_created/pong/ack）。
	msgGen := proto.NewMessageIDGen(time.Now)
	sendEncrypted(t, conn1, cipher1, auth1, msgGen.New(proto.MessageFromClient), &mt.PingRequest{PingID: 1})
	collectReplies(t, conn1, cipher1, auth1.AuthKey, mt.PongTypeID)
	sendEncrypted(t, conn2, cipher2, auth2, msgGen.New(proto.MessageFromClient), &mt.PingRequest{PingID: 2})
	collectReplies(t, conn2, cipher2, auth2.AuthKey, mt.PongTypeID)

	if got := srv.Conns().Online(); got != 2 {
		t.Fatalf("online = %d, want 2", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1) PushToSession：session2 尚未进入 updates 同步入口时先暂存，ready 后下发。
	if err := srv.Conns().PushToSession(ctx, auth2.SessionID, proto.MessageFromServer, &tg.UpdatesTooLong{}); err != nil {
		t.Fatalf("push to session: %v", err)
	}
	srv.Conns().SetReceivesUpdates(auth2.SessionID, true)
	r2 := collectReplies(t, conn2, cipher2, auth2.AuthKey, tg.UpdatesTooLongTypeID)
	mustHave(t, r2, tg.UpdatesTooLongTypeID, "pushed updates on conn2")

	// 2) BindUser + PushToUser：按 user 维度推送给 conn1。
	srv.Conns().BindUser(auth1.SessionID, 100)
	srv.Conns().SetReceivesUpdates(auth1.SessionID, true)
	sent, err := srv.Conns().PushToUser(ctx, 100, proto.MessageFromServer, &tg.UpdatesTooLong{})
	if err != nil {
		t.Fatalf("push to user: %v", err)
	}
	if sent != 1 {
		t.Fatalf("pushed to %d conns, want 1", sent)
	}
	r1 := collectReplies(t, conn1, cipher1, auth1.AuthKey, tg.UpdatesTooLongTypeID)
	mustHave(t, r1, tg.UpdatesTooLongTypeID, "pushed updates on conn1")

	// 3) PushToUserExceptSession：模拟 SyncUpdatesNotMe，跳过当前 session。
	srv.Conns().BindUser(auth1.SessionID, 200)
	srv.Conns().BindUser(auth2.SessionID, 200)
	sent, err = srv.Conns().PushToUserExceptSession(ctx, 200, auth2.SessionID, proto.MessageFromServer, &tg.UpdatesTooLong{})
	if err != nil {
		t.Fatalf("push to user except session: %v", err)
	}
	if sent != 1 {
		t.Fatalf("pushed to %d conns, want 1 after excluding current session", sent)
	}
	r1 = collectReplies(t, conn1, cipher1, auth1.AuthKey, tg.UpdatesTooLongTypeID)
	mustHave(t, r1, tg.UpdatesTooLongTypeID, "pushed not-me updates on conn1")
}

func BenchmarkSessionManagerOnlineCandidateFilter(b *testing.B) {
	sm := NewSessionManager(zaptest.NewLogger(b))
	const online = 200_000
	rawPrefix := [8]byte{9}
	for i := 1; i <= online; i++ {
		raw := rawPrefix
		raw[1] = byte(i)
		raw[2] = byte(i >> 8)
		raw[3] = byte(i >> 16)
		raw[4] = byte(i >> 24)
		c := &Conn{sessionID: int64(i), authKeyID: raw}
		sm.Register(c)
		sm.BindUserForAuthKey(raw, int64(i), int64(i))
	}
	candidates := make([]int64, 0, 500)
	for i := 0; i < 500; i++ {
		candidates = append(candidates, int64(i*97+1))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := sm.OnlineUserIDsForCandidates(candidates, 500)
		if len(got) == 0 {
			b.Fatal("no candidates matched")
		}
	}
}
