package postgres

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	appauth "telesrv/internal/app/auth"
	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

func TestAuthSignUpWritesOfficialLoginMessagePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	phone := fmt.Sprintf("1555%d31", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE phone = $1", phone)
	})

	users := NewUserStore(pool)
	dialogs := NewDialogStore(pool)
	messages := NewMessageStore(pool)
	svc := appauth.NewService(
		users,
		NewAuthorizationStore(pool),
		memory.NewCodeStore(),
		nil,
		nil,
		"12345",
		appauth.WithLoginMessages(messages, dialogs),
	)

	var authKeyID [8]byte
	var authKeyBody [256]byte
	if _, err := rand.Read(authKeyID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(authKeyBody[:]); err != nil {
		t.Fatal(err)
	}
	if err := NewAuthKeyStore(pool).Save(ctx, store.AuthKeyData{ID: authKeyID, Value: authKeyBody}); err != nil {
		t.Fatalf("save auth key: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM auth_keys WHERE auth_key_id = $1", authKeyIDToInt64(authKeyID))
	})
	hash, err := svc.SendCode(ctx, phone)
	if err != nil {
		t.Fatalf("SendCode: %v", err)
	}
	if _, _, needSignUp, err := svc.SignIn(ctx, domain.Authorization{AuthKeyID: authKeyID}, phone, hash, "12345"); err != nil || !needSignUp {
		t.Fatalf("SignIn needSignUp = %v err = %v, want need sign-up", needSignUp, err)
	}

	u, msg, err := svc.SignUp(ctx, domain.Authorization{AuthKeyID: authKeyID}, phone, hash, "PgLogin", "Test")
	if err != nil {
		t.Fatalf("SignUp: %v", err)
	}
	if u.Phone != phone || msg.ID == 0 || !strings.Contains(msg.Body, "Login code: 12345") {
		t.Fatalf("sign-up user/message = user %+v message %+v, want login message", u, msg)
	}

	systemUser, found, err := users.ByID(ctx, domain.OfficialSystemUserID)
	if err != nil || !found || !systemUser.Verified || !systemUser.Support {
		t.Fatalf("official system user = %+v found=%v err=%v, want seeded verified support user", systemUser, found, err)
	}
	list, err := dialogs.ListByUser(ctx, u.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("ListByUser: %v", err)
	}
	if len(list.Dialogs) != 1 || list.Dialogs[0].Peer.ID != domain.OfficialSystemUserID {
		t.Fatalf("dialogs = %+v, want official login dialog", list.Dialogs)
	}
	if len(list.Messages) != 1 || list.Messages[0].ID != msg.ID || !strings.Contains(list.Messages[0].Body, "Login code: 12345") {
		t.Fatalf("messages = %+v, want returned login message", list.Messages)
	}
	if len(list.Users) != 1 || list.Users[0].ID != domain.OfficialSystemUserID || !list.Users[0].Verified || !list.Users[0].Support {
		t.Fatalf("users = %+v, want official support user", list.Users)
	}
}
