package auth

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mtcrypto "github.com/gotd/td/crypto"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

func TestBindTempAuthKeyValidatesEncryptedMessage(t *testing.T) {
	ctx := context.Background()
	keys := memory.NewAuthKeyStore()
	tempBindings := memory.NewTempAuthKeyBindingStore()
	permKey := testAuthKey(0x11)
	tempKey := testAuthKey(0x55)
	saveAuthKey(t, keys, permKey)
	saveAuthKey(t, keys, tempKey)

	svc := NewService(memory.NewUserStore(), memory.NewAuthorizationStore(), memory.NewCodeStore(), keys, tempBindings, "12345")

	const (
		nonce     = int64(0x12345678)
		sessionID = int64(0x1020304050)
		msgID     = int64(0x0102030405060708)
	)
	expiresAt := int(time.Now().Add(time.Hour).Unix())
	encrypted, err := mtcrypto.EncryptBindMessage(
		bytes.NewReader(bytes.Repeat([]byte{0xCD}, 128)),
		permKey,
		msgID,
		&mtcrypto.BindAuthKeyInner{
			Nonce:         nonce,
			TempAuthKeyID: tempKey.IntID(),
			PermAuthKeyID: permKey.IntID(),
			TempSessionID: sessionID,
			ExpiresAt:     expiresAt,
		},
	)
	if err != nil {
		t.Fatalf("encrypt bind message: %v", err)
	}

	err = svc.BindTempAuthKey(ctx, sessionID, domain.TempAuthKeyBinding{
		TempAuthKeyID:    tempKey.ID,
		PermAuthKeyID:    permKey.IntID(),
		Nonce:            nonce,
		ExpiresAt:        expiresAt,
		EncryptedMessage: encrypted,
	})
	if err != nil {
		t.Fatalf("BindTempAuthKey valid message: %v", err)
	}

	err = svc.BindTempAuthKey(ctx, sessionID+1, domain.TempAuthKeyBinding{
		TempAuthKeyID:    tempKey.ID,
		PermAuthKeyID:    permKey.IntID(),
		Nonce:            nonce,
		ExpiresAt:        expiresAt,
		EncryptedMessage: encrypted,
	})
	if !errors.Is(err, ErrEncryptedMessageInvalid) {
		t.Fatalf("BindTempAuthKey wrong session err = %v, want ErrEncryptedMessageInvalid", err)
	}
}

func TestResolveAuthKeyUsesValidTempBinding(t *testing.T) {
	ctx := context.Background()
	tempBindings := memory.NewTempAuthKeyBindingStore()
	permKey := testAuthKey(0x11)
	tempKey := testAuthKey(0x55)
	svc := NewService(memory.NewUserStore(), memory.NewAuthorizationStore(), memory.NewCodeStore(), nil, tempBindings, "12345")

	if err := tempBindings.Save(ctx, domain.TempAuthKeyBinding{
		TempAuthKeyID: tempKey.ID,
		PermAuthKeyID: permKey.IntID(),
		ExpiresAt:     int(time.Now().Add(time.Hour).Unix()),
	}); err != nil {
		t.Fatalf("save temp binding: %v", err)
	}

	got, ok, err := svc.ResolveAuthKey(ctx, tempKey.ID)
	if err != nil {
		t.Fatalf("ResolveAuthKey: %v", err)
	}
	if !ok || got != permKey.ID {
		t.Fatalf("resolved = %x ok=%v, want perm %x", got, ok, permKey.ID)
	}
}

func TestPhoneCodeAcceptsTDesktopDigitsOnlySignIn(t *testing.T) {
	ctx := context.Background()
	users := memory.NewUserStore()
	authz := memory.NewAuthorizationStore()
	svc := NewService(users, authz, memory.NewCodeStore(), nil, nil, "12345")

	hash, err := svc.SendCode(ctx, "+15550004310")
	if err != nil {
		t.Fatalf("SendCode: %v", err)
	}

	_, _, needSignUp, err := svc.SignIn(ctx, domain.Authorization{}, "15550004310", hash, "12345")
	if err != nil {
		t.Fatalf("SignIn with digits-only phone: %v", err)
	}
	if !needSignUp {
		t.Fatal("SignIn needSignUp = false, want true")
	}

	u, _, err := svc.SignUp(ctx, domain.Authorization{}, "+1 555 000 4310", hash, "Test", "User")
	if err != nil {
		t.Fatalf("SignUp with formatted phone: %v", err)
	}
	if u.Phone != "15550004310" {
		t.Fatalf("created phone = %q, want normalized digits", u.Phone)
	}
	if u.ID != domain.UserIDSequenceBase {
		t.Fatalf("created user id = %d, want base %d", u.ID, domain.UserIDSequenceBase)
	}
}

func TestMultipleAuthKeysKeepSeparateUsers(t *testing.T) {
	ctx := context.Background()
	authz := memory.NewAuthorizationStore()
	svc := NewService(memory.NewUserStore(), authz, memory.NewCodeStore(), nil, nil, "12345")
	var key1, key2 [8]byte
	key1[0] = 1
	key2[0] = 2

	hash1, err := svc.SendCode(ctx, "+15550005001")
	if err != nil {
		t.Fatalf("SendCode user1: %v", err)
	}
	user1, _, err := svc.SignUp(ctx, domain.Authorization{AuthKeyID: key1}, "+15550005001", hash1, "One", "")
	if err != nil {
		t.Fatalf("SignUp user1: %v", err)
	}
	hash2, err := svc.SendCode(ctx, "+15550005002")
	if err != nil {
		t.Fatalf("SendCode user2: %v", err)
	}
	user2, _, err := svc.SignUp(ctx, domain.Authorization{AuthKeyID: key2}, "+15550005002", hash2, "Two", "")
	if err != nil {
		t.Fatalf("SignUp user2: %v", err)
	}

	got1, found, err := svc.UserID(ctx, key1)
	if err != nil || !found || got1 != user1.ID {
		t.Fatalf("key1 user = %d found=%v err=%v, want %d", got1, found, err, user1.ID)
	}
	got2, found, err := svc.UserID(ctx, key2)
	if err != nil || !found || got2 != user2.ID {
		t.Fatalf("key2 user = %d found=%v err=%v, want %d", got2, found, err, user2.ID)
	}
	if got1 == got2 {
		t.Fatalf("auth keys mapped to same user id %d", got1)
	}
}

func TestLogOutThenSignInSameAuthKeySwitchesUser(t *testing.T) {
	ctx := context.Background()
	authz := memory.NewAuthorizationStore()
	svc := NewService(memory.NewUserStore(), authz, memory.NewCodeStore(), nil, nil, "12345")
	var key [8]byte
	key[0] = 9

	hash1, err := svc.SendCode(ctx, "+15550006001")
	if err != nil {
		t.Fatalf("SendCode user1: %v", err)
	}
	user1, _, err := svc.SignUp(ctx, domain.Authorization{AuthKeyID: key}, "+15550006001", hash1, "One", "")
	if err != nil {
		t.Fatalf("SignUp user1: %v", err)
	}
	if got, found, err := svc.UserID(ctx, key); err != nil || !found || got != user1.ID {
		t.Fatalf("initial auth user = %d found=%v err=%v, want %d", got, found, err, user1.ID)
	}
	if err := svc.LogOut(ctx, key); err != nil {
		t.Fatalf("LogOut: %v", err)
	}
	if got, found, err := svc.UserID(ctx, key); err != nil || found || got != 0 {
		t.Fatalf("after logout user = %d found=%v err=%v, want none", got, found, err)
	}

	hash2, err := svc.SendCode(ctx, "+15550006002")
	if err != nil {
		t.Fatalf("SendCode user2: %v", err)
	}
	user2, _, err := svc.SignUp(ctx, domain.Authorization{AuthKeyID: key}, "+15550006002", hash2, "Two", "")
	if err != nil {
		t.Fatalf("SignUp user2: %v", err)
	}
	if got, found, err := svc.UserID(ctx, key); err != nil || !found || got != user2.ID {
		t.Fatalf("after switch user = %d found=%v err=%v, want %d", got, found, err, user2.ID)
	}
	if user1.ID == user2.ID {
		t.Fatalf("user ids did not change after switch: %d", user1.ID)
	}
}

func TestSignUpWritesOfficialLoginMessage(t *testing.T) {
	ctx := context.Background()
	dialogs := memory.NewDialogStore()
	messages := memory.NewMessageStore(dialogs)
	svc := NewService(memory.NewUserStore(), memory.NewAuthorizationStore(), memory.NewCodeStore(), nil, nil, "12345", WithLoginMessages(messages, dialogs))

	hash, err := svc.SendCode(ctx, "+15550004311")
	if err != nil {
		t.Fatalf("SendCode: %v", err)
	}
	u, msg, err := svc.SignUp(ctx, domain.Authorization{}, "+15550004311", hash, "Test", "User")
	if err != nil {
		t.Fatalf("SignUp: %v", err)
	}

	list, err := dialogs.ListByUser(ctx, u.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("ListByUser: %v", err)
	}
	if len(list.Dialogs) != 1 || list.Dialogs[0].Peer.ID != domain.OfficialSystemUserID {
		t.Fatalf("dialogs = %+v, want official system dialog", list.Dialogs)
	}
	if len(list.Users) != 1 || list.Users[0].ID != domain.OfficialSystemUserID || !list.Users[0].Verified || !list.Users[0].Support {
		t.Fatalf("users = %+v, want verified support system user", list.Users)
	}
	if msg.ID == 0 || !strings.Contains(msg.Body, "Login code: 12345") {
		t.Fatalf("login message = %+v, want returned official login code message", msg)
	}
	if len(list.Messages) != 1 || !strings.Contains(list.Messages[0].Body, "Login code: 12345") {
		t.Fatalf("messages = %+v, want login code message", list.Messages)
	}
	if list.Dialogs[0].TopMessage != list.Messages[0].ID || list.Dialogs[0].UnreadCount != 1 {
		t.Fatalf("dialog top/unread = %+v, message = %+v", list.Dialogs[0], list.Messages[0])
	}
}

func testAuthKey(seed byte) mtcrypto.AuthKey {
	var raw mtcrypto.Key
	for i := range raw {
		raw[i] = seed + byte(i)
	}
	return raw.WithID()
}

func saveAuthKey(t *testing.T, keys store.AuthKeyStore, key mtcrypto.AuthKey) {
	t.Helper()
	var value [256]byte
	copy(value[:], key.Value[:])
	if err := keys.Save(context.Background(), store.AuthKeyData{ID: key.ID, Value: value}); err != nil {
		t.Fatalf("save auth key: %v", err)
	}
}
