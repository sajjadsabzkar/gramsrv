package rpc

import (
	"context"
	"testing"

	"github.com/gotd/td/clock"
	"github.com/gotd/td/tg"
	"go.uber.org/zap/zaptest"

	appusers "telesrv/internal/app/users"
	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

// TestUploadProfilePhotoPushesUpdateToOtherDevices 守护审计修复：换头像后必须向该账号其它在线
// 设备推送（updateUser 信号 + Updates.Users 带新 self user），否则其它设备头像不刷新。
// 原因：updateUserName 不含 photo 无法刷新头像，唯有带 user 对象最可靠（见 photos.go
// pushSelfPhotoUpdate 注释）。曾完全不推送，本测试防回归。
func TestUploadProfilePhotoPushesUpdateToOtherDevices(t *testing.T) {
	ctx := context.Background()
	userStore := memory.NewUserStore()
	owner, _ := userStore.Create(ctx, domain.User{AccessHash: 11, Phone: "15550001001", FirstName: "Owner"})
	sessions := &captureSessions{}
	files := &fakeFiles{}
	r := New(Config{DC: 2, IP: "127.0.0.1", Port: 2398}, Deps{
		Users:    appusers.NewService(userStore),
		Files:    files,
		Sessions: sessions,
	}, zaptest.NewLogger(t), clock.System)

	req := &tg.PhotosUploadProfilePhotoRequest{}
	req.SetFile(&tg.InputFile{ID: 42, Parts: 1, Name: "a.jpg"}) // File 是 flags 可选字段，须 SetFile 置位
	if _, err := r.onPhotosUploadProfilePhoto(WithUserID(ctx, owner.ID), req); err != nil {
		t.Fatalf("uploadProfilePhoto: %v", err)
	}

	snap := sessions.snapshot()
	if snap.userID != owner.ID {
		t.Fatalf("push target user = %d, want %d", snap.userID, owner.ID)
	}
	updates, ok := snap.message.(*tg.Updates)
	if !ok {
		t.Fatalf("pushed message = %T, want *tg.Updates", snap.message)
	}
	hasUserUpdate := false
	for _, u := range updates.Updates {
		if uu, ok := u.(*tg.UpdateUser); ok && uu.UserID == owner.ID {
			hasUserUpdate = true
		}
	}
	if !hasUserUpdate {
		t.Fatalf("updates = %+v, want UpdateUser for self", updates.Updates)
	}
	if len(updates.Users) == 0 {
		t.Fatal("pushed updates missing self user — other devices cannot refresh avatar")
	}
}
