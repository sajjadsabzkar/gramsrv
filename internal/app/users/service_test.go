package users

import (
	"context"
	"errors"
	"strings"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestServiceUsernameLifecycle(t *testing.T) {
	ctx := context.Background()
	store := memory.NewUserStore()
	owner, err := store.Create(ctx, domain.User{AccessHash: 1, Phone: "15550000001", FirstName: "Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	other, err := store.Create(ctx, domain.User{AccessHash: 2, Phone: "15550000002", FirstName: "Other", Username: "taken_name"})
	if err != nil {
		t.Fatalf("create other: %v", err)
	}
	svc := NewService(store)

	if ok, err := svc.CheckUsername(ctx, owner.ID, "123bad"); err == nil || ok || !errors.Is(err, domain.ErrUsernameInvalid) {
		t.Fatalf("CheckUsername invalid = ok %v err %v, want username invalid", ok, err)
	}
	if ok, err := svc.CheckUsername(ctx, owner.ID, "taken_name"); err != nil || ok {
		t.Fatalf("CheckUsername occupied = ok %v err %v, want false/nil", ok, err)
	}
	if ok, err := svc.CheckUsername(ctx, owner.ID, "owner_name"); err != nil || !ok {
		t.Fatalf("CheckUsername available = ok %v err %v, want true/nil", ok, err)
	}

	updated, err := svc.UpdateUsername(ctx, owner.ID, "@Owner_Name")
	if err != nil {
		t.Fatalf("UpdateUsername: %v", err)
	}
	if updated.Username != "Owner_Name" {
		t.Fatalf("updated username = %q, want Owner_Name", updated.Username)
	}
	resolved, found, err := svc.ResolveUsername(ctx, other.ID, "owner_name")
	if err != nil || !found || resolved.ID != owner.ID {
		t.Fatalf("ResolveUsername = user %+v found %v err %v, want owner", resolved, found, err)
	}
	if _, err := svc.UpdateUsername(ctx, owner.ID, "TAKEN_NAME"); !errors.Is(err, domain.ErrUsernameOccupied) {
		t.Fatalf("UpdateUsername duplicate err = %v, want username occupied", err)
	}
	phoneUser, found, err := svc.ResolvePhone(ctx, owner.ID, "+1 (555) 000-0002")
	if err != nil || !found || phoneUser.ID != other.ID {
		t.Fatalf("ResolvePhone = user %+v found %v err %v, want other", phoneUser, found, err)
	}
	cleared, err := svc.UpdateUsername(ctx, owner.ID, "")
	if err != nil {
		t.Fatalf("clear username: %v", err)
	}
	if cleared.Username != "" {
		t.Fatalf("cleared username = %q, want empty", cleared.Username)
	}
}

func TestServiceUpdateProfile(t *testing.T) {
	ctx := context.Background()
	store := memory.NewUserStore()
	owner, err := store.Create(ctx, domain.User{AccessHash: 1, Phone: "15550000001", FirstName: "Owner", LastName: "Old"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	svc := NewService(store)

	updated, err := svc.UpdateProfile(ctx, owner.ID, domain.UserProfileUpdate{
		FirstName:    " New ",
		HasFirstName: true,
		LastName:     "Name",
		HasLastName:  true,
		About:        "bio",
		HasAbout:     true,
	})
	if err != nil {
		t.Fatalf("UpdateProfile: %v", err)
	}
	if updated.FirstName != "New" || updated.LastName != "Name" || updated.About != "bio" {
		t.Fatalf("updated profile = %+v, want trimmed names and about", updated)
	}
	if _, err := svc.UpdateProfile(ctx, owner.ID, domain.UserProfileUpdate{FirstName: " ", HasFirstName: true}); !errors.Is(err, domain.ErrFirstNameInvalid) {
		t.Fatalf("empty first name err = %v, want first name invalid", err)
	}
	if _, err := svc.UpdateProfile(ctx, owner.ID, domain.UserProfileUpdate{About: strings.Repeat("x", 71), HasAbout: true}); !errors.Is(err, domain.ErrAboutTooLong) {
		t.Fatalf("long about err = %v, want about too long", err)
	}
}

func TestServiceByIDDoesNotReloadSelf(t *testing.T) {
	ctx := context.Background()
	base := memory.NewUserStore()
	owner, err := base.Create(ctx, domain.User{AccessHash: 1, Phone: "15550000001", FirstName: "Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	target, err := base.Create(ctx, domain.User{AccessHash: 2, Phone: "15550000002", FirstName: "Target"})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	store := &countingUserStore{UserStore: base}
	svc := NewService(store)

	got, found, err := svc.ByID(ctx, owner.ID, target.ID)
	if err != nil || !found || got.ID != target.ID {
		t.Fatalf("ByID = %+v found %v err %v, want target", got, found, err)
	}
	if store.byIDCalls != 1 {
		t.Fatalf("store ByID calls = %d, want 1 target lookup only", store.byIDCalls)
	}
	if store.lastByID != target.ID {
		t.Fatalf("last ByID id = %d, want target %d", store.lastByID, target.ID)
	}
}

type countingUserStore struct {
	*memory.UserStore
	byIDCalls int
	lastByID  int64
}

func (s *countingUserStore) ByID(ctx context.Context, id int64) (domain.User, bool, error) {
	s.byIDCalls++
	s.lastByID = id
	return s.UserStore.ByID(ctx, id)
}
