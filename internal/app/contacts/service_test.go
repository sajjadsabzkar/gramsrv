package contacts

import (
	"context"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestImportContactsBatchesPhonesAndDedupesUpserts(t *testing.T) {
	ctx := context.Background()
	users := memory.NewUserStore()
	contactsStore := memory.NewContactStore()
	owner, err := users.Create(ctx, domain.User{Phone: "100", FirstName: "Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	target, err := users.Create(ctx, domain.User{Phone: "15551234567", FirstName: "Alice"})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	svc := NewService(contactsStore, users)

	res, err := svc.ImportContacts(ctx, owner.ID, []domain.ContactInput{
		{ClientID: 11, Phone: "+1 (555) 123-4567", FirstName: "A"},
		{ClientID: 12, Phone: "15551234567", FirstName: "Alice Final"},
	})
	if err != nil {
		t.Fatalf("ImportContacts: %v", err)
	}
	if len(res.Imported) != 2 {
		t.Fatalf("imported = %d, want 2", len(res.Imported))
	}
	if res.Imported[0].UserID != target.ID || res.Imported[1].UserID != target.ID {
		t.Fatalf("imported user ids = %+v, want target %d", res.Imported, target.ID)
	}
	if len(res.Contacts) != 1 {
		t.Fatalf("contacts = %d, want 1 deduped upsert", len(res.Contacts))
	}
	if res.Contacts[0].FirstName != "Alice Final" {
		t.Fatalf("contact first name = %q, want final input", res.Contacts[0].FirstName)
	}
}
