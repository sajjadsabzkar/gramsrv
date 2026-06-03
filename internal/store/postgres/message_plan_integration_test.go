package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
)

func TestMessagePartitionSeekPlansUseIndexes(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender, err := users.Create(ctx, domain.User{
		AccessHash: 51,
		Phone:      "+1999" + suffix + "01",
		FirstName:  "PlanSender",
	})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(ctx, domain.User{
		AccessHash: 52,
		Phone:      "+1999" + suffix + "02",
		FirstName:  "PlanRecipient",
	})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})

	messages := NewMessageStore(pool)
	for i := 0; i < 3; i++ {
		if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID:    sender.ID,
			RecipientUserID: recipient.ID,
			RandomID:        int64(9000 + i),
			Message:         "plan check",
			Date:            1700000300 + i,
		}); err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		UPDATE dispatch_outbox
		SET status = 'dispatching',
		    updated_at = now() - interval '1 minute'
		WHERE target_user_id = $1
	`, recipient.ID); err != nil {
		t.Fatalf("mark dispatch stale: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin explain tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatalf("disable seqscan: %v", err)
	}

	historyPlan := explainText(t, ctx, tx, `
SELECT box_id
FROM message_boxes
WHERE owner_user_id = $1
  AND peer_type = 'user'
  AND peer_id = $2
  AND NOT deleted
  AND box_id < $3
ORDER BY box_id DESC
LIMIT 20
`, recipient.ID, sender.ID, 100000)
	requirePlanUsesPartitionIndex(t, historyPlan, "message_boxes")
	requirePlanNotContains(t, historyPlan, "Append")

	updatesPlan := explainText(t, ctx, tx, `
SELECT pts
FROM user_update_events
WHERE user_id = $1
  AND pts > $2
ORDER BY pts ASC
LIMIT 100
`, recipient.ID, 0)
	requirePlanUsesPartitionIndex(t, updatesPlan, "user_update_events")
	requirePlanNotContains(t, updatesPlan, "Append")

	dispatchPlan := explainText(t, ctx, tx, `
WITH picked AS (
  SELECT target_user_id, id
  FROM dispatch_outbox
  WHERE (
      status = 'pending'
      AND next_attempt_at <= now()
    )
    OR (
      status = 'dispatching'
      AND updated_at < now() - interval '30 seconds'
    )
  ORDER BY next_attempt_at ASC, target_user_id ASC, id ASC
  LIMIT 100
  FOR UPDATE SKIP LOCKED
)
SELECT target_user_id, id
FROM picked
`)
	requirePlanContains(t, dispatchPlan, "dispatch_outbox_p")
	requirePlanContains(t, dispatchPlan, "Index")
	requirePlanNotContains(t, dispatchPlan, "Seq Scan")
}

func explainText(t *testing.T, ctx context.Context, tx pgx.Tx, query string, args ...any) string {
	t.Helper()
	rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+query, args...)
	if err != nil {
		t.Fatalf("explain query: %v", err)
	}
	defer rows.Close()

	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan explain row: %v", err)
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read explain rows: %v", err)
	}
	return b.String()
}

func requirePlanUsesPartitionIndex(t *testing.T, plan, table string) {
	t.Helper()
	requirePlanContains(t, plan, table+"_p")
	requirePlanContains(t, plan, "Index")
	requirePlanNotContains(t, plan, "Seq Scan")
}

func requirePlanContains(t *testing.T, plan string, needle string) {
	t.Helper()
	if !strings.Contains(plan, needle) {
		t.Fatalf("plan missing %q:\n%s", needle, plan)
	}
}

func requirePlanNotContains(t *testing.T, plan string, needle string) {
	t.Helper()
	if strings.Contains(plan, needle) {
		t.Fatalf("plan contains %q:\n%s", needle, plan)
	}
}
