package main

import (
	"context"
	"testing"
)

func TestPublishOutboxConcurrent(t *testing.T) {
	ctx := context.Background()

	db, err := ConnectDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	query := `
SELECT id
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT 1
FOR UPDATE SKIP LOCKED
`

	var id1 int64
	err = tx1.QueryRowContext(ctx, query).Scan(&id1)
	if err != nil {
		t.Fatal(err)
	}

	var id2 int64

	err = tx2.QueryRowContext(ctx, query).Scan(&id2)
	if err != nil {
		t.Fatal(err)
	}
	if id1 == id2 {
		t.Fatalf("both transactions selected the same row: %d", id1)
	}
}
