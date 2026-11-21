package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func PublishOutbox(ctx context.Context, db *sql.DB, writer *kafka.Writer) error {
	query := `
	SELECT id, job_id, topic, payload
	FROM outbox
	WHERE published_at IS NULL
	ORDER BY id
	LIMIT 100
	FOR UPDATE SKIP LOCKED
	`
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var item OutboxJobMessage
		if err := rows.Scan(&item.ID, &item.JobID, &item.Topic, &item.Payload); err != nil {
			return err
		}
		message := kafka.Message{
			Topic: item.Topic,
			Key:   []byte(item.JobID),
			Value: item.Payload,
		}
		err := writer.WriteMessages(ctx, message)
		if err != nil {
			return err
		}
		updateQuery := `
		UPDATE outbox SET published_at = $1 WHERE id = $2
		`
		result, err := tx.ExecContext(ctx, updateQuery, time.Now(), item.ID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return fmt.Errorf("expected 1 updated row, got %d", affected)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return tx.Commit()
}

func runPublisher(ctx context.Context, db *sql.DB, writer *kafka.Writer) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	if ctx.Err() != nil {
		fmt.Println("Publisher shutting down...")
		return
	}

	err := PublishOutbox(ctx, db, writer)
	if err != nil {
		fmt.Println("Publishing failed:", err)
	}

	for {
		select {
		case <-ticker.C:
			err := PublishOutbox(ctx, db, writer)
			if err != nil {
				fmt.Println("Publishing failed:", err)
				continue
			}
		case <-ctx.Done():
			fmt.Println("Publisher shutting down...")
			return
		}
	}
}
