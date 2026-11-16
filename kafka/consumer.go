package main

import (
	"context"
	"fmt"
	"errors"
	"strings"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	brokerAddress := "localhost:9092"
	topic := "jobs"

	db, err := ConnectDB()
	if err != nil{
		fmt.Println("Database connection failed:", err)
		return
	}
	defer db.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "job-workers",
	})

	defer reader.Close()

	ctx := context.Background()
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			fmt.Println("Failed to read message:", err)
			return
		}
		err = ProcessWithRetry(ctx, message, db)
		if err != nil{
			fmt.Println("Failed to process message:",err)
			return
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			fmt.Println("Failed to commit message:", err)
			return
		}
		fmt.Println("Message committed successfully!")
	}

}

func ProcessMessage(ctx context.Context, message kafka.Message, db *sql.DB)error{
	var input JobMessage

	err := json.Unmarshal(message.Value, &input)
	if err != nil{
		return err
	}

	if strings.TrimSpace(input.JobID) == "" {
    return errors.New("job_id is required")
	}

	if strings.TrimSpace(input.Type) == "" {
			return errors.New("type is required")
	}

	query := `INSERT INTO jobs (job_id, type, payload) VALUES($1, $2, $3)
	ON CONFLICT (job_id) DO NOTHING`

	_, err = db.ExecContext(ctx,
		query,
		input.JobID,
		input.Type,
		input.Payload,
	)
	if err != nil{
		return err
	}


	return nil
}

func ProcessWithRetry(ctx context.Context, message kafka.Message, db *sql.DB)error{
	maxAttempts := 3
	var lastErr error
	
	for attempt := 1; attempt <= maxAttempts; attempt++{
		if err := ctx.Err(); err != nil{
			lastErr = err
			return err
		}

		err := ProcessMessage(ctx, message, db)
		if err == nil{
			return nil
		}

		lastErr = err

		if attempt < maxAttempts{
			delay := time.Duration(attempt)*time.Second

			select{
			case <-time.After(delay):
				fmt.Println("Retrying...")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("all retry attempts failed: %w", lastErr)
}