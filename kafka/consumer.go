package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func runConsumer() {

	brokerAddress := "localhost:9092"
	topic := "jobs"

	db, err := ConnectDB()
	if err != nil {
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

	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    "jobs-dlq",
		Balancer: &kafka.LeastBytes{},
	}

	defer dlqWriter.Close()

	ctx := context.Background()
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			fmt.Println("Failed to read message:", err)
			return
		}
		err = ProcessWithRetry(ctx, message, db)
		if err != nil {
			fmt.Println("Failed to process message:", err)

			if dlqErr := SendToDLQ(ctx, dlqWriter, message, err); dlqErr != nil {
				fmt.Println("Failed to send message to DLQ:", dlqErr)
				return
			}

			fmt.Println("Message sent to DLQ successfully!")
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			fmt.Println("Failed to commit message:", err)
			return
		}
		fmt.Println("Message committed successfully!")
	}

}

var ErrInvalidMessage error = errors.New("invalid message")

func ProcessMessage(ctx context.Context, message kafka.Message, db *sql.DB) error {
	var input JobMessage

	err := json.Unmarshal(message.Value, &input)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}

	if strings.TrimSpace(input.JobID) == "" {
		return ErrInvalidMessage
	}

	if strings.TrimSpace(input.Type) == "" {
		return ErrInvalidMessage
	}

	var status string

	checkQuery := `
	SELECT status FROM jobs WHERE job_id = $1
	`
	err = db.QueryRowContext(ctx, checkQuery, input.JobID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows){
		return fmt.Errorf("job not found: %s", input.JobID)
	}
	if err != nil{
		return err
	}
	if status == "completed"{
		return nil
	}
	if status != "pending" {
    return fmt.Errorf("unexpected job status: %s", status)
	}

	query := `
	UPDATE jobs
	SET status = 'completed'
	WHERE job_id = $1 AND status = 'pending'
	`

	result, err := db.ExecContext(ctx, query, input.JobID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		var currentStatus string

		err = db.QueryRowContext(ctx, checkQuery, input.JobID).Scan(&currentStatus)
		if err != nil{
			return err
		}
		if currentStatus == "completed"{
			return nil
		}

		return fmt.Errorf("job status was not updated: %s", input.JobID)
	}

	return nil
}

func ProcessWithRetry(ctx context.Context, message kafka.Message, db *sql.DB) error {
	maxAttempts := 3
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			lastErr = err
			return err
		}

		err := ProcessMessage(ctx, message, db)
		if errors.Is(err, ErrInvalidMessage) {
			return err
		}
		if err == nil {
			return nil
		}

		lastErr = err

		if attempt < maxAttempts {
			delay := time.Duration(attempt) * time.Second

			select {
			case <-time.After(delay):
				fmt.Println("Retrying...")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("all retry attempts failed: %w", lastErr)
}

func SendToDLQ(ctx context.Context, writer *kafka.Writer, message kafka.Message, processErr error) error {
	dlqMessage := DLQMessage{
		OriginalTopic: message.Topic,
		OriginalValue: message.Value,
		ErrorReason:   processErr.Error(),
		FailedAt:      time.Now(),
	}

	dlqValue, err := json.Marshal(dlqMessage)
	if err != nil {
		return err
	}

	err = writer.WriteMessages(ctx, kafka.Message{
		Value: dlqValue,
	})
	if err != nil {
		return err
	}
	return nil
}
