package main

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

func PublishJob(ctx context.Context, writer *kafka.Writer, job JobMessage) error {

	value, err := json.Marshal(job)
	if err != nil {
		return err
	}
	err = writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(job.JobID),
			Value: value,
		},
	)
	if err != nil {
		return err
	}
	return nil
}
