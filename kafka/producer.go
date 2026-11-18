package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokerAddress := "localhost:9092"
	topic := "jobs"

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	ctx := context.Background()

	err := writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte("job-1"),
			Value: []byte(`{"type": "email"}`),
		},
	)
	if err != nil {
		fmt.Println("Failed to write message:", err)
		return
	}
	fmt.Println("Message sent successfully!")
}
