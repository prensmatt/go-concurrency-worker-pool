package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {

	brokerAddress := "localhost:9092"
	topic := "jobs"

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
		err = ProcessMessage(message)
		if err != nil{
			fmt.Println("Failed to process message:",err)
			continue
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			fmt.Println("Failed to commit message:", err)
			return
		}
		fmt.Println("Message committed successfully!")
	}

}

func ProcessMessage(message kafka.Message)error{
	fmt.Println("Received message:", string(message.Value))

	return nil
}