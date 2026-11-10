package main

import (
	"context"
	"fmt"
	"time"
)

func processJob(ctx context.Context, job int) {
	fmt.Println("Job is started")

	select {
	case <-time.After(2 * time.Second):
		fmt.Println("Job is finished:", job)
	case <-ctx.Done():
		fmt.Println("Job is cancelled:", job)
		return
	}
}
