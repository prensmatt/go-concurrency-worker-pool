package main

import (
	"context"
	"fmt"
	"time"
)

func retryJob(ctx context.Context, doJob func() error) error {
	var err error

	for i := 1; i < 4; i++ {

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		err = doJob()

		if err == nil {
			fmt.Printf("Job done at %v attempt", i)
			return nil
		}
		fmt.Printf("Job failed at %v attempt", i)
		if i == 3 {
			break
		}
		select {
		case <-time.After(2 * time.Second):
			fmt.Println("Retrying")
		case <-ctx.Done():
			fmt.Println("Retry cancelled")
			return ctx.Err()
		}

	}
	return err
}
