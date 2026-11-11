package main

import (
	"context"
	"fmt"
	"sync"
)

func worker(ctx context.Context, id int, jobs <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Worker stopped:", id)
			return
		case job, ok := <-jobs:
			if ok {
				err := job.Process(ctx)
				if err != nil {
					fmt.Println("Job error:", err)
				}
			} else {
				fmt.Println("Worker finished:", id)
				return
			}
		}
	}
}
