package main

import (
	"context"
	"fmt"
	"sync"
)

func worker(ctx context.Context, id int, jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Worker stopped:", id)
			return
		case job, ok := <-jobs:
			if ok {
				processJob(ctx, job)
			} else {
				fmt.Println("Worker finished:", id)
				return
			}
		}
	}
}
