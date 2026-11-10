package main

import (
	"context"
	"fmt"
	"sync"
)

func main() {
	ctx := context.Background()

	var wg sync.WaitGroup

	jobs := make(chan int, 10)

	wg.Add(3)
	go worker(ctx, 1, jobs, &wg)
	go worker(ctx, 2, jobs, &wg)
	go worker(ctx, 3, jobs, &wg)

	for job := 1; job < 11; job++ {
		jobs <- job
	}

	close(jobs)
	wg.Wait()
	fmt.Println("Program finished")
}
