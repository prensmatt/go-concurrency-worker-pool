package main

import(
	"fmt"
	"sync"
	"context"
)

func worker(ctx context.Context, id int, jobs <-chan int, wg *sync.WaitGroup){
	defer wg.Done()

	for{
		select{
		case <-ctx.Done():
			fmt.Println("Job is cancelled:",id)
			return
		case job,ok := <- jobs:
			if ok{
				processJob(ctx, job)
			}else{
				return
			}
		}
		}
	}
