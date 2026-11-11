package main

import (
	"context"
	"errors"
	"fmt"
)

type Job interface {
	Process(ctx context.Context) error
}

type EmailJob struct {
	To string
}
type ReportJob struct {
	Name string
}

func (j EmailJob) Process(ctx context.Context) error {
	// fmt.Println("Email send to", j.To)
	// return nil
	err := errors.New("email sending failed")
	return err
}

func (j ReportJob) Process(ctx context.Context) error {
	fmt.Println("Report was created by", j.Name)
	return nil
}

// func processJob(ctx context.Context, job int) {
// 	fmt.Println("Job is started")

// 	select {
// 	case <-time.After(2 * time.Second):
// 		fmt.Println("Job is finished:", job)
// 	case <-ctx.Done():
// 		fmt.Println("Job is cancelled:", job)
// 		return
// 	}
// }
