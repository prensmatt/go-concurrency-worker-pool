package main

import (
	"context"
	"errors"
	"testing"
)

func TestRetryJobSuccess(t *testing.T) {
	attempts := 0
	doJob := func() error {
		attempts++
		return nil
	}

	ctx := context.Background()

	err := retryJob(ctx, doJob)
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
	if err != nil {
		t.Errorf("Error happened: %v", err)
	}
}

func TestRetryJobAfterFailures(t *testing.T) {
	attempts := 0
	doJob := func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary error")
		}
		return nil
	}

	ctx := context.Background()
	err := retryJob(ctx, doJob)

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}

	if err != nil {
		t.Errorf("Error happened %v", err)
	}
}

func TestRetryJobAllAttemptsFail(t *testing.T) {
	attempts := 0
	doJob := func() error {
		attempts++
		return errors.New("Temporary error")
	}
	ctx := context.Background()
	err := retryJob(ctx, doJob)
	if err == nil {
		t.Errorf("Expected an error, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryJobContextCanceled(t *testing.T) {
	attempts := 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	doJob := func() error {
		attempts++
		return nil
	}

	err := retryJob(ctx, doJob)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Cancelled error, got %v", err)
	}
	if attempts != 0 {
		t.Errorf("Expected attempts 0, got %d", attempts)
	}
}
