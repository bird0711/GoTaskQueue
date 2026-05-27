package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func TestWaitForRunnerReturnsWhenRunnerExits(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	errCh <- nil

	done := make(chan struct{})
	go func() {
		waitForRunner(context.Background(), "worker", errCh, logger)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitForRunner did not return after runner exited cleanly")
	}
}

func TestWaitForRunnerIgnoresServerClosedError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	errCh <- http.ErrServerClosed

	done := make(chan struct{})
	go func() {
		waitForRunner(context.Background(), "server", errCh, logger)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitForRunner did not return after server runner exited with ErrServerClosed")
	}
}

func TestWaitForRunnerReturnsOnTimeout(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		waitForRunner(ctx, "worker", errCh, logger)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitForRunner did not return after shutdown context timed out")
	}

	select {
	case errCh <- errors.New("late exit"):
	default:
	}
}
