package task

import (
	"testing"
	"time"
)

func TestDecideFailureRetriesBeforeMaxRetries(t *testing.T) {
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	decision := DecideFailure(now, 0, 3)

	if decision.Status != StatusRetrying {
		t.Fatalf("expected retrying, got %q", decision.Status)
	}
	if decision.RetryCount != 1 {
		t.Fatalf("expected retry count 1, got %d", decision.RetryCount)
	}
	if decision.NextRetryAt == nil {
		t.Fatal("expected next retry time")
	}
	if !decision.NextRetryAt.Equal(now.Add(1 * time.Second)) {
		t.Fatalf("expected next retry at +1s, got %s", decision.NextRetryAt)
	}
}

func TestDecideFailureDeadAfterMaxRetries(t *testing.T) {
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	decision := DecideFailure(now, 3, 3)

	if decision.Status != StatusDead {
		t.Fatalf("expected dead, got %q", decision.Status)
	}
	if decision.RetryCount != 4 {
		t.Fatalf("expected retry count 4, got %d", decision.RetryCount)
	}
	if decision.NextRetryAt != nil {
		t.Fatalf("expected nil next retry at, got %s", decision.NextRetryAt)
	}
}

func TestBackoffDelayCaps(t *testing.T) {
	if BackoffDelay(1) != 1*time.Second {
		t.Fatalf("expected first retry delay to be 1s")
	}
	if BackoffDelay(7) != 32*time.Second {
		t.Fatalf("expected capped retry delay to be 32s")
	}
}
