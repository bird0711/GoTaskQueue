package task

import "time"

type FailureDecision struct {
	Status      Status
	RetryCount  int
	NextRetryAt *time.Time
}

func DecideFailure(now time.Time, retryCount int, maxRetries int) FailureDecision {
	nextRetryCount := retryCount + 1
	if nextRetryCount > maxRetries {
		return FailureDecision{
			Status:     StatusDead,
			RetryCount: maxRetries,
		}
	}

	nextRetryAt := now.Add(BackoffDelay(nextRetryCount))
	return FailureDecision{
		Status:      StatusRetrying,
		RetryCount:  nextRetryCount,
		NextRetryAt: &nextRetryAt,
	}
}

func BackoffDelay(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	if retryCount > 6 {
		retryCount = 6
	}

	return time.Duration(1<<uint(retryCount-1)) * time.Second
}
