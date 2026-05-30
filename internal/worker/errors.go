package worker

import "errors"

type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string {
	return e.Err.Error()
}

func (e RetryableError) Unwrap() error {
	return e.Err
}

type NonRetryableError struct {
	Err error
}

func (e NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e NonRetryableError) Unwrap() error {
	return e.Err
}

func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return RetryableError{Err: err}
}

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return NonRetryableError{Err: err}
}

func IsNonRetryable(err error) bool {
	var target NonRetryableError
	return errors.As(err, &target)
}
