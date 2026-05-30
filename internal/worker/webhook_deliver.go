package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/task"
)

const defaultWebhookTimeout = 10 * time.Second

type WebhookDeliverHandler struct {
	Client  *http.Client
	Timeout time.Duration
}

type webhookDeliverPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

func (h WebhookDeliverHandler) Handle(ctx context.Context, taskModel *task.Task) error {
	payload, err := parseWebhookDeliverPayload(taskModel.Payload)
	if err != nil {
		return err
	}

	timeout := h.Timeout
	if timeout <= 0 {
		timeout = defaultWebhookTimeout
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, payload.Method, payload.URL, bytes.NewReader(payload.Body))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	for key, value := range payload.Headers {
		req.Header.Set(key, value)
	}

	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return Retryable(fmt.Errorf("webhook request timed out after %s", timeout))
		}
		return Retryable(fmt.Errorf("webhook request failed: %w", err))
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices:
		return nil
	case resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError:
		return NonRetryable(fmt.Errorf("webhook returned non-retryable status %d", resp.StatusCode))
	case resp.StatusCode >= http.StatusInternalServerError:
		return Retryable(fmt.Errorf("webhook returned retryable status %d", resp.StatusCode))
	default:
		return Retryable(fmt.Errorf("webhook returned unexpected status %d", resp.StatusCode))
	}
}

func parseWebhookDeliverPayload(raw json.RawMessage) (webhookDeliverPayload, error) {
	if len(raw) == 0 {
		return webhookDeliverPayload{}, NonRetryable(errors.New("webhook payload is required"))
	}

	var payload webhookDeliverPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return webhookDeliverPayload{}, NonRetryable(fmt.Errorf("decode webhook payload: %w", err))
	}
	payload.URL = strings.TrimSpace(payload.URL)
	payload.Method = strings.ToUpper(strings.TrimSpace(payload.Method))
	if payload.Method == "" {
		payload.Method = http.MethodPost
	}
	if payload.URL == "" {
		return webhookDeliverPayload{}, NonRetryable(errors.New("webhook payload url is required"))
	}
	if payload.Body == nil {
		payload.Body = json.RawMessage(`null`)
	}

	return payload, nil
}
