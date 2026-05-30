package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/task"
)

func TestWebhookDeliverHandlerSuccess(t *testing.T) {
	var gotMethod string
	var gotHeader string
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotHeader = r.Header.Get("X-Test")
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		gotBody = string(body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	handler := WebhookDeliverHandler{Client: server.Client(), Timeout: time.Second}
	err := handler.Handle(context.Background(), webhookTask(t, map[string]any{
		"url":     server.URL,
		"method":  "put",
		"headers": map[string]string{"X-Test": "ok"},
		"body":    map[string]any{"message": "hello"},
	}))
	if err != nil {
		t.Fatalf("handle webhook: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("expected PUT, got %q", gotMethod)
	}
	if gotHeader != "ok" {
		t.Fatalf("expected X-Test header ok, got %q", gotHeader)
	}
	if gotBody != `{"message":"hello"}` {
		t.Fatalf("expected JSON body, got %q", gotBody)
	}
}

func TestWebhookDeliverHandlerReturnsNonRetryable4xxError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	handler := WebhookDeliverHandler{Client: server.Client(), Timeout: time.Second}
	err := handler.Handle(context.Background(), webhookTask(t, map[string]any{
		"url": server.URL,
	}))
	if err == nil {
		t.Fatal("expected 4xx error")
	}
	if !strings.Contains(err.Error(), "non-retryable status 400") {
		t.Fatalf("expected non-retryable 400 error, got %v", err)
	}
	if !IsNonRetryable(err) {
		t.Fatalf("expected non-retryable error, got %T", err)
	}
}

func TestWebhookDeliverHandlerReturnsRetryable5xxError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	handler := WebhookDeliverHandler{Client: server.Client(), Timeout: time.Second}
	err := handler.Handle(context.Background(), webhookTask(t, map[string]any{
		"url": server.URL,
	}))
	if err == nil {
		t.Fatal("expected 5xx error")
	}
	if !strings.Contains(err.Error(), "retryable status 503") {
		t.Fatalf("expected retryable 503 error, got %v", err)
	}
	if IsNonRetryable(err) {
		t.Fatal("expected retryable 5xx error")
	}
}

func TestWebhookDeliverHandlerTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	defer server.Close()

	handler := WebhookDeliverHandler{Client: server.Client(), Timeout: 10 * time.Millisecond}
	err := handler.Handle(context.Background(), webhookTask(t, map[string]any{
		"url": server.URL,
	}))
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestWebhookDeliverHandlerInvalidPayload(t *testing.T) {
	handler := WebhookDeliverHandler{Timeout: time.Second}
	err := handler.Handle(context.Background(), &task.Task{
		ID:      "task_bad",
		Type:    "webhook.deliver",
		Payload: json.RawMessage(`{"method":"POST"}`),
	})
	if err == nil {
		t.Fatal("expected payload validation error")
	}
	if !strings.Contains(err.Error(), "url is required") {
		t.Fatalf("expected missing url error, got %v", err)
	}
	if !IsNonRetryable(err) {
		t.Fatalf("expected non-retryable payload error, got %T", err)
	}
}

func TestWebhookDeliverHandlerInvalidPayloadFieldType(t *testing.T) {
	handler := WebhookDeliverHandler{Timeout: time.Second}
	err := handler.Handle(context.Background(), &task.Task{
		ID:      "task_bad",
		Type:    "webhook.deliver",
		Payload: json.RawMessage(`{"url":123}`),
	})
	if err == nil {
		t.Fatal("expected payload validation error")
	}
	if !strings.Contains(err.Error(), "decode webhook payload") {
		t.Fatalf("expected decode payload error, got %v", err)
	}
	if !IsNonRetryable(err) {
		t.Fatalf("expected non-retryable payload error, got %T", err)
	}
}

func webhookTask(t *testing.T, payload map[string]any) *task.Task {
	t.Helper()

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return &task.Task{
		ID:      "task_webhook",
		Type:    "webhook.deliver",
		Payload: rawPayload,
	}
}
