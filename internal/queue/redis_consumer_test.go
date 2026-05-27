package queue

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestStreamMessageFromRedisIncludesTraceID(t *testing.T) {
	message, err := streamMessageFromRedis(redis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"task_id":   "task_abc",
			"task_type": "demo.echo",
			"trace_id":  "trace_xyz",
		},
	}, false)
	if err != nil {
		t.Fatalf("parse stream message: %v", err)
	}
	if message.Task.TraceID != "trace_xyz" {
		t.Fatalf("expected trace id trace_xyz, got %q", message.Task.TraceID)
	}
}

func TestStreamMessageFromRedisToleratesMissingTraceID(t *testing.T) {
	message, err := streamMessageFromRedis(redis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"task_id":   "task_abc",
			"task_type": "demo.echo",
		},
	}, false)
	if err != nil {
		t.Fatalf("parse stream message: %v", err)
	}
	if message.Task.TraceID != "" {
		t.Fatalf("expected empty trace id, got %q", message.Task.TraceID)
	}
}
