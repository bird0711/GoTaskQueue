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

func TestStreamMessagesFromRedisParsesBatch(t *testing.T) {
	messages, err := streamMessagesFromRedis([]redis.XMessage{
		{
			ID: "1-0",
			Values: map[string]interface{}{
				"task_id":   "task_1",
				"task_type": "demo.echo",
			},
		},
		{
			ID: "2-0",
			Values: map[string]interface{}{
				"task_id":   "task_2",
				"task_type": "demo.echo",
				"trace_id":  "trace_2",
			},
		},
	}, false)
	if err != nil {
		t.Fatalf("parse stream messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].RedisID != "1-0" || messages[0].Task.ID != "task_1" {
		t.Fatalf("unexpected first message: %#v", messages[0])
	}
	if messages[1].RedisID != "2-0" || messages[1].Task.ID != "task_2" || messages[1].Task.TraceID != "trace_2" {
		t.Fatalf("unexpected second message: %#v", messages[1])
	}
}
