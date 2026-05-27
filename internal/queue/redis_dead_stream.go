package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type DeadTaskMessage struct {
	ID         string
	Type       string
	TraceID    string
	LastError  string
	RetryCount int
}

type RedisDeadStreamPublisher struct {
	client     *redis.Client
	streamName string
}

func NewRedisDeadStreamPublisher(client *redis.Client, streamName string) *RedisDeadStreamPublisher {
	return &RedisDeadStreamPublisher{
		client:     client,
		streamName: streamName,
	}
}

func (p *RedisDeadStreamPublisher) PublishDead(ctx context.Context, message DeadTaskMessage) error {
	values := map[string]any{
		"task_id":     message.ID,
		"task_type":   message.Type,
		"retry_count": message.RetryCount,
	}
	if message.TraceID != "" {
		values["trace_id"] = message.TraceID
	}
	if message.LastError != "" {
		values["last_error"] = message.LastError
	}
	if _, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		Values: values,
	}).Result(); err != nil {
		return fmt.Errorf("publish dead task %s to redis stream %s: %w", message.ID, p.streamName, err)
	}
	return nil
}
