package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type TaskMessage struct {
	ID   string
	Type string
}

type RedisStreamPublisher struct {
	client     *redis.Client
	streamName string
}

func NewRedisStreamPublisher(client *redis.Client, streamName string) *RedisStreamPublisher {
	return &RedisStreamPublisher{
		client:     client,
		streamName: streamName,
	}
}

func (p *RedisStreamPublisher) PublishTask(ctx context.Context, message TaskMessage) error {
	_, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		Values: map[string]any{
			"task_id":   message.ID,
			"task_type": message.Type,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("publish task %s to redis stream %s: %w", message.ID, p.streamName, err)
	}

	return nil
}
