package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrNoMessages = errors.New("no redis stream messages available")

type StreamMessage struct {
	RedisID   string
	Task      TaskMessage
	Recovered bool
}

type RedisStreamConsumer struct {
	client     *redis.Client
	streamName string
	groupName  string
	consumer   string
	block      time.Duration
}

func NewRedisStreamConsumer(client *redis.Client, streamName, groupName, consumer string, block time.Duration) *RedisStreamConsumer {
	return &RedisStreamConsumer{
		client:     client,
		streamName: streamName,
		groupName:  groupName,
		consumer:   consumer,
		block:      block,
	}
}

func (c *RedisStreamConsumer) EnsureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.streamName, c.groupName, "0").Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("create redis stream consumer group %s on %s: %w", c.groupName, c.streamName, err)
	}
	return nil
}

func (c *RedisStreamConsumer) Read(ctx context.Context) (StreamMessage, error) {
	messages, err := c.ReadBatch(ctx, 1)
	if err != nil {
		return StreamMessage{}, err
	}
	return messages[0], nil
}

func (c *RedisStreamConsumer) ReadBatch(ctx context.Context, count int64) ([]StreamMessage, error) {
	if count <= 0 {
		count = 1
	}

	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.groupName,
		Consumer: c.consumer,
		Streams:  []string{c.streamName, ">"},
		Count:    count,
		Block:    c.block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNoMessages
	}
	if isNoGroup(err) {
		if groupErr := c.EnsureGroup(ctx); groupErr != nil {
			return nil, groupErr
		}
		return nil, ErrNoMessages
	}
	if err != nil {
		return nil, fmt.Errorf("read redis stream %s: %w", c.streamName, err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, ErrNoMessages
	}

	return streamMessagesFromRedis(streams[0].Messages, false)
}

func (c *RedisStreamConsumer) ClaimPending(ctx context.Context, minIdle time.Duration, count int64) ([]StreamMessage, error) {
	pending, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.streamName,
		Group:  c.groupName,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  count,
	}).Result()
	if isNoGroup(err) {
		if groupErr := c.EnsureGroup(ctx); groupErr != nil {
			return nil, groupErr
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect redis stream pending entries on %s: %w", c.streamName, err)
	}
	if len(pending) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(pending))
	for _, entry := range pending {
		ids = append(ids, entry.ID)
	}

	claimed, err := c.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   c.streamName,
		Group:    c.groupName,
		Consumer: c.consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("claim redis stream pending entries on %s: %w", c.streamName, err)
	}

	messages := make([]StreamMessage, 0, len(claimed))
	for _, redisMessage := range claimed {
		message, err := streamMessageFromRedis(redisMessage, true)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}

	return messages, nil
}

func streamMessagesFromRedis(redisMessages []redis.XMessage, recovered bool) ([]StreamMessage, error) {
	messages := make([]StreamMessage, 0, len(redisMessages))
	for _, redisMessage := range redisMessages {
		message, err := streamMessageFromRedis(redisMessage, recovered)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func (c *RedisStreamConsumer) Ack(ctx context.Context, redisID string) error {
	if err := c.client.XAck(ctx, c.streamName, c.groupName, redisID).Err(); err != nil {
		return fmt.Errorf("ack redis message %s on stream %s: %w", redisID, c.streamName, err)
	}
	return nil
}

func streamMessageFromRedis(message redis.XMessage, recovered bool) (StreamMessage, error) {
	taskID, ok := stringValue(message.Values["task_id"])
	if !ok || taskID == "" {
		return StreamMessage{}, fmt.Errorf("redis message %s missing task_id", message.ID)
	}
	taskType, ok := stringValue(message.Values["task_type"])
	if !ok || taskType == "" {
		return StreamMessage{}, fmt.Errorf("redis message %s missing task_type", message.ID)
	}
	traceID, _ := stringValue(message.Values["trace_id"])

	return StreamMessage{
		RedisID:   message.ID,
		Recovered: recovered,
		Task: TaskMessage{
			ID:      taskID,
			Type:    taskType,
			TraceID: traceID,
		},
	}, nil
}

func isBusyGroup(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP")
}

func isNoGroup(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOGROUP")
}

func stringValue(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}
