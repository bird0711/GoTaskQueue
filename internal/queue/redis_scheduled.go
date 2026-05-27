package queue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisScheduledQueue struct {
	client *redis.Client
	key    string
}

func NewRedisScheduledQueue(client *redis.Client, key string) *RedisScheduledQueue {
	return &RedisScheduledQueue{
		client: client,
		key:    key,
	}
}

func (q *RedisScheduledQueue) ScheduleTask(ctx context.Context, taskID string, runAt time.Time) error {
	if err := q.client.ZAdd(ctx, q.key, redis.Z{
		Score:  float64(runAt.Unix()),
		Member: taskID,
	}).Err(); err != nil {
		return fmt.Errorf("schedule task %s in redis zset %s: %w", taskID, q.key, err)
	}
	return nil
}

func (q *RedisScheduledQueue) DueTaskIDs(ctx context.Context, now time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	ids, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     q.key,
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.Unix(), 10),
		ByScore: true,
		Count:   int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("scan due redis zset %s: %w", q.key, err)
	}
	return ids, nil
}

func (q *RedisScheduledQueue) Remove(ctx context.Context, taskID string) error {
	if err := q.client.ZRem(ctx, q.key, taskID).Err(); err != nil {
		return fmt.Errorf("remove task %s from redis zset %s: %w", taskID, q.key, err)
	}
	return nil
}
