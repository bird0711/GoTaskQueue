package dependencies

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/bird0711/GoTaskQueue/internal/config"
)

type Dependencies struct {
	Redis    *redis.Client
	Postgres *pgxpool.Pool
}

func Connect(ctx context.Context, cfg config.Config, logger *slog.Logger) (*Dependencies, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.Redis.Addr,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		_ = redisClient.Close()
		return nil, fmt.Errorf("connect redis at %s: %w", cfg.Redis.Addr, err)
	}
	logger.Info("redis connection initialized", "addr", cfg.Redis.Addr)

	postgresPool, err := pgxpool.New(ctx, postgresDSN(cfg.Postgres))
	if err != nil {
		_ = redisClient.Close()
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := postgresPool.Ping(ctx); err != nil {
		postgresPool.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("connect postgres at %s:%s/%s: %w", cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, err)
	}
	logger.Info("postgres connection initialized", "host", cfg.Postgres.Host, "port", cfg.Postgres.Port, "database", cfg.Postgres.Database)

	return &Dependencies{
		Redis:    redisClient,
		Postgres: postgresPool,
	}, nil
}

func (d *Dependencies) Close(logger *slog.Logger) {
	if d == nil {
		return
	}

	if d.Redis != nil {
		if err := d.Redis.Close(); err != nil {
			logger.Error("close redis connection", "error", err)
		} else {
			logger.Info("redis connection closed")
		}
	}

	if d.Postgres != nil {
		d.Postgres.Close()
		logger.Info("postgres connection pool closed")
	}
}

func postgresDSN(cfg config.PostgresConfig) string {
	values := url.Values{}
	values.Set("sslmode", cfg.SSLMode)

	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     cfg.Host + ":" + cfg.Port,
		Path:     cfg.Database,
		RawQuery: values.Encode(),
	}

	return dsn.String()
}
