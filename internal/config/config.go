package config

import "os"

type Config struct {
	HTTP       HTTPConfig
	Redis      RedisConfig
	Postgres   PostgresConfig
	Prometheus PrometheusConfig
	Scheduler  SchedulerConfig
}

type HTTPConfig struct {
	Addr string
}

type RedisConfig struct {
	Addr          string
	StreamName    string
	ConsumerGroup string
	ConsumerName  string
}

type PostgresConfig struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
	SSLMode  string
}

type PrometheusConfig struct {
	URL string
}

type SchedulerConfig struct {
	IntervalSeconds int
	BatchSize       int
}

func Load() Config {
	return Config{
		HTTP: HTTPConfig{
			Addr: env("HTTP_ADDR", ":8080"),
		},
		Redis: RedisConfig{
			Addr:          env("REDIS_ADDR", "localhost:6380"),
			StreamName:    env("REDIS_STREAM_NAME", "tasks:stream"),
			ConsumerGroup: env("REDIS_CONSUMER_GROUP", "gotaskqueue-workers"),
			ConsumerName:  env("REDIS_CONSUMER_NAME", "gotaskqueue-worker-1"),
		},
		Postgres: PostgresConfig{
			Host:     env("POSTGRES_HOST", "localhost"),
			Port:     env("POSTGRES_PORT", "5432"),
			Database: env("POSTGRES_DB", "gotaskqueue"),
			User:     env("POSTGRES_USER", "gotaskqueue"),
			Password: env("POSTGRES_PASSWORD", "gotaskqueue"),
			SSLMode:  env("POSTGRES_SSLMODE", "disable"),
		},
		Prometheus: PrometheusConfig{
			URL: env("PROMETHEUS_URL", "http://localhost:9090"),
		},
		Scheduler: SchedulerConfig{
			IntervalSeconds: envInt("SCHEDULER_INTERVAL_SECONDS", 2),
			BatchSize:       envInt("SCHEDULER_BATCH_SIZE", 100),
		},
	}
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	var parsed int
	for _, char := range value {
		if char < '0' || char > '9' {
			return fallback
		}
		parsed = parsed*10 + int(char-'0')
	}
	if parsed <= 0 {
		return fallback
	}
	return parsed
}
