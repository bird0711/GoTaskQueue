CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    idempotency_key TEXT,
    run_at TIMESTAMPTZ NOT NULL,
    timeout_seconds INTEGER NOT NULL DEFAULT 300,
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    last_error TEXT,
    worker_id TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT tasks_status_check CHECK (
        status IN ('scheduled', 'pending', 'running', 'success', 'failed', 'retrying', 'dead')
    ),
    CONSTRAINT tasks_timeout_seconds_check CHECK (timeout_seconds > 0),
    CONSTRAINT tasks_max_retries_check CHECK (max_retries >= 0),
    CONSTRAINT tasks_retry_count_check CHECK (retry_count >= 0 AND retry_count <= max_retries)
);

CREATE UNIQUE INDEX IF NOT EXISTS tasks_idempotency_key_unique
    ON tasks (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status);
CREATE INDEX IF NOT EXISTS tasks_run_at_idx ON tasks (run_at);
CREATE INDEX IF NOT EXISTS tasks_created_at_idx ON tasks (created_at);
CREATE INDEX IF NOT EXISTS tasks_next_retry_at_idx ON tasks (next_retry_at)
    WHERE next_retry_at IS NOT NULL;
