CREATE INDEX IF NOT EXISTS tasks_status_updated_created_idx
    ON tasks (status, updated_at DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS tasks_updated_created_idx
    ON tasks (updated_at DESC, created_at DESC);
