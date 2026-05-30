package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestDashboardQueryIndexesMigration(t *testing.T) {
	body, err := os.ReadFile("000003_add_dashboard_query_indexes.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	migration := string(body)
	expected := []string{
		"CREATE INDEX IF NOT EXISTS tasks_status_updated_created_idx",
		"ON tasks (status, updated_at DESC, created_at DESC)",
		"CREATE INDEX IF NOT EXISTS tasks_updated_created_idx",
		"ON tasks (updated_at DESC, created_at DESC)",
	}
	for _, fragment := range expected {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("expected migration to contain %q, got:\n%s", fragment, migration)
		}
	}
}
