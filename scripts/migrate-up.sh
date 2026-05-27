#!/usr/bin/env bash
set -euo pipefail

compose="${COMPOSE:-docker compose}"
postgres_service="${POSTGRES_SERVICE:-postgres}"
postgres_db="${POSTGRES_DB:-gotaskqueue}"
postgres_user="${POSTGRES_USER:-gotaskqueue}"
migrations_dir="${MIGRATIONS_DIR:-migrations}"

if [[ ! -d "$migrations_dir" ]]; then
  echo "migrations directory not found: $migrations_dir" >&2
  exit 1
fi

psql_cmd=(
  $compose
  exec
  -T
  "$postgres_service"
  psql
  -U "$postgres_user"
  -d "$postgres_db"
  -v ON_ERROR_STOP=1
)

"${psql_cmd[@]}" <<'SQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
SQL

shopt -s nullglob
migration_files=("$migrations_dir"/*.up.sql)
if (( ${#migration_files[@]} == 0 )); then
  echo "no up migrations found in $migrations_dir"
  exit 0
fi

for migration_file in "${migration_files[@]}"; do
  filename="$(basename "$migration_file")"
  version="${filename%.up.sql}"
  if [[ ! "$version" =~ ^[A-Za-z0-9_-]+$ ]]; then
    echo "invalid migration version: $version" >&2
    exit 1
  fi

  applied="$("${psql_cmd[@]}" -tAc "SELECT 1 FROM schema_migrations WHERE version = '$version';" | tr -d '[:space:]')"
  if [[ "$applied" == "1" ]]; then
    echo "skip migration $version"
    continue
  fi

  echo "apply migration $version"
  temp_file="$(mktemp)"
  {
    echo "BEGIN;"
    echo "SELECT pg_advisory_xact_lock(hashtext('gotaskqueue_schema_migrations'));"
    echo "SELECT CASE WHEN EXISTS (SELECT 1 FROM schema_migrations WHERE version = '$version') THEN 'true' ELSE 'false' END AS already_applied"
    echo "\\gset"
    echo "\\if :already_applied"
    echo "\\echo skip migration $version"
    echo "\\else"
    cat "$migration_file"
    echo "INSERT INTO schema_migrations (version) VALUES ('$version') ON CONFLICT (version) DO NOTHING;"
    echo "\\endif"
    echo "COMMIT;"
  } > "$temp_file"

  if ! "${psql_cmd[@]}" < "$temp_file"; then
    rm -f "$temp_file"
    exit 1
  fi
  rm -f "$temp_file"
done
