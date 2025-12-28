#!/bin/bash
# Initialize checkpoint tables manually (optional - they're created automatically on first run)

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

cd "$PROJECT_ROOT"

PROPERTIES_FILE="${PROPERTIES_FILE:-src/main/resources/migration.properties}"

YUGABYTE_HOST=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | cut -d',' -f1)
YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_DB=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_USER=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_PASS=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2)

echo "Initializing checkpoint tables..."

PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
-- Create run_info table (run-level metadata)
CREATE TABLE IF NOT EXISTS public.migration_run_info (
    table_name      TEXT,
    run_id          BIGINT,
    run_type        TEXT,
    prev_run_id     BIGINT,
    start_time      TIMESTAMPTZ DEFAULT now(),
    end_time        TIMESTAMPTZ,
    run_info        TEXT,
    status          TEXT,
    PRIMARY KEY (table_name, run_id)
);

-- Create run_details table (partition/token-range level tracking)
CREATE TABLE IF NOT EXISTS public.migration_run_details (
    table_name      TEXT,
    run_id          BIGINT,
    start_time      TIMESTAMPTZ DEFAULT now(),
    token_min       BIGINT,
    token_max       BIGINT,
    partition_id    INT,
    status          TEXT,
    run_info        TEXT,
    PRIMARY KEY ((table_name, run_id), token_min, partition_id)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_run_details_status ON public.migration_run_details (table_name, run_id, status);
CREATE INDEX IF NOT EXISTS idx_run_info_status ON public.migration_run_info (table_name, status);

SELECT 'Checkpoint tables initialized successfully' as status;
EOF

echo "✅ Checkpoint tables initialized"

