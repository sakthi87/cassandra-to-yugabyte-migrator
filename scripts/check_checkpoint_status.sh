#!/bin/bash
# Check checkpoint status for a run

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

RUN_ID="${1:-}"

echo "=========================================="
echo "Checkpoint Status"
echo "=========================================="

if [ -n "$RUN_ID" ]; then
    echo "Run ID: $RUN_ID"
    echo ""
    
    # Show run info
    echo "Run Information:"
    PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    table_name,
    run_id,
    prev_run_id,
    run_type,
    status,
    start_time,
    end_time,
    run_info
FROM public.migration_run_info
WHERE run_id = $RUN_ID;
EOF
    
    echo ""
    echo "Partition Status:"
    PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    status,
    COUNT(*) as count,
    MIN(start_time) as first_start,
    MAX(start_time) as last_start
FROM public.migration_run_details
WHERE run_id = $RUN_ID
GROUP BY status
ORDER BY status;
EOF
    
    echo ""
    echo "Pending Partitions (can be resumed):"
    PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    partition_id,
    token_min,
    token_max,
    status,
    run_info
FROM public.migration_run_details
WHERE run_id = $RUN_ID 
  AND status IN ('NOT_STARTED', 'STARTED', 'FAIL')
ORDER BY partition_id
LIMIT 20;
EOF
else
    echo "Recent Runs:"
    PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    table_name,
    run_id,
    prev_run_id,
    status,
    start_time,
    end_time
FROM public.migration_run_info
ORDER BY run_id DESC
LIMIT 10;
EOF
    
    echo ""
    echo "Usage: $0 <run_id>"
    echo "   Example: $0 1703456789"
fi

