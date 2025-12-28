#!/bin/bash
# Test script for checkpoint/resume functionality
# This script demonstrates stopping a migration mid-run and resuming from checkpoint

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

cd "$PROJECT_ROOT"

# Load configuration
PROPERTIES_FILE="${PROPERTIES_FILE:-src/main/resources/migration.properties}"
SPARK_HOME="${SPARK_HOME:-$HOME/spark-3.5.1}"

if [ ! -d "$SPARK_HOME" ]; then
    echo "ERROR: SPARK_HOME not set or Spark not found at $SPARK_HOME"
    echo "Please set SPARK_HOME environment variable"
    exit 1
fi

# Extract table info from properties
TARGET_SCHEMA=$(grep "^table.target.schema=" "$PROPERTIES_FILE" | cut -d'=' -f2)
TARGET_TABLE=$(grep "^table.target.table=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_HOST=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | cut -d',' -f1)
YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_DB=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_USER=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_PASS=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2)

echo "=========================================="
echo "Checkpoint Test Script"
echo "=========================================="
echo "Target Table: $TARGET_SCHEMA.$TARGET_TABLE"
echo "YugabyteDB: $YUGABYTE_HOST:$YUGABYTE_PORT/$YUGABYTE_DB"
echo ""

# Step 1: Truncate target table
echo "Step 1: Truncating target table..."
PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
TRUNCATE TABLE $TARGET_SCHEMA.$TARGET_TABLE;
SELECT 'Table truncated successfully' as status;
EOF

if [ $? -eq 0 ]; then
    echo "✅ Table truncated successfully"
else
    echo "⚠️  Warning: Could not truncate table (may not exist or may be empty)"
fi

echo ""

# Step 2: Initialize checkpoint tables (if they don't exist)
echo "Step 2: Initializing checkpoint tables..."
if PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -c "\d public.migration_run_info" &>/dev/null; then
    echo "✅ Checkpoint tables already exist"
else
    echo "Creating checkpoint tables..."
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
EOF
    echo "✅ Checkpoint tables created"
fi

# Show recent runs (if any)
echo ""
echo "Recent runs:"
PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF 2>/dev/null || echo "No runs yet"
SELECT 
    table_name,
    run_id,
    prev_run_id,
    status,
    start_time,
    end_time
FROM public.migration_run_info
ORDER BY run_id DESC
LIMIT 5;
EOF

echo ""

# Step 3: Start migration (will be killed manually)
echo "=========================================="
echo "Step 3: Starting migration..."
echo "=========================================="
echo "⚠️  IMPORTANT: This migration will run until you manually stop it (Ctrl+C or kill)"
echo "   After stopping, use the resume script to continue from checkpoint"
echo ""
echo "Starting migration in 5 seconds..."
sleep 5

# Generate a unique run ID
RUN_ID=$(date +%s)
echo "Run ID: $RUN_ID"

# Create a temporary properties file with the run ID
TEMP_PROPERTIES=$(mktemp)
cp "$PROPERTIES_FILE" "$TEMP_PROPERTIES"
echo "migration.runId=$RUN_ID" >> "$TEMP_PROPERTIES"
echo "migration.prevRunId=0" >> "$TEMP_PROPERTIES"

# Run migration
JAR_FILE="$PROJECT_ROOT/target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar"
LOG_FILE="$PROJECT_ROOT/checkpoint_test_run_${RUN_ID}.log"

echo "Running migration..."
echo "Log file: $LOG_FILE"
echo "To stop: Press Ctrl+C or run: pkill -f 'Cassandra-to-YugabyteDB Migration'"
echo ""

"$SPARK_HOME/bin/spark-submit" \
    --class com.company.migration.MainApp \
    --master "local[*]" \
    --driver-memory 4g \
    --executor-memory 4g \
    --properties-file "$TEMP_PROPERTIES" \
    "$JAR_FILE" \
    "$TEMP_PROPERTIES" 2>&1 | tee "$LOG_FILE"

EXIT_CODE=$?

# Cleanup temp file
rm -f "$TEMP_PROPERTIES"

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ Migration completed successfully!"
else
    echo ""
    echo "⚠️  Migration stopped (exit code: $EXIT_CODE)"
    echo "   This is expected if you stopped it manually"
    echo ""
    echo "To resume, run:"
    echo "  ./scripts/resume_checkpoint.sh $RUN_ID"
fi

echo ""
echo "Checkpoint status:"
PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    table_name,
    run_id,
    status,
    COUNT(*) FILTER (WHERE status = 'PASS') as completed,
    COUNT(*) FILTER (WHERE status IN ('NOT_STARTED', 'STARTED', 'FAIL')) as pending,
    COUNT(*) as total
FROM public.migration_run_details
WHERE run_id = $RUN_ID
GROUP BY table_name, run_id, status
ORDER BY status;
EOF

