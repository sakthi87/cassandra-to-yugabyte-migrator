#!/bin/bash
# Resume migration from a previous run using checkpoint

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

# Get previous run ID from argument
if [ -z "$1" ]; then
    echo "Usage: $0 <previous_run_id>"
    echo ""
    echo "To find the previous run ID, check the checkpoint tables:"
    echo "  psql -h <host> -p <port> -U <user> -d <database> -c \"SELECT run_id, status FROM public.migration_run_info ORDER BY run_id DESC LIMIT 5;\""
    exit 1
fi

PREV_RUN_ID=$1

# Generate new run ID
NEW_RUN_ID=$(date +%s)

echo "=========================================="
echo "Resume Migration from Checkpoint"
echo "=========================================="
echo "Previous Run ID: $PREV_RUN_ID"
echo "New Run ID: $NEW_RUN_ID"
echo ""

# Check if previous run exists
YUGABYTE_HOST=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | cut -d',' -f1)
YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_DB=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_USER=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_PASS=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2)

echo "Checking previous run status..."
PREV_RUN_STATUS=$(PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT status FROM public.migration_run_info WHERE run_id = $PREV_RUN_ID;" | xargs)

if [ -z "$PREV_RUN_STATUS" ]; then
    echo "❌ ERROR: Previous run $PREV_RUN_ID not found!"
    exit 1
fi

echo "Previous run status: $PREV_RUN_STATUS"
echo ""

# Check pending partitions
echo "Pending partitions from previous run:"
PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    status,
    COUNT(*) as count
FROM public.migration_run_details
WHERE run_id = $PREV_RUN_ID
GROUP BY status
ORDER BY status;
EOF

PENDING_COUNT=$(PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT COUNT(*) FROM public.migration_run_details WHERE run_id = $PREV_RUN_ID AND status IN ('NOT_STARTED', 'STARTED', 'FAIL');" | xargs)

if [ "$PENDING_COUNT" -eq 0 ]; then
    echo ""
    echo "⚠️  WARNING: No pending partitions found. Previous run may have completed."
    echo "   Do you want to continue anyway? (y/n)"
    read -r response
    if [ "$response" != "y" ]; then
        exit 0
    fi
fi

echo ""
echo "Starting resume migration in 3 seconds..."
sleep 3

# Create a temporary properties file with the new run ID and previous run ID
TEMP_PROPERTIES=$(mktemp)
cp "$PROPERTIES_FILE" "$TEMP_PROPERTIES"
echo "migration.runId=$NEW_RUN_ID" >> "$TEMP_PROPERTIES"
echo "migration.prevRunId=$PREV_RUN_ID" >> "$TEMP_PROPERTIES"

# Run migration
JAR_FILE="$PROJECT_ROOT/target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar"
LOG_FILE="$PROJECT_ROOT/checkpoint_resume_${NEW_RUN_ID}.log"

echo "Running resume migration..."
echo "Log file: $LOG_FILE"
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
    echo "✅ Resume migration completed successfully!"
else
    echo ""
    echo "❌ Resume migration failed (exit code: $EXIT_CODE)"
    exit $EXIT_CODE
fi

echo ""
echo "Final checkpoint status:"
PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    table_name,
    run_id,
    status,
    COUNT(*) FILTER (WHERE status = 'PASS') as completed,
    COUNT(*) FILTER (WHERE status IN ('NOT_STARTED', 'STARTED', 'FAIL')) as pending,
    COUNT(*) as total
FROM public.migration_run_details
WHERE run_id = $NEW_RUN_ID
GROUP BY table_name, run_id, status
ORDER BY status;
EOF

