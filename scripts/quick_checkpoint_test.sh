#!/bin/bash
# Quick checkpoint test - simplified version

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

cd "$PROJECT_ROOT"

PROPERTIES_FILE="${PROPERTIES_FILE:-src/main/resources/migration.properties}"
SPARK_HOME="${SPARK_HOME:-$HOME/spark-3.5.1}"

if [ ! -d "$SPARK_HOME" ]; then
    echo "ERROR: SPARK_HOME not set or Spark not found at $SPARK_HOME"
    exit 1
fi

# Extract config
TARGET_SCHEMA=$(grep "^table.target.schema=" "$PROPERTIES_FILE" | cut -d'=' -f2)
TARGET_TABLE=$(grep "^table.target.table=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_HOST=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | cut -d',' -f1)
YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_DB=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_USER=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_PASS=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2)

echo "=========================================="
echo "Quick Checkpoint Test"
echo "=========================================="
echo ""

# Step 1: Truncate table
echo "Step 1: Truncating target table..."
PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -c "TRUNCATE TABLE $TARGET_SCHEMA.$TARGET_TABLE;" 2>/dev/null || echo "⚠️  Table may not exist (will be created)"
echo "✅ Table truncated"
echo ""

# Step 2: Start migration with run ID
RUN_ID=$(date +%s)
echo "Step 2: Starting migration (Run ID: $RUN_ID)"
echo "⚠️  Let it run for 10-20 seconds, then press Ctrl+C to stop it"
echo ""

TEMP_PROPERTIES=$(mktemp)
cp "$PROPERTIES_FILE" "$TEMP_PROPERTIES"
echo "migration.runId=$RUN_ID" >> "$TEMP_PROPERTIES"
echo "migration.prevRunId=0" >> "$TEMP_PROPERTIES"

JAR_FILE="$PROJECT_ROOT/target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar"

# Run migration (will be stopped manually after ~20 seconds)
# Note: timeout may not work on macOS, so user should manually stop with Ctrl+C
"$SPARK_HOME/bin/spark-submit" \
    --class com.company.migration.MainApp \
    --master "local[*]" \
    --driver-memory 4g \
    --executor-memory 4g \
    --properties-file "$TEMP_PROPERTIES" \
    "$JAR_FILE" \
    "$TEMP_PROPERTIES" 2>&1 || true

rm -f "$TEMP_PROPERTIES"

echo ""
echo "Step 3: Checking checkpoint status..."
PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    status,
    COUNT(*) as count
FROM public.migration_run_details
WHERE run_id = $RUN_ID
GROUP BY status;
EOF

echo ""
echo "Step 4: Resuming migration..."
NEW_RUN_ID=$(date +%s)
echo "New Run ID: $NEW_RUN_ID (resuming from $RUN_ID)"
echo ""

TEMP_PROPERTIES2=$(mktemp)
cp "$PROPERTIES_FILE" "$TEMP_PROPERTIES2"
echo "migration.runId=$NEW_RUN_ID" >> "$TEMP_PROPERTIES2"
echo "migration.prevRunId=$RUN_ID" >> "$TEMP_PROPERTIES2"

"$SPARK_HOME/bin/spark-submit" \
    --class com.company.migration.MainApp \
    --master "local[*]" \
    --driver-memory 4g \
    --executor-memory 4g \
    --properties-file "$TEMP_PROPERTIES2" \
    "$JAR_FILE" \
    "$TEMP_PROPERTIES2" 2>&1

rm -f "$TEMP_PROPERTIES2"

echo ""
echo "Step 5: Final status..."
PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
SELECT 
    run_id,
    status,
    COUNT(*) as partitions
FROM public.migration_run_details
WHERE run_id = $NEW_RUN_ID
GROUP BY run_id, status;
EOF

echo ""
echo "✅ Checkpoint test completed!"

