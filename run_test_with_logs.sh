#!/bin/bash
# Test migration and save logs to project directory

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/migration_test_${TIMESTAMP}.log"

echo "=========================================="
echo "Migration Test with Logging Verification"
echo "=========================================="
echo "Log file: $LOG_FILE"
echo ""

# Check Spark
if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME=$HOME/spark-3.5.1
fi

if [ ! -d "$SPARK_HOME" ]; then
    echo "ERROR: Spark not found at $SPARK_HOME"
    echo "Please set SPARK_HOME or install Spark 3.5.1"
    exit 1
fi

echo "Using Spark: $SPARK_HOME"
echo ""

# Run migration and capture all output
echo "Starting migration..."
echo "This will test:"
echo "  1. No generated code in logs"
echo "  2. SELECT COUNT(*) queries (validation)"
echo ""

"$SPARK_HOME/bin/spark-submit" \
  --class com.company.migration.MainApp \
  --master 'local[4]' \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --conf spark.default.parallelism=16 \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j2.properties" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j2.properties" \
  --jars target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties \
  2>&1 | tee "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "=========================================="
echo "Migration Completed"
echo "=========================================="
echo "Exit Code: $EXIT_CODE"
echo "Log File: $LOG_FILE"
echo ""

# Analyze logs
echo "=========================================="
echo "Log Analysis"
echo "=========================================="

echo ""
echo "1. Checking for generated code (should be minimal or none):"
GENERATED_CODE=$(grep -i "SpecificUnsafeProjection\|class.*extends.*UnsafeProjection\|public.*apply.*InternalRow" "$LOG_FILE" | wc -l | tr -d ' ')
if [ "$GENERATED_CODE" -eq 0 ]; then
    echo "   ✅ No generated code found in logs"
else
    echo "   ⚠️  Found $GENERATED_CODE instances of generated code"
    echo "   First few instances:"
    grep -i "SpecificUnsafeProjection\|class.*extends.*UnsafeProjection" "$LOG_FILE" | head -3 | sed 's/^/      /'
fi

echo ""
echo "2. Checking for SELECT COUNT(*) queries:"
SELECT_COUNT=$(grep -i "SELECT COUNT(\*)" "$LOG_FILE" | wc -l | tr -d ' ')
if [ "$SELECT_COUNT" -gt 0 ]; then
    echo "   Found $SELECT_COUNT SELECT COUNT(*) queries"
    echo "   These are from validation (expected behavior):"
    grep -i "SELECT COUNT(\*)" "$LOG_FILE" | head -3 | sed 's/^/      /'
    echo ""
    echo "   Context (why they appear):"
    grep -B2 -A2 "SELECT COUNT(\*)" "$LOG_FILE" | grep -E "(Validating|Row count validation|Running validation)" | head -1 | sed 's/^/      /'
else
    echo "   ✅ No SELECT COUNT(*) queries found"
fi

echo ""
echo "3. Checking for CodeGenerator messages (should be minimal):"
CODEGEN=$(grep -i "CodeGenerator.*Code generated" "$LOG_FILE" | wc -l | tr -d ' ')
if [ "$CODEGEN" -gt 0 ]; then
    echo "   Found $CODEGEN CodeGenerator messages (this is normal, just timing info)"
    echo "   These are INFO level, not the actual generated code"
else
    echo "   ✅ No CodeGenerator messages found"
fi

echo ""
echo "4. Log verbosity summary:"
TOTAL_LINES=$(wc -l < "$LOG_FILE" | tr -d ' ')
DEBUG_LINES=$(grep -i "DEBUG" "$LOG_FILE" | wc -l | tr -d ' ')
INFO_LINES=$(grep -i "INFO" "$LOG_FILE" | wc -l | tr -d ' ')
WARN_LINES=$(grep -i "WARN" "$LOG_FILE" | wc -l | tr -d ' ')
ERROR_LINES=$(grep -i "ERROR" "$LOG_FILE" | wc -l | tr -d ' ')

echo "   Total lines: $TOTAL_LINES"
echo "   DEBUG: $DEBUG_LINES"
echo "   INFO: $INFO_LINES"
echo "   WARN: $WARN_LINES"
echo "   ERROR: $ERROR_LINES"

echo ""
echo "=========================================="
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "✅ Migration test completed successfully"
else
    echo "❌ Migration test failed with exit code: $EXIT_CODE"
fi
echo "=========================================="
echo ""
echo "Full log: $LOG_FILE"
echo ""

