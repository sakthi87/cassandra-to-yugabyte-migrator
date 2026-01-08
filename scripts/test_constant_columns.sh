#!/bin/bash

# Test script for Constant Columns feature
# This script tests that constant columns are populated correctly

set -e

PROPERTIES_FILE="${1:-src/main/resources/migration.properties}"
YUGABYTE_HOST="${2:-localhost}"
YUGABYTE_PORT="${3:-5433}"
YUGABYTE_DB="${4:-transaction_datastore}"
YUGABYTE_USER="${5:-yugabyte}"

echo "=========================================="
echo "Constant Columns Feature Test"
echo "=========================================="
echo "Properties file: $PROPERTIES_FILE"
echo "YugabyteDB: $YUGABYTE_HOST:$YUGABYTE_PORT/$YUGABYTE_DB"
echo ""

# Read table configuration from properties
TARGET_SCHEMA=$(grep "^table.target.schema=" "$PROPERTIES_FILE" | cut -d'=' -f2 || echo "public")
TARGET_TABLE=$(grep "^table.target.table=" "$PROPERTIES_FILE" | cut -d'=' -f2 || echo "")

if [ -z "$TARGET_TABLE" ]; then
  echo "❌ ERROR: table.target.table not found in properties file"
  exit 1
fi

FULL_TABLE_NAME="${TARGET_SCHEMA}.${TARGET_TABLE}"

echo "Target table: $FULL_TABLE_NAME"
echo ""

# Step 1: Check if constant columns are configured
CONSTANT_NAMES=$(grep "^table.constantColumns.names=" "$PROPERTIES_FILE" | cut -d'=' -f2 || echo "")
CONSTANT_VALUES=$(grep "^table.constantColumns.values=" "$PROPERTIES_FILE" | cut -d'=' -f2 || echo "")

if [ -z "$CONSTANT_NAMES" ] || [ -z "$CONSTANT_VALUES" ]; then
  echo "⚠️  WARNING: Constant columns not configured in properties file"
  echo "   Add the following to enable constant columns:"
  echo "   table.constantColumns.names=created_by,migration_date"
  echo "   table.constantColumns.values=CDM_MIGRATION,2024-12-16"
  echo ""
  echo "Proceeding with test anyway (will verify table structure)..."
fi

# Step 2: Truncate the target table
echo "Step 1: Truncating target table..."
PGPASSWORD=yugabyte psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -c "TRUNCATE TABLE $FULL_TABLE_NAME;" 2>&1
if [ $? -eq 0 ]; then
  echo "✅ Table truncated successfully"
else
  echo "❌ ERROR: Failed to truncate table"
  exit 1
fi
echo ""

# Step 3: Check table structure (to see if constant columns exist)
echo "Step 2: Checking table structure..."
PGPASSWORD=yugabyte psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -c "\d $FULL_TABLE_NAME" 2>&1 | head -30
echo ""

# Step 4: Run the migration
echo "Step 3: Running migration with constant columns..."
echo "⚠️  IMPORTANT: This will start the migration. Press Ctrl+C to stop if needed."
echo "   Waiting 5 seconds before starting..."
sleep 5

# Get Spark home
SPARK_HOME="${SPARK_HOME:-$(brew --prefix apache-spark 2>/dev/null || echo "")}"
if [ -z "$SPARK_HOME" ] || [ ! -d "$SPARK_HOME" ]; then
  echo "❌ ERROR: SPARK_HOME not set or invalid"
  echo "   Please set SPARK_HOME environment variable or install Spark via Homebrew"
  exit 1
fi

# Run migration
cd "$(dirname "$0")/.."
$SPARK_HOME/bin/spark-submit \
  --class com.company.migration.MainApp \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  --properties-file "$PROPERTIES_FILE" \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  "$PROPERTIES_FILE" 2>&1 | tee migration_test.log

MIGRATION_EXIT_CODE=${PIPESTATUS[0]}

if [ $MIGRATION_EXIT_CODE -eq 0 ]; then
  echo ""
  echo "✅ Migration completed successfully"
else
  echo ""
  echo "❌ ERROR: Migration failed with exit code $MIGRATION_EXIT_CODE"
  echo "   Check migration_test.log for details"
  exit 1
fi
echo ""

# Step 5: Verify results
echo "Step 4: Verifying constant columns were populated..."

if [ -n "$CONSTANT_NAMES" ]; then
  # Parse constant column names
  IFS=',' read -ra COL_NAMES <<< "$CONSTANT_NAMES"
  
  echo "Checking constant columns: ${COL_NAMES[@]}"
  echo ""
  
  # Check a few sample rows
  for col_name in "${COL_NAMES[@]}"; do
    col_name_trimmed=$(echo "$col_name" | xargs)
    echo "Checking column: $col_name_trimmed"
    
    # Get distinct values (should be constant)
    DISTINCT_COUNT=$(PGPASSWORD=yugabyte psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT COUNT(DISTINCT $col_name_trimmed) FROM $FULL_TABLE_NAME;" 2>&1 | xargs)
    
    if [ "$DISTINCT_COUNT" = "1" ] || [ "$DISTINCT_COUNT" = "0" ]; then
      echo "  ✅ Column $col_name_trimmed: All rows have the same value (or table is empty)"
      
      # Show the value if table has data
      if [ "$DISTINCT_COUNT" = "1" ]; then
        VALUE=$(PGPASSWORD=yugabyte psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT $col_name_trimmed FROM $FULL_TABLE_NAME LIMIT 1;" 2>&1 | xargs)
        echo "     Value: $VALUE"
      fi
    else
      echo "  ⚠️  Column $col_name_trimmed: Found $DISTINCT_COUNT distinct values (expected 1)"
    fi
    echo ""
  done
else
  echo "⚠️  Constant columns not configured, skipping verification"
fi

# Step 6: Show row count and sample data
echo "Step 5: Checking migration results..."
ROW_COUNT=$(PGPASSWORD=yugabyte psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT COUNT(*) FROM $FULL_TABLE_NAME;" 2>&1 | xargs)
echo "Total rows migrated: $ROW_COUNT"
echo ""

if [ "$ROW_COUNT" -gt 0 ]; then
  echo "Sample data (first 3 rows):"
  PGPASSWORD=yugabyte psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -c "SELECT * FROM $FULL_TABLE_NAME LIMIT 3;" 2>&1
else
  echo "⚠️  No rows found in table"
fi

echo ""
echo "=========================================="
echo "Test Complete"
echo "=========================================="

