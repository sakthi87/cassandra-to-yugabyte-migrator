#!/bin/bash

# Test script for Constant Columns feature
# Tests the feature with local YugabyteDB database

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROPERTIES_FILE="$PROJECT_DIR/src/main/resources/migration.properties"

YUGABYTE_HOST="localhost"
YUGABYTE_PORT="5433"
YUGABYTE_DB="transaction_datastore"
YUGABYTE_USER="yugabyte"
YUGABYTE_PASSWORD="yugabyte"

TARGET_SCHEMA="public"
TARGET_TABLE="dda_pstd_fincl_txn_cnsmr_by_accntnbr"
FULL_TABLE_NAME="${TARGET_SCHEMA}.${TARGET_TABLE}"

echo "=========================================="
echo "Constant Columns Feature Test"
echo "=========================================="
echo "Properties file: $PROPERTIES_FILE"
echo "YugabyteDB: $YUGABYTE_HOST:$YUGABYTE_PORT/$YUGABYTE_DB"
echo "Target table: $FULL_TABLE_NAME"
echo ""

# Step 1: Check if YugabyteDB is running
echo "Step 1: Checking YugabyteDB connection..."
if ! PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -c "SELECT 1;" > /dev/null 2>&1; then
  echo "❌ ERROR: Cannot connect to YugabyteDB"
  echo "   Please ensure YugabyteDB is running and accessible"
  exit 1
fi
echo "✅ Connected to YugabyteDB"
echo ""

# Step 2: Add constant columns to the table (if they don't exist)
echo "Step 2: Setting up constant columns in target table..."
PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
ALTER TABLE $FULL_TABLE_NAME 
ADD COLUMN IF NOT EXISTS created_by TEXT;

ALTER TABLE $FULL_TABLE_NAME 
ADD COLUMN IF NOT EXISTS migration_date DATE;

ALTER TABLE $FULL_TABLE_NAME 
ADD COLUMN IF NOT EXISTS source_system TEXT;

ALTER TABLE $FULL_TABLE_NAME 
ADD COLUMN IF NOT EXISTS migration_run_id BIGINT;
EOF

if [ $? -eq 0 ]; then
  echo "✅ Constant columns added/verified"
else
  echo "❌ ERROR: Failed to add constant columns"
  exit 1
fi
echo ""

# Step 3: Truncate the target table
echo "Step 3: Truncating target table..."
PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -c "TRUNCATE TABLE $FULL_TABLE_NAME;" 2>&1
if [ $? -eq 0 ]; then
  echo "✅ Table truncated successfully"
else
  echo "❌ ERROR: Failed to truncate table"
  exit 1
fi
echo ""

# Step 4: Update properties file with constant columns configuration
echo "Step 4: Configuring constant columns in properties file..."
# Backup the properties file
cp "$PROPERTIES_FILE" "$PROPERTIES_FILE.backup"

# Check if constant columns are already configured
if grep -q "^table.constantColumns.names=" "$PROPERTIES_FILE"; then
  echo "⚠️  Constant columns already configured in properties file"
  echo "   Current configuration:"
  grep "^table.constantColumns" "$PROPERTIES_FILE" || true
else
  echo "   Adding constant columns configuration..."
  # Add constant columns configuration before the Table Configuration comment or at the end
  if grep -q "^# Table Configuration" "$PROPERTIES_FILE"; then
    # Add before Table Configuration section
    sed -i.bak '/^# Table Configuration/i\
# Constant Columns (Default Values for Target Columns)\
table.constantColumns.names=created_by,migration_date,source_system,migration_run_id\
table.constantColumns.values=CDM_MIGRATION,2024-12-16,CASSANDRA_PROD,1702732800000\
' "$PROPERTIES_FILE"
  else
    # Append at the end
    cat >> "$PROPERTIES_FILE" <<EOF

# Constant Columns (Default Values for Target Columns)
table.constantColumns.names=created_by,migration_date,source_system,migration_run_id
table.constantColumns.values=CDM_MIGRATION,2024-12-16,CASSANDRA_PROD,1702732800000
EOF
  fi
  echo "✅ Constant columns configuration added"
fi
echo ""

# Step 5: Verify constant columns configuration
echo "Step 5: Verifying constant columns configuration..."
CONSTANT_NAMES=$(grep "^table.constantColumns.names=" "$PROPERTIES_FILE" | cut -d'=' -f2 || echo "")
CONSTANT_VALUES=$(grep "^table.constantColumns.values=" "$PROPERTIES_FILE" | cut -d'=' -f2 || echo "")

if [ -z "$CONSTANT_NAMES" ] || [ -z "$CONSTANT_VALUES" ]; then
  echo "❌ ERROR: Constant columns not properly configured"
  exit 1
fi

echo "   Constant column names: $CONSTANT_NAMES"
echo "   Constant column values: $CONSTANT_VALUES"
echo ""

# Step 6: Run the migration
echo "Step 6: Running migration with constant columns..."
echo "⚠️  This will start the migration. Press Ctrl+C to stop if needed."
echo "   Waiting 3 seconds before starting..."
sleep 3

# Get Spark home
SPARK_HOME="${SPARK_HOME:-$(brew --prefix apache-spark 2>/dev/null || echo "")}"
if [ -z "$SPARK_HOME" ] || [ ! -d "$SPARK_HOME" ]; then
  echo "❌ ERROR: SPARK_HOME not set or invalid"
  echo "   Please set SPARK_HOME environment variable or install Spark via Homebrew"
  exit 1
fi

echo "   Using Spark at: $SPARK_HOME"
echo ""

# Run migration
cd "$PROJECT_DIR"
$SPARK_HOME/bin/spark-submit \
  --class com.company.migration.MainApp \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  --properties-file "$PROPERTIES_FILE" \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  "$PROPERTIES_FILE" 2>&1 | tee "$PROJECT_DIR/migration_constant_columns_test.log"

MIGRATION_EXIT_CODE=${PIPESTATUS[0]}

if [ $MIGRATION_EXIT_CODE -eq 0 ]; then
  echo ""
  echo "✅ Migration completed successfully"
else
  echo ""
  echo "❌ ERROR: Migration failed with exit code $MIGRATION_EXIT_CODE"
  echo "   Check migration_constant_columns_test.log for details"
  # Restore backup
  mv "$PROPERTIES_FILE.backup" "$PROPERTIES_FILE"
  exit 1
fi
echo ""

# Step 7: Verify constant columns were populated
echo "Step 7: Verifying constant columns were populated..."
ROW_COUNT=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT COUNT(*) FROM $FULL_TABLE_NAME;" 2>&1 | xargs)
echo "Total rows migrated: $ROW_COUNT"
echo ""

if [ "$ROW_COUNT" -gt 0 ]; then
  echo "Checking constant columns..."
  
  # Check created_by (should all be 'CDM_MIGRATION')
  CREATED_BY_COUNT=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT COUNT(DISTINCT created_by) FROM $FULL_TABLE_NAME WHERE created_by IS NOT NULL;" 2>&1 | xargs)
  CREATED_BY_VALUE=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT created_by FROM $FULL_TABLE_NAME WHERE created_by IS NOT NULL LIMIT 1;" 2>&1 | xargs)
  
  if [ "$CREATED_BY_COUNT" = "1" ] && [ "$CREATED_BY_VALUE" = "CDM_MIGRATION" ]; then
    echo "  ✅ created_by: All rows have value '$CREATED_BY_VALUE'"
  else
    echo "  ⚠️  created_by: Found $CREATED_BY_COUNT distinct values (expected 1), value: '$CREATED_BY_VALUE'"
  fi
  
  # Check migration_date (should all be '2024-12-16')
  MIGRATION_DATE_COUNT=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT COUNT(DISTINCT migration_date) FROM $FULL_TABLE_NAME WHERE migration_date IS NOT NULL;" 2>&1 | xargs)
  MIGRATION_DATE_VALUE=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT migration_date FROM $FULL_TABLE_NAME WHERE migration_date IS NOT NULL LIMIT 1;" 2>&1 | xargs)
  
  if [ "$MIGRATION_DATE_COUNT" = "1" ] && [ "$MIGRATION_DATE_VALUE" = "2024-12-16" ]; then
    echo "  ✅ migration_date: All rows have value '$MIGRATION_DATE_VALUE'"
  else
    echo "  ⚠️  migration_date: Found $MIGRATION_DATE_COUNT distinct values (expected 1), value: '$MIGRATION_DATE_VALUE'"
  fi
  
  # Check source_system (should all be 'CASSANDRA_PROD')
  SOURCE_SYSTEM_COUNT=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT COUNT(DISTINCT source_system) FROM $FULL_TABLE_NAME WHERE source_system IS NOT NULL;" 2>&1 | xargs)
  SOURCE_SYSTEM_VALUE=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT source_system FROM $FULL_TABLE_NAME WHERE source_system IS NOT NULL LIMIT 1;" 2>&1 | xargs)
  
  if [ "$SOURCE_SYSTEM_COUNT" = "1" ] && [ "$SOURCE_SYSTEM_VALUE" = "CASSANDRA_PROD" ]; then
    echo "  ✅ source_system: All rows have value '$SOURCE_SYSTEM_VALUE'"
  else
    echo "  ⚠️  source_system: Found $SOURCE_SYSTEM_COUNT distinct values (expected 1), value: '$SOURCE_SYSTEM_VALUE'"
  fi
  
  # Check migration_run_id (should all be 1702732800000)
  MIGRATION_RUN_ID_COUNT=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT COUNT(DISTINCT migration_run_id) FROM $FULL_TABLE_NAME WHERE migration_run_id IS NOT NULL;" 2>&1 | xargs)
  MIGRATION_RUN_ID_VALUE=$(PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -t -c "SELECT migration_run_id FROM $FULL_TABLE_NAME WHERE migration_run_id IS NOT NULL LIMIT 1;" 2>&1 | xargs)
  
  if [ "$MIGRATION_RUN_ID_COUNT" = "1" ] && [ "$MIGRATION_RUN_ID_VALUE" = "1702732800000" ]; then
    echo "  ✅ migration_run_id: All rows have value '$MIGRATION_RUN_ID_VALUE'"
  else
    echo "  ⚠️  migration_run_id: Found $MIGRATION_RUN_ID_COUNT distinct values (expected 1), value: '$MIGRATION_RUN_ID_VALUE'"
  fi
  
  echo ""
  echo "Sample data (first 2 rows with constant columns):"
  PGPASSWORD="$YUGABYTE_PASSWORD" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" -c "SELECT cmpny_id, accnt_nbr, created_by, migration_date, source_system, migration_run_id FROM $FULL_TABLE_NAME LIMIT 2;" 2>&1
else
  echo "⚠️  No rows found in table - cannot verify constant columns"
fi

echo ""
echo "=========================================="
echo "Test Complete"
echo "=========================================="
echo ""
echo "Note: Properties file backup saved as: $PROPERTIES_FILE.backup"
echo "      To restore original: mv $PROPERTIES_FILE.backup $PROPERTIES_FILE"

