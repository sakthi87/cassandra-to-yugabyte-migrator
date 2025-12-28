#!/bin/bash
# Truncate the target YugabyteDB table

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

cd "$PROJECT_ROOT"

PROPERTIES_FILE="${PROPERTIES_FILE:-src/main/resources/migration.properties}"

TARGET_SCHEMA=$(grep "^table.target.schema=" "$PROPERTIES_FILE" | cut -d'=' -f2)
TARGET_TABLE=$(grep "^table.target.table=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_HOST=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | cut -d',' -f1)
YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_DB=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_USER=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2)
YUGABYTE_PASS=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2)

echo "Truncating table: $TARGET_SCHEMA.$TARGET_TABLE"

PGPASSWORD="$YUGABYTE_PASS" psql -h "$YUGABYTE_HOST" -p "$YUGABYTE_PORT" -U "$YUGABYTE_USER" -d "$YUGABYTE_DB" <<EOF
TRUNCATE TABLE $TARGET_SCHEMA.$TARGET_TABLE;
SELECT 'Table truncated successfully' as status;
EOF

echo "✅ Table truncated"

