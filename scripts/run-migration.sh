#!/bin/bash

# Script to run the Cassandra to YugabyteDB migration
# Usage: ./scripts/run-migration.sh [properties-file]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROPERTIES_FILE="${1:-migration.properties}"

echo "=========================================="
echo "Cassandra to YugabyteDB Migration"
echo "=========================================="
echo "Project Directory: $PROJECT_DIR"
echo "Properties File: $PROPERTIES_FILE"
echo ""

# Check if JAR exists
JAR_FILE="$PROJECT_DIR/target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found: $JAR_FILE"
    echo "Please build the project first: mvn clean package"
    exit 1
fi

# Properties file is loaded from classpath (src/main/resources)
# If a custom file is provided, it should be in the classpath or passed as absolute path

# Set Spark submit command
SPARK_SUBMIT="spark-submit"

# Check if spark-submit is available
if ! command -v $SPARK_SUBMIT &> /dev/null; then
    echo "Error: spark-submit not found in PATH"
    echo "Please ensure Spark is installed and spark-submit is in your PATH"
    exit 1
fi

# Get Spark configuration from config file (if available)
# This is a simplified version - in production, you'd parse the config file

echo "Starting migration..."
echo ""

# Run the migration
$SPARK_SUBMIT \
    --class com.company.migration.MainApp \
    --master yarn \
    --deploy-mode client \
    "$JAR_FILE" \
    "$PROPERTIES_FILE"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✅ Migration completed successfully"
    echo "=========================================="
else
    echo ""
    echo "=========================================="
    echo "❌ Migration failed with exit code: $EXIT_CODE"
    echo "=========================================="
fi

exit $EXIT_CODE

