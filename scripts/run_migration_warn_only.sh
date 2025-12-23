#!/bin/bash
# Run migration with WARN/ERROR only logging

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

PROPERTIES_FILE=${1:-"migration.properties"}

if [ ! -f "$PROPERTIES_FILE" ]; then
    echo "ERROR: Properties file not found: $PROPERTIES_FILE"
    exit 1
fi

# Check Spark
if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME=$HOME/spark-3.5.1
fi

if [ ! -d "$SPARK_HOME" ]; then
    echo "ERROR: Spark not found at $SPARK_HOME"
    exit 1
fi

echo "=========================================="
echo "Running Migration (WARN/ERROR logs only)"
echo "=========================================="
echo "Properties: $PROPERTIES_FILE"
echo "Spark: $SPARK_HOME"
echo ""

# Get log4j2.properties path (from classpath or file system)
LOG4J2_FILE="src/main/resources/log4j2.properties"
if [ ! -f "$LOG4J2_FILE" ]; then
    LOG4J2_FILE="log4j2.properties"
fi

if [ ! -f "$LOG4J2_FILE" ]; then
    echo "WARNING: log4j2.properties not found, using Spark defaults"
    LOG4J2_OPTIONS=""
else
    echo "Using log4j2.properties: $LOG4J2_FILE"
    # Pass log4j2.properties to Spark
    LOG4J2_OPTIONS="-Dlog4j.configuration=file:$(pwd)/$LOG4J2_FILE"
fi

"$SPARK_HOME/bin/spark-submit" \
  --class com.company.migration.MainApp \
  --master 'local[4]' \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --conf spark.default.parallelism=16 \
  --conf spark.driver.extraJavaOptions="$LOG4J2_OPTIONS" \
  --conf spark.executor.extraJavaOptions="$LOG4J2_OPTIONS" \
  --jars target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  "$PROPERTIES_FILE"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Migration completed successfully"
else
    echo "❌ Migration failed with exit code: $EXIT_CODE"
fi

exit $EXIT_CODE

