#!/bin/bash
# Test script that uses Spark 3.5.1 if available
# Usage: ./test_with_spark35.sh

SPARK_351_HOME="${SPARK_351_HOME:-/opt/spark-3.5.1-bin-hadoop3}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"

if [ -f "$SPARK_351_HOME/bin/spark-submit" ]; then
    echo "Using Spark 3.5.1 from: $SPARK_351_HOME"
    SPARK_SUBMIT="$SPARK_351_HOME/bin/spark-submit"
elif command -v spark-submit &> /dev/null; then
    VERSION=$(spark-submit --version 2>&1 | grep -oP 'version \K[0-9]+\.[0-9]+\.[0-9]+' | head -1)
    if [[ "$VERSION" == "3.5"* ]]; then
        echo "Using system Spark $VERSION"
    else
        echo "WARNING: System Spark is $VERSION, but Spark 3.5.1 is required"
        echo "Please install Spark 3.5.1 or set SPARK_351_HOME"
        exit 1
    fi
else
    echo "ERROR: spark-submit not found"
    exit 1
fi

$SPARK_SUBMIT --class com.company.migration.MainApp \
    --master 'local[2]' \
    --driver-memory 2g \
    --executor-memory 2g \
    --packages com.datastax.spark:spark-cassandra-connector_2.13:3.5.1 \
    target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
    migration.properties
