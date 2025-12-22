#!/bin/bash

# Script to run local test migration
# Usage: ./scripts/run_local_test.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Local Migration Test"
echo "=========================================="

cd "$PROJECT_DIR"

# Step 1: Setup test environment
echo ""
echo "Step 1: Setting up test environment..."
./scripts/setup_test_environment.sh

# Step 2: Create test tables
echo ""
echo "Step 2: Creating test tables..."
./scripts/create_test_tables.sh

# Step 3: Update properties file for test
echo ""
echo "Step 3: Updating properties file for test..."
cat > src/main/resources/migration.properties <<EOF
# =============================================================================
# Cassandra to YugabyteDB Migration Configuration - TEST
# =============================================================================

# =============================================================================
# Cassandra Connection Settings
# =============================================================================
cassandra.host=localhost
cassandra.port=9043
cassandra.localDC=datacenter1
cassandra.username=
cassandra.password=

# Cassandra Read Settings
cassandra.readTimeoutMs=120000
cassandra.fetchSizeInRows=1000
cassandra.consistencyLevel=LOCAL_QUORUM
cassandra.retryCount=3
cassandra.inputSplitSizeMb=64
cassandra.concurrentReads=512
cassandra.readsPerSec=0

# =============================================================================
# YugabyteDB Connection Settings
# =============================================================================
yugabyte.host=localhost
yugabyte.port=5433
yugabyte.database=test_keyspace
yugabyte.username=yugabyte
yugabyte.password=yugabyte

# YugabyteDB Connection Pool Settings
yugabyte.maxPoolSize=8
yugabyte.minIdle=2
yugabyte.connectionTimeout=30000
yugabyte.idleTimeout=300000
yugabyte.maxLifetime=1800000

# YugabyteDB JDBC Parameters
yugabyte.loadBalanceHosts=true
yugabyte.reWriteBatchedInserts=true
yugabyte.tcpKeepAlive=true
yugabyte.binaryTransfer=true
yugabyte.socketTimeout=0
yugabyte.loginTimeout=30

# YugabyteDB COPY Settings
yugabyte.copyBufferSize=10000
yugabyte.copyFlushEvery=10000
yugabyte.csvDelimiter=,
yugabyte.csvNull=
yugabyte.csvQuote="
yugabyte.csvEscape="

# YugabyteDB Transaction Settings
yugabyte.isolationLevel=READ_COMMITTED
yugabyte.autoCommit=false

# =============================================================================
# Spark Job Configuration (Local Mode)
# =============================================================================
spark.executor.instances=2
spark.executor.cores=2
spark.executor.memory=2g
spark.executor.memoryOverhead=512m
spark.driver.memory=2g
spark.default.parallelism=4
spark.sql.shuffle.partitions=4
spark.memory.fraction=0.6
spark.memory.storageFraction=0.3
spark.task.maxFailures=10
spark.stage.maxConsecutiveAttempts=4
spark.network.timeout=800s
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.dynamicAllocation.enabled=false
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=4
spark.dynamicAllocation.initialExecutors=2

# =============================================================================
# Migration Settings
# =============================================================================
migration.jobId=migration-job-test-\${timestamp}
migration.checkpoint.enabled=false
migration.checkpoint.table=migration_checkpoint
migration.checkpoint.interval=10000
migration.validation.enabled=true
migration.validation.sampleSize=1000

# =============================================================================
# Table Configuration
# =============================================================================
table.source.keyspace=test_keyspace
table.source.table=customer_transactions
table.target.schema=public
table.target.table=customer_transactions
table.validate=true
EOF

echo "Properties file updated for test"

# Step 4: Build project
echo ""
echo "Step 4: Building project..."
mvn clean package -DskipTests

# Step 5: Run migration (using local Spark mode)
echo ""
echo "Step 5: Running migration..."
echo ""

# Check if spark-submit is available
if command -v spark-submit &> /dev/null; then
    spark-submit \
        --class com.company.migration.MainApp \
        --master local[2] \
        --driver-memory 2g \
        --executor-memory 2g \
        target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
        migration.properties
else
    echo "Warning: spark-submit not found in PATH"
    echo "Please install Spark or run manually:"
    echo "  spark-submit --class com.company.migration.MainApp --master local[2] target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar migration.properties"
    exit 1
fi

# Step 6: Validate results
echo ""
echo "Step 6: Validating migration results..."
echo ""

echo "Checking row counts..."
CASSANDRA_COUNT=$(docker exec -i cassandra cqlsh localhost 9042 -e "USE test_keyspace; SELECT COUNT(*) FROM customer_transactions;" | grep -oP '\d+' | head -1)
YUGABYTE_COUNT=$(docker exec -i yugabyte bash -c '/home/yugabyte/bin/ysqlsh --host $(hostname) -U yugabyte -d test_keyspace -t -c "SELECT COUNT(*) FROM customer_transactions;"' | grep -oP '\d+' | head -1)

echo "Cassandra row count: $CASSANDRA_COUNT"
echo "YugabyteDB row count: $YUGABYTE_COUNT"

if [ "$CASSANDRA_COUNT" = "$YUGABYTE_COUNT" ]; then
    echo ""
    echo "=========================================="
    echo "✅ Migration test PASSED!"
    echo "=========================================="
    echo "Row counts match: $CASSANDRA_COUNT records migrated successfully"
else
    echo ""
    echo "=========================================="
    echo "❌ Migration test FAILED!"
    echo "=========================================="
    echo "Row counts don't match: Cassandra=$CASSANDRA_COUNT, YugabyteDB=$YUGABYTE_COUNT"
    exit 1
fi

echo ""
echo "Sample data in YugabyteDB:"
docker exec -i yugabyte bash -c '/home/yugabyte/bin/ysqlsh --host $(hostname) -U yugabyte -d test_keyspace -c "SELECT * FROM customer_transactions LIMIT 5;"'

echo ""
echo "=========================================="
echo "✅ Test completed successfully!"
echo "=========================================="

