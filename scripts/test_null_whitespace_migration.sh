#!/bin/bash
# Complete test for NULL/whitespace/non-ASCII handling

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "=========================================="
echo "Testing NULL/Whitespace/Non-ASCII Migration"
echo "=========================================="

# Step 1: Create test tables
echo ""
echo "Step 1: Creating test tables..."
"$SCRIPT_DIR/create_test_table.sh"
"$SCRIPT_DIR/create_yugabyte_test_table.sh"

# Step 2: Truncate YugabyteDB table
echo ""
echo "Step 2: Truncating YugabyteDB table..."
docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d test_migration -c "TRUNCATE TABLE test_null_whitespace;"' 2>&1 | grep -v "Warnings\|Note:" || true

# Step 3: Create test properties file
echo ""
echo "Step 3: Creating test properties file..."
cat > /tmp/test_migration.properties <<EOF
# =============================================================================
# Test Migration Configuration for NULL/Whitespace/Non-ASCII Testing
# =============================================================================

# Cassandra Connection Settings
cassandra.host=localhost
cassandra.port=9043
cassandra.localDC=datacenter1
cassandra.username=
cassandra.password=

# Cassandra Read Settings
cassandra.readTimeoutMs=120000
cassandra.fetchSizeInRows=1000
cassandra.consistencyLevel=LOCAL_ONE
cassandra.retryCount=3
cassandra.inputSplitSizeMb=64
cassandra.concurrentReads=512
cassandra.readsPerSec=0

# Cassandra Spark Connector Settings
cassandra.connection.localConnectionsPerExecutor=4
cassandra.connection.remoteConnectionsPerExecutor=1
cassandra.connection.timeoutMs=60000
cassandra.connection.keepAliveMs=30000
cassandra.connection.reconnectionDelayMs.min=1000
cassandra.connection.reconnectionDelayMs.max=60000
cassandra.connection.maxRequestsPerConnection.local=32768
cassandra.connection.maxRequestsPerConnection.remote=2000
cassandra.connection.factory=com.datastax.spark.connector.cql.DefaultConnectionFactory

# YugabyteDB Connection Settings
yugabyte.host=localhost
yugabyte.port=5433
yugabyte.database=test_migration
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
yugabyte.binaryTransfer=false
yugabyte.socketTimeout=0
yugabyte.loginTimeout=10

# YugabyteDB COPY Settings
yugabyte.copyBufferSize=10000
yugabyte.copyFlushEvery=5000
yugabyte.csvDelimiter=,
yugabyte.csvNull=
yugabyte.csvQuote="
yugabyte.csvEscape="

# YugabyteDB Transaction Settings
yugabyte.isolationLevel=READ_COMMITTED
yugabyte.autoCommit=false

# Spark Job Configuration
spark.executor.instances=2
spark.executor.cores=2
spark.executor.memory=4g
spark.executor.memoryOverhead=1024m
spark.driver.memory=2g
spark.default.parallelism=4
spark.sql.shuffle.partitions=4
spark.memory.fraction=0.8
spark.memory.storageFraction=0.2
spark.task.maxFailures=10
spark.stage.maxConsecutiveAttempts=4
spark.network.timeout=800s
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.dynamicAllocation.enabled=false

# Migration Settings
migration.jobId=test-null-whitespace-\${timestamp}
migration.checkpoint.enabled=false
migration.checkpoint.table=migration_checkpoint
migration.checkpoint.interval=1000
migration.validation.enabled=true
migration.validation.sampleSize=10

# Table Configuration
table.source.keyspace=test_migration
table.source.table=test_null_whitespace
table.target.schema=public
table.target.table=test_null_whitespace
table.validate=true
EOF

# Step 4: Run migration
echo ""
echo "Step 4: Running migration..."
if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME=$HOME/spark-3.5.1
fi

if [ ! -d "$SPARK_HOME" ]; then
    echo "ERROR: Spark not found at $SPARK_HOME"
    exit 1
fi

LOG_FILE="logs/test_null_whitespace_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

"$SPARK_HOME/bin/spark-submit" \
  --class com.company.migration.MainApp \
  --master 'local[2]' \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --conf spark.default.parallelism=4 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j2.properties" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j2.properties" \
  --jars target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  /tmp/test_migration.properties \
  2>&1 | tee "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

# Step 5: Verify results
echo ""
echo "=========================================="
echo "Step 5: Verifying Results"
echo "=========================================="

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "Migration completed. Verifying data..."
    
    echo ""
    echo "Records in YugabyteDB:"
    docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d test_migration -c "
SELECT 
    id,
    normal_text,
    CASE WHEN empty_string = '\'''\'' THEN '\''[EMPTY]'\'' ELSE '\''[NOT EMPTY]'\'' END as empty_string_status,
    CASE WHEN whitespace_only = '\''   '\'' THEN '\''[3 SPACES]'\'' 
         WHEN whitespace_only = '\''     '\'' THEN '\''[5 SPACES]'\''
         WHEN whitespace_only = '\''        '\'' THEN '\''[8 SPACES]'\''
         WHEN whitespace_only = '\'' '\'' THEN '\''[1 SPACE]'\''
         WHEN whitespace_only = '\''	'\'' THEN '\''[TAB]'\''
         ELSE '\''[OTHER]'\'' END as whitespace_only_status,
    leading_trailing_spaces,
    non_ascii_text,
    CASE WHEN null_value IS NULL THEN '\''[NULL]'\'' ELSE '\''[NOT NULL]'\'' END as null_value_status,
    mixed_content
FROM test_null_whitespace
ORDER BY id;
"' 2>&1 | grep -v "Warnings\|Note:" | head -20
    
    echo ""
    echo "Detailed verification for key test cases:"
    echo ""
    
    # Test 1: Empty string should be empty (not NULL)
    echo "Test 1: Empty string (id=1, should be empty string, not NULL):"
    docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d test_migration -t -c "
SELECT id, 
       CASE WHEN empty_string = '\'''\'' THEN '\''PASS: Empty string'\'' ELSE '\''FAIL: Not empty'\'' END,
       CASE WHEN empty_string IS NULL THEN '\''FAIL: Is NULL'\'' ELSE '\''PASS: Not NULL'\'' END
FROM test_null_whitespace WHERE id = 1;
"' 2>&1 | grep -v "Warnings\|Note:" | head -5
    
    # Test 2: Whitespace-only should be preserved
    echo ""
    echo "Test 2: Whitespace-only (id=1, should be 3 spaces, not NULL):"
    docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d test_migration -t -c "
SELECT id,
       length(whitespace_only) as length,
       CASE WHEN whitespace_only = '\''   '\'' THEN '\''PASS: 3 spaces preserved'\'' ELSE '\''FAIL: Not 3 spaces'\'' END,
       CASE WHEN whitespace_only IS NULL THEN '\''FAIL: Is NULL'\'' ELSE '\''PASS: Not NULL'\'' END
FROM test_null_whitespace WHERE id = 1;
"' 2>&1 | grep -v "Warnings\|Note:" | head -5
    
    # Test 3: Non-ASCII should be preserved
    echo ""
    echo "Test 3: Non-ASCII characters (id=1, should be café, not corrupted):"
    docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d test_migration -t -c "
SELECT id,
       non_ascii_text,
       CASE WHEN non_ascii_text = '\''café'\'' THEN '\''PASS: Non-ASCII preserved'\'' ELSE '\''FAIL: Not preserved'\'' END
FROM test_null_whitespace WHERE id = 1;
"' 2>&1 | grep -v "Warnings\|Note:" | head -5
    
    # Test 4: NULL value should be NULL
    echo ""
    echo "Test 4: NULL value (id=1, null_value should be NULL):"
    docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d test_migration -t -c "
SELECT id,
       CASE WHEN null_value IS NULL THEN '\''PASS: Is NULL'\'' ELSE '\''FAIL: Not NULL'\'' END
FROM test_null_whitespace WHERE id = 1;
"' 2>&1 | grep -v "Warnings\|Note:" | head -5
    
    echo ""
    echo "Record count comparison:"
    CASSANDRA_COUNT=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM test_migration.test_null_whitespace;" 2>&1 | grep -E "count|^[[:space:]]*[0-9]" | tr -d ' ' | grep -oE "[0-9]+")
    YUGABYTE_COUNT=$(docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d test_migration -t -c "SELECT COUNT(*) FROM test_null_whitespace;"' 2>&1 | grep -E "^[[:space:]]*[0-9]" | tr -d ' ')
    
    echo "  Cassandra: $CASSANDRA_COUNT"
    echo "  YugabyteDB: $YUGABYTE_COUNT"
    
    if [ "$CASSANDRA_COUNT" = "$YUGABYTE_COUNT" ]; then
        echo "  ✅ Counts match!"
    else
        echo "  ❌ Counts don't match!"
    fi
    
    echo ""
    echo "=========================================="
    if [ "$CASSANDRA_COUNT" = "$YUGABYTE_COUNT" ]; then
        echo "✅ Test PASSED: All records migrated correctly"
    else
        echo "❌ Test FAILED: Record count mismatch"
    fi
    echo "=========================================="
else
    echo ""
    echo "=========================================="
    echo "❌ Migration FAILED with exit code: $EXIT_CODE"
    echo "Check log file: $LOG_FILE"
    echo "=========================================="
fi

echo ""
echo "Log file: $LOG_FILE"
echo ""

