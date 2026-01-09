# INSERT Mode Optimized Configuration

## System Configuration
- **YugabyteDB**: 3 nodes, 8 cores, 32GB RAM each (Azure Central, 3 zones)
- **Spark Workers**: 3 nodes, 8 cores, 32GB RAM each
- **Spark Master**: 1 node, 4 cores, 16GB RAM
- **Cassandra**: 3 nodes on-prem (cross-region from Azure)
- **Table**: 86M rows
- **Issue**: COPY mode failing with duplicates, need INSERT mode

---

## Optimized Properties File for INSERT Mode

```properties
# =============================================================================
# INSERT Mode Configuration (Idempotent, handles duplicates)
# =============================================================================

# INSERT Mode - Use for duplicate handling
yugabyte.insertMode=INSERT
yugabyte.insertBatchSize=500  # Increased from 300 for better throughput (still safe)

# YugabyteDB Connection Settings
yugabyte.host=your-yugabyte-host1,your-yugabyte-host2,your-yugabyte-host3
yugabyte.port=5433
yugabyte.database=transaction_datastore
yugabyte.username=yugabyte
yugabyte.password=yugabyte

# Connection Pool (optimized for INSERT mode)
yugabyte.maxPoolSize=12  # Increase for INSERT mode (more concurrent connections)
yugabyte.minIdle=4
yugabyte.connectionTimeout=30000
yugabyte.idleTimeout=300000
yugabyte.maxLifetime=1800000

# Transaction Settings
yugabyte.isolationLevel=READ_COMMITTED
yugabyte.autoCommit=false

# =============================================================================
# Spark Configuration (Optimized for 3 workers × 8 cores × 32GB)
# =============================================================================

# Executor Configuration
spark.executor.instances=3
spark.executor.cores=6  # Leave 2 cores for OS/system per worker
spark.executor.memory=24g  # Leave 8GB for OS/overhead per worker
spark.executor.memoryOverhead=4g
spark.driver.memory=8g

# Parallelism (optimized for INSERT mode)
spark.default.parallelism=120  # 3 executors × 6 cores × 6-7 partitions per core
spark.sql.shuffle.partitions=120

# Memory Settings
spark.memory.fraction=0.8
spark.memory.storageFraction=0.2

# Network and Timeouts (critical for INSERT mode)
spark.network.timeout=800s
spark.task.maxFailures=10
spark.stage.maxConsecutiveAttempts=4

# Serialization
spark.serializer=org.apache.spark.serializer.KryoSerializer

# Disable dynamic allocation for consistent performance
spark.dynamicAllocation.enabled=false

# Additional Optimizations
spark.locality.wait=0s
spark.executor.heartbeatInterval=60s
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true

# =============================================================================
# Cassandra Configuration (Cross-region optimization)
# =============================================================================

cassandra.host=your-cassandra-host1,your-cassandra-host2,your-cassandra-host3
cassandra.port=9042
cassandra.localDC=datacenter1
cassandra.readTimeoutMs=180000  # Increased for cross-region latency
cassandra.fetchSizeInRows=50000
cassandra.consistencyLevel=LOCAL_ONE
cassandra.concurrentReads=4096

# Split Size (auto-determined, but can override)
cassandra.inputSplitSizeMb.autoDetermine=true
# cassandra.inputSplitSizeMb.override=512

# =============================================================================
# Migration Settings
# =============================================================================

migration.checkpoint.enabled=true
migration.checkpoint.keyspace=public
migration.checkpoint.interval=50000  # Checkpoint every 50K rows

migration.runId=
migration.prevRunId=0

migration.validation.enabled=true
migration.validation.sampleSize=1000

# =============================================================================
# Table Configuration
# =============================================================================

table.source.keyspace=transaction_datastore
table.source.table=dda_pstd_fincl_txn_cnsmr_by_accntnbr

table.target.schema=public
table.target.table=dda_pstd_fincl_txn_cnsmr_by_accntnbr

table.validate=true
```

---

## Key Configuration Differences for INSERT Mode

### 1. Batch Size: 500 (not 300)
- **300**: Very safe, but slower (~15K-18K rows/sec)
- **500**: Good balance, faster (~18K-22K rows/sec), still stable
- **Rationale**: 500 is safe for your infrastructure, gives better throughput

### 2. Parallelism: 120 (not 150)
- **150**: Too many concurrent INSERT transactions = serialization conflicts
- **120**: Better balance, reduces conflicts
- **Formula**: 3 executors × 6 cores × 6-7 partitions per core

### 3. Connection Pool: 12 (increased)
- **Default**: 8
- **INSERT mode**: 12 (more concurrent connections for INSERT operations)
- **Rationale**: INSERT mode benefits from more connections

### 4. Executor Cores: 6 (not 8)
- **8 cores total**: Use 6 for Spark, leave 2 for OS/system
- **Prevents**: Resource contention

### 5. Executor Memory: 24g (not 32g)
- **32GB total**: Use 24GB for Spark, leave 8GB for OS/overhead
- **Prevents**: Out of memory errors

---

## Spark Submit Command for INSERT Mode

### For YARN:

```bash
spark-submit \
  --class com.company.migration.MainApp \
  --master yarn \
  --deploy-mode client \
  --driver-memory 8g \
  --executor-memory 24g \
  --executor-cores 6 \
  --num-executors 3 \
  --conf spark.default.parallelism=120 \
  --conf spark.sql.shuffle.partitions=120 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.network.timeout=800s \
  --conf spark.task.maxFailures=10 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties
```

### For Standalone:

```bash
spark-submit \
  --class com.company.migration.MainApp \
  --master spark://<master-host>:7077 \
  --driver-memory 8g \
  --executor-memory 24g \
  --executor-cores 6 \
  --total-executor-cores 18 \
  --conf spark.default.parallelism=120 \
  --conf spark.sql.shuffle.partitions=120 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.network.timeout=800s \
  --conf spark.task.maxFailures=10 \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties
```

---

## Expected Performance for INSERT Mode

### Throughput Targets:
- **Expected**: 18K-22K rows/sec (with batch size 500)
- **Conservative**: 15K-18K rows/sec (if issues occur, reduce batch size to 300)

### YBA UI Metrics:
- **YSQL Ops/Sec**: 36-44 ops/sec (for 18K-22K rows/sec)
  - Calculation: 18,000 rows/sec ÷ 500 rows/batch = 36 ops/sec
  - Calculation: 22,000 rows/sec ÷ 500 rows/batch = 44 ops/sec
- **CPU Usage**: 50-70% per node
- **Memory Usage**: Reasonable (not maxed out)

### Time Estimate for 86M Rows:
- **At 18K rows/sec**: 86M ÷ 18K = 4,778 seconds = **80 minutes (1.3 hours)**
- **At 22K rows/sec**: 86M ÷ 22K = 3,909 seconds = **65 minutes (1.1 hours)**
- **Conservative estimate**: **1.5-2 hours** (if performance is lower)

---

## What to Check in YBA UI

### 1. YSQL Tab - Performance Metrics
- **Total YSQL Ops/Sec**: Should be **36-44 ops/sec**
  - If lower: Check for errors, reduce batch size to 300
  - If higher: Good, but monitor for errors
- **INSERT Ops/Sec** (if available): Should match YSQL Ops/Sec

### 2. Resource Usage
- **CPU Usage**: **50-70%** per node (good utilization)
- **Memory Usage**: Should be reasonable (not maxed out)
- **Network I/O**: Check for bottlenecks

### 3. Cluster Health
- **All 3 nodes**: UP and healthy
- **Replication**: Verify data is distributed
- **Compaction**: Check if affecting performance

### 4. Errors
- **Watch for**: "Snapshot too old" errors
  - If occurs: Reduce batch size to 300
- **Watch for**: Serialization conflicts
  - If occurs: Reduce parallelism to 100

---

## Troubleshooting INSERT Mode

### If Throughput is Low (< 10K rows/sec):

1. **Check batch size**: Should be 500 (not 300)
2. **Check parallelism**: Should be 120
3. **Check for errors**: Look for "snapshot too old" or serialization errors
4. **Check YBA UI**: Are all nodes healthy?
5. **Check network**: Cross-region latency (Cassandra to Azure)

### If Getting "Snapshot Too Old" Errors:

**Solution**: Reduce batch size
```properties
yugabyte.insertBatchSize=300  # Reduce from 500 to 300
```

### If Getting Serialization Conflicts:

**Solution**: Reduce parallelism
```properties
spark.default.parallelism=100  # Reduce from 120 to 100
spark.sql.shuffle.partitions=100
```

### If Performance is Still Low:

1. **Check Spark UI**: Are all partitions running?
2. **Check logs**: Any partition failures?
3. **Check YBA UI**: CPU/memory bottlenecks?
4. **Check network**: High latency to Cassandra?

---

## Performance Comparison

| Mode | Throughput | Time for 86M | Duplicate Handling |
|------|-----------|--------------|-------------------|
| **COPY** | 25K-35K rows/sec | 40-60 min | ❌ Fails on duplicates |
| **INSERT (batch 500)** | 18K-22K rows/sec | 65-80 min | ✅ Handles duplicates |
| **INSERT (batch 300)** | 15K-18K rows/sec | 80-95 min | ✅ Handles duplicates (safer) |

---

## Summary

**For INSERT mode with your infrastructure:**

1. ✅ **Batch size: 500** (good balance of speed and stability)
2. ✅ **Parallelism: 120** (reduces conflicts)
3. ✅ **Executor cores: 6** (leaves 2 for OS)
4. ✅ **Executor memory: 24g** (leaves 8GB for OS)
5. ✅ **Connection pool: 12** (more concurrent INSERTs)
6. ✅ **Expected: 18K-22K rows/sec** (65-80 minutes for 86M rows)
7. ✅ **YBA UI: 36-44 YSQL ops/sec**

**If issues occur:**
- Reduce batch size to 300
- Reduce parallelism to 100
- Monitor YBA UI for bottlenecks

