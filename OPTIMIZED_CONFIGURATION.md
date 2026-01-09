# Optimized Configuration for Your Infrastructure

## System Configuration Summary
- **YugabyteDB**: 3 nodes, 8 cores, 32GB RAM each (Azure Central, 3 zones)
- **Spark Workers**: 3 nodes, 8 cores, 32GB RAM each
- **Spark Master**: 1 node, 4 cores, 16GB RAM
- **Cassandra**: 3 nodes on-prem (cross-region from Azure)
- **Table**: 86M rows

## Issue: INSERT Mode vs COPY Mode Performance

**Reality Check:**
- COPY mode: 25K-35K rows/sec (fast but fails on duplicates)
- INSERT mode: Expected 15K-22K rows/sec, but getting 500 rows/sec
- Current performance is **unacceptable** (2-3% of expected)

## Recommendation: Use COPY Mode for Initial Load

Since you're doing a fresh load (no existing data), **COPY mode is the right choice**:

1. ✅ **Much faster** (25K-35K rows/sec vs 15K-22K INSERT mode)
2. ✅ **Faster completion** (~40-60 minutes for 86M rows)
3. ✅ **No duplicates** (fresh load)
4. ✅ **Better for bulk loading**

**Use INSERT mode only if:**
- Resuming from checkpoint (has existing data)
- Need idempotency (retries/resumes)
- Loading into table with existing data

## Action Plan

### Option 1: Use COPY Mode (RECOMMENDED for Fresh Load)

**Properties file changes:**
```properties
# Switch to COPY mode (much faster for fresh loads)
yugabyte.insertMode=COPY

# COPY mode settings (already optimized)
yugabyte.copyBufferSize=100000
yugabyte.copyFlushEvery=50000
```

**Expected performance:**
- Throughput: 25K-35K rows/sec
- Time for 86M rows: ~40-60 minutes
- No duplicate handling needed (fresh load)

### Option 2: Optimize INSERT Mode (if you must use INSERT)

**Properties file:**
```properties
# INSERT mode (slower but idempotent)
yugabyte.insertMode=INSERT
yugabyte.insertBatchSize=500  # Increased from 300 (better throughput)

# Spark configuration (optimized for your infrastructure)
spark.executor.instances=3
spark.executor.cores=6
spark.executor.memory=24g
spark.executor.memoryOverhead=4g
spark.driver.memory=8g
spark.default.parallelism=150
spark.sql.shuffle.partitions=150
```

**Expected performance:**
- Throughput: 15K-22K rows/sec
- Time for 86M rows: ~1-1.5 hours

## Optimized Properties File (COPY Mode)

```properties
# =============================================================================
# OPTIMIZED FOR: 86M Rows, Fresh Load, Azure Central YugabyteDB
# =============================================================================

# INSERT Mode - Use COPY for fresh loads
yugabyte.insertMode=COPY
yugabyte.insertBatchSize=5000  # Not used in COPY mode

# COPY Mode Settings (Optimized)
yugabyte.copyBufferSize=200000  # Increased buffer
yugabyte.copyFlushEvery=100000  # Flush more frequently

# Spark Configuration (Optimized for 3 workers × 8 cores × 32GB)
spark.executor.instances=3
spark.executor.cores=6  # Leave 2 cores for OS/system
spark.executor.memory=24g  # Leave 8GB for OS/overhead
spark.executor.memoryOverhead=4g
spark.driver.memory=8g
spark.default.parallelism=150  # 3 executors × 6 cores × 8-10 partitions per core
spark.sql.shuffle.partitions=150

# Memory settings
spark.memory.fraction=0.8
spark.memory.storageFraction=0.2

# Network and timeouts
spark.network.timeout=800s
spark.task.maxFailures=10

# Cassandra settings (cross-region optimization)
cassandra.fetchSizeInRows=50000
cassandra.concurrentReads=4096
cassandra.readTimeoutMs=180000  # Increased for cross-region
cassandra.inputSplitSizeMb.autoDetermine=true
```

## Spark Submit Command

```bash
spark-submit \
  --class com.company.migration.MainApp \
  --master yarn \
  --deploy-mode client \
  --driver-memory 8g \
  --executor-memory 24g \
  --executor-cores 6 \
  --num-executors 3 \
  --conf spark.default.parallelism=150 \
  --conf spark.sql.shuffle.partitions=150 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.network.timeout=800s \
  --conf spark.task.maxFailures=10 \
  --jars target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties
```

**OR if using standalone mode:**
```bash
spark-submit \
  --class com.company.migration.MainApp \
  --master spark://<master-host>:7077 \
  --driver-memory 8g \
  --executor-memory 24g \
  --executor-cores 6 \
  --total-executor-cores 18 \
  --conf spark.default.parallelism=150 \
  --conf spark.sql.shuffle.partitions=150 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.network.timeout=800s \
  --conf spark.task.maxFailures=10 \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties
```

## Things to Check in YugabyteDB

### 1. YBA UI - Performance Metrics
- **YSQL Ops/Sec**: Should be 80-120 ops/sec for COPY mode
- **CPU Usage**: Should be 50-80% per node
- **Memory Usage**: Should be reasonable (not maxed out)
- **Network I/O**: Check for bottlenecks

### 2. YBA UI - Database Metrics
- **Connections**: Number of active connections
- **Transaction Rate**: Should match YSQL Ops/Sec
- **Read/Write Latency**: Should be < 10ms for local operations

### 3. Cross-Region Latency
- **Cassandra → Spark**: Measure latency (on-prem to Azure Central)
- **Expected**: 20-50ms for cross-region
- **Impact**: Higher latency = slower reads from Cassandra

### 4. YugabyteDB Cluster Health
- **Node Status**: All 3 nodes should be UP
- **Replication**: Verify replication factor
- **Compaction**: Check if compactions are affecting performance
- **Load Balance**: Data should be distributed across zones

## Performance Targets

### COPY Mode (Recommended)
- **Throughput**: 25K-35K rows/sec
- **YSQL Ops/Sec**: 80-120 ops/sec
- **Time for 86M rows**: 40-60 minutes
- **CPU Usage**: 50-80% per node

### INSERT Mode (If using)
- **Throughput**: 15K-22K rows/sec
- **YSQL Ops/Sec**: 50-75 ops/sec
- **Time for 86M rows**: 1-1.5 hours
- **CPU Usage**: 50-70% per node

## Final Recommendation

**For 86M row fresh load:**
1. ✅ **Use COPY mode** (faster, suitable for fresh loads)
2. ✅ **Optimize Spark configuration** (use settings above)
3. ✅ **Monitor YBA UI** for bottlenecks
4. ✅ **Check cross-region latency** (Cassandra to Azure)
5. ✅ **Verify YugabyteDB cluster health**

**Expected result:**
- COPY mode: 40-60 minutes for 86M rows
- INSERT mode: 1-1.5 hours for 86M rows (if you must use it)

