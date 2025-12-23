# How Spark Parallelism Affects YugabyteDB Connections

## The Connection

**Key Concept**: Each Spark partition = One YugabyteDB connection = One COPY stream

## How It Works

### 1. Spark Partitions → YugabyteDB Connections

```
Spark DataFrame (from Cassandra)
  │
  ├─ Partition 0 ──> Connection 0 ──> COPY stream 0
  ├─ Partition 1 ──> Connection 1 ──> COPY stream 1
  ├─ Partition 2 ──> Connection 2 ──> COPY stream 2
  ├─ Partition 3 ──> Connection 3 ──> COPY stream 3
  └─ ...
```

### 2. Code Flow

**In `TableMigrationJob.scala`**:
```scala
df.foreachPartition { (partition: Iterator[Row]) =>
  // Each partition executes independently
  val localConnectionFactory = new YugabyteConnectionFactory(localYugabyteConfig)
  val conn = localConnectionFactory.getConnection()  // NEW CONNECTION PER PARTITION
  val copyWriter = new CopyWriter(conn, copySql)
  // ... process partition ...
}
```

**Key Point**: `foreachPartition` runs once per partition, and each partition creates its own connection.

### 3. Parallelism → Partitions → Connections

| spark.default.parallelism | Spark Partitions | YugabyteDB Connections | Concurrent COPY Streams |
|---------------------------|------------------|------------------------|------------------------|
| 16                        | ~16              | 16                     | 16                     |
| 32                        | ~32              | 32                     | 32                     |
| 64                        | ~64              | 64                     | 64                     |

**Formula**: 
- `spark.default.parallelism` ≈ Number of partitions
- Number of partitions ≈ Number of concurrent YugabyteDB connections
- Each connection = One COPY FROM STDIN stream

## Why This Matters for Performance

### With Low Parallelism (16)

```
Time →
Partition 0: [████████████████] (processing)
Partition 1: [████████████████] (processing)
...
Partition 15: [████████████████] (processing)

Total: 16 concurrent COPY streams
Throughput: Limited by 16 streams
```

**Problem**: With network latency, each COPY stream waits for network I/O. Only 16 streams = limited parallelism.

### With High Parallelism (32)

```
Time →
Partition 0:  [████████████████] (processing)
Partition 1:  [████████████████] (processing)
...
Partition 31: [████████████████] (processing)

Total: 32 concurrent COPY streams
Throughput: 2x more concurrent work
```

**Benefit**: More streams = better latency hiding. While some streams wait for network, others are processing.

## Network Latency Impact

### Low Parallelism (16) with 20ms Latency

```
Each COPY operation:
  - Network round-trip: 20ms
  - Data transfer: 10ms
  - Total per operation: 30ms

With 16 streams:
  - Throughput = 16 streams / 0.03s = 533 operations/sec
```

### High Parallelism (32) with 20ms Latency

```
Same latency per operation: 30ms

With 32 streams:
  - Throughput = 32 streams / 0.03s = 1,066 operations/sec
  - 2x improvement!
```

## Connection Distribution Across YugabyteDB Nodes

With 3 YugabyteDB nodes and load balancing:

```
spark.default.parallelism=32

Connections:
  Node 1: ~11 connections (COPY streams)
  Node 2: ~11 connections (COPY streams)
  Node 3: ~10 connections (COPY streams)

Total: 32 concurrent COPY streams across 3 nodes
```

**YugabyteDB Smart Driver** automatically distributes connections across nodes when `yugabyte.loadBalanceHosts=true`.

## Why More Connections = Better Performance (Up to a Point)

### Benefits

1. **Latency Hiding**: While connection 1 waits for network, connections 2-32 are processing
2. **Better Resource Utilization**: More CPU cores can be used
3. **Load Distribution**: Work spread across all YugabyteDB nodes
4. **Pipeline Efficiency**: More concurrent operations = higher throughput

### Limits

**Too Many Connections** (>100-200):
- Connection overhead
- YugabyteDB resource limits
- Network congestion
- Diminishing returns

**Optimal Range**: 32-64 connections for remote environments

## Example: Your Current Situation

### Current (3.3K records/sec)

**Likely Configuration**:
```properties
spark.default.parallelism=16
```

**Result**:
- 16 Spark partitions
- 16 YugabyteDB connections
- 16 concurrent COPY streams
- Limited parallelism for network latency

### Optimized (Expected 6-7K records/sec)

**Optimized Configuration**:
```properties
spark.default.parallelism=32
```

**Result**:
- 32 Spark partitions
- 32 YugabyteDB connections
- 32 concurrent COPY streams
- 2x more concurrent work
- Better latency hiding

## Connection Lifecycle

### Per Partition

```scala
df.foreachPartition { partition =>
  // 1. Create connection (one per partition)
  val conn = connectionFactory.getConnection()
  
  // 2. Start COPY stream
  val copyWriter = new CopyWriter(conn, copySql)
  copyWriter.start()
  
  // 3. Process all rows in partition
  partition.foreach { row =>
    copyWriter.writeRow(csvRow)
  }
  
  // 4. End COPY and commit
  copyWriter.endCopy()
  conn.commit()
  
  // 5. Close connection
  conn.close()
}
```

**Key Points**:
- One connection per partition
- Connection lives for entire partition processing
- Connection closed after partition completes
- No connection pooling (by design for COPY)

## Monitoring Connections

### Check Active Connections

```sql
-- In YugabyteDB
SELECT 
  datname,
  COUNT(*) as connections,
  state
FROM pg_stat_activity
WHERE datname = 'your_database'
GROUP BY datname, state;
```

**Expected**: ~32 connections (if parallelism=32) in `active` or `idle in transaction` state

### During Migration

```
spark.default.parallelism=32
→ 32 partitions
→ 32 connections
→ 32 COPY streams
→ Higher throughput
```

## Memory Impact of Increased Parallelism

### Important Clarifications (Read First!)

⚠️ **Critical Understanding**:

1. **Memory pressure is PER EXECUTOR**, not cluster-wide
   - Parallelism increases memory pressure per executor, not total cluster memory
   - Formula: `Memory per executor = (parallelism / executors) × memory per partition`

2. **Concurrent tasks per executor** is what really matters
   - Limited by `spark.executor.cores`, not parallelism
   - Only `executor.cores` tasks run simultaneously per executor
   - Formula: `Memory per executor ≈ executor.cores × memory_per_task × safety_factor`

3. **Effective parallelism may be limited by Cassandra**
   - Cassandra Spark Connector often overrides parallelism
   - Actual partitions = min(requested parallelism, Cassandra token splits)
   - Token splits depend on: `spark.cassandra.input.split.size_in_mb`, cluster topology

4. **Memory per partition is workload-specific**
   - The 200-300MB figure is for **this specific workload** (Cassandra → CSV → COPY)
   - Depends on: row width, null density, string columns, collection types, COPY buffer size
   - **Not a universal constant** for all Spark jobs

5. **COPY buffer size directly affects memory**
   - `yugabyte.copyBufferSize` (e.g., 50,000 rows) = main memory consumer
   - Increasing buffer size = linear increase in memory usage
   - Formula: `Buffer memory = copyBufferSize × avg_row_size`

### How Parallelism Affects Memory

**Key Concept**: More parallelism = More concurrent operations = More memory usage **per executor**

**Important**: Memory pressure is driven by **concurrent tasks per executor** (limited by `executor.cores`), not total parallelism.

### Memory Components Per Partition

Each Spark partition (and thus each YugabyteDB connection) uses memory for:

1. **Row Buffer** (in `CopyWriter`):
   - Buffer size: `yugabyte.copyBufferSize` (default: 100,000 rows)
   - Memory per buffer: ~10-50MB (depends on row size)
   - Example: 100K rows × 500 bytes/row = ~50MB per partition

2. **CSV Encoding Buffer**:
   - Temporary memory for CSV conversion
   - ~5-10MB per partition

3. **Spark Task Memory**:
   - Spark execution memory per task
   - ~100-200MB per task (default)

4. **Connection Overhead**:
   - JDBC connection buffers
   - ~1-5MB per connection

**Total per partition**: ~150-300MB (varies by row size and buffer settings)

**Note**: This is a **workload-specific estimate** for:
- Cassandra rows → CSV transformation → COPY streaming
- Typical row width (10-50 columns)
- COPY buffer size: 50,000 rows (configurable via `yugabyte.copyBufferSize`)

**For different workloads**, memory per partition can vary significantly:
- Narrow tables (5 columns): ~100-150MB
- Wide tables (100+ columns): ~400-600MB
- Large collections/JSON: ~500MB-1GB+

### Memory Calculation

#### The Correct Formula

**Memory pressure is PER EXECUTOR**, not cluster-wide:

```
Memory per Executor = 
  Concurrent Tasks per Executor × 
  Memory per Task × 
  Safety Factor

Where:
  Concurrent Tasks per Executor = min(executor.cores, tasks_per_executor)
  Tasks per Executor = parallelism / executor.instances
  Memory per Task ≈ 200-300MB (workload-specific)
  Safety Factor = 1.5-2x
```

**Key Insight**: Only `executor.cores` tasks run **simultaneously** per executor, even if parallelism is higher.

#### Formula

```
Total Memory Needed = 
  (Memory per Partition × Number of Partitions) + 
  Driver Memory + 
  Overhead
```

#### Example Calculations

**Configuration 1: parallelism=16, executors=4, cores=4**
```
Partitions per executor: 16 / 4 = 4
Concurrent tasks per executor: min(4 cores, 4 partitions) = 4
Memory per task: ~200MB
Memory per executor: 4 × 200MB × 1.5 = 1.2GB
Recommended executor.memory: 4-6GB (with overhead)
Total cluster memory: 4 × 6GB = 24GB
```

**Configuration 2: parallelism=32, executors=8, cores=4**
```
Partitions per executor: 32 / 8 = 4
Concurrent tasks per executor: min(4 cores, 4 partitions) = 4
Memory per task: ~200MB
Memory per executor: 4 × 200MB × 1.5 = 1.2GB
Recommended executor.memory: 8-12GB (with overhead)
Total cluster memory: 8 × 8GB = 64GB
```

**Configuration 3: parallelism=64, executors=8, cores=8**
```
Partitions per executor: 64 / 8 = 8
Concurrent tasks per executor: min(8 cores, 8 partitions) = 8
Memory per task: ~200MB
Memory per executor: 8 × 200MB × 1.5 = 2.4GB
Recommended executor.memory: 16-24GB (with overhead)
Total cluster memory: 8 × 16GB = 128GB
```

**Key Point**: Memory pressure is **per executor**, not total. More executors = more total memory, but same pressure per executor.

### Memory Configuration Parameters

#### 1. Executor Memory (`spark.executor.memory`)

**Purpose**: Total memory available to each executor

**Correct Formula** (emphasizing concurrent tasks):
```
spark.executor.memory = 
  executor.cores × 
  Memory per task × 
  Safety factor (1.5-2x)

Where:
  Memory per task ≈ 200-300MB (workload-specific)
  executor.cores = concurrent tasks per executor (the real limit)
```

**Alternative Formula** (using parallelism):
```
spark.executor.memory = 
  (spark.default.parallelism / spark.executor.instances) × 
  Memory per partition × 
  Safety factor (1.5-2x)

But note: Only min(executor.cores, partitions_per_executor) tasks run concurrently
```

**Examples**:

| Parallelism | Executor Instances | Partitions per Executor | Recommended Memory |
|-------------|-------------------|------------------------|-------------------|
| 16          | 4                 | 4                      | 4-6GB             |
| 32          | 8                 | 4                      | 8-12GB            |
| 32          | 4                 | 8                      | 12-16GB           |
| 64          | 8                 | 8                      | 16-24GB           |

#### 2. Executor Memory Overhead (`spark.executor.memoryOverhead`)

**Purpose**: Memory for JVM overhead, native libraries, etc.

**Default**: `max(executor.memory × 0.1, 384MB)`

**Recommended**: 
```properties
spark.executor.memoryOverhead=2048m  # For 8GB+ executor memory
```

**Total Memory per Executor** = `executor.memory` + `executor.memoryOverhead`

#### 3. Driver Memory (`spark.driver.memory`)

**Purpose**: Memory for Spark driver (coordinator)

**Recommended**:
```properties
spark.driver.memory=4g  # For most workloads
spark.driver.memory=8g  # For large datasets or many partitions
```

#### 4. Memory Fraction (`spark.memory.fraction`)

**Purpose**: Fraction of executor memory for execution (vs storage)

**Default**: 0.6

**Recommended for COPY workloads**:
```properties
spark.memory.fraction=0.8  # More memory for execution (COPY operations)
spark.memory.storageFraction=0.2  # Less for caching
```

**Why**: COPY operations are execution-heavy, not storage-heavy

### Memory Matching Strategy

#### Step 1: Determine Target Parallelism

Based on performance needs:
- **Low latency environment**: parallelism = 16-32
- **High latency environment**: parallelism = 32-64
- **Very high latency**: parallelism = 64-128

#### Step 2: Calculate Partitions per Executor

```
Partitions per Executor = spark.default.parallelism / spark.executor.instances
```

**Important**: Actual partitions may be limited by Cassandra token splits:
```
Effective Partitions = min(
  spark.default.parallelism,
  Cassandra token splits (based on split.size_in_mb)
)
```

**Target**: 4-8 partitions per executor (optimal range)

**Note**: Only `executor.cores` tasks run **concurrently** per executor, even with more partitions.

#### Step 3: Calculate Memory per Executor

**Correct Formula** (emphasizing concurrent tasks):
```
Memory per Executor = 
  min(executor.cores, Partitions per Executor) × 
  Memory per Task (200-300MB) × 
  Safety Factor (1.5-2x)
```

**Why**: Only `executor.cores` tasks run simultaneously, even if there are more partitions.

**Alternative** (simpler, but less precise):
```
Memory per Executor = 
  Partitions per Executor × 
  Memory per Partition (200-300MB) × 
  Safety Factor (1.5-2x)
```

**Note**: This assumes partitions per executor ≤ executor.cores (which is typical).

#### Step 4: Configure Spark

```properties
spark.executor.instances=8
spark.executor.cores=4
spark.executor.memory=8g
spark.executor.memoryOverhead=2048m
spark.driver.memory=4g
spark.memory.fraction=0.8
spark.memory.storageFraction=0.2
```

### Real-World Examples

#### Example 1: Small Dataset (97K records)

**Target**: 32 parallelism for remote environment

**Configuration**:
```properties
spark.default.parallelism=32
spark.executor.instances=8
spark.executor.cores=4
spark.executor.memory=8g
spark.executor.memoryOverhead=2048m
spark.driver.memory=4g
```

**Memory Check**:
- Partitions per executor: 32 / 8 = 4
- Concurrent tasks per executor: min(4 cores, 4 partitions) = 4
- Memory per task: ~200MB
- Needed per executor: 4 × 200MB × 1.5 = 1.2GB
- Configured: 8GB ✅ (6.7x headroom - excellent)

#### Example 2: Large Dataset (25M records)

**Target**: 64 parallelism for high throughput

**Configuration**:
```properties
spark.default.parallelism=64
spark.executor.instances=8
spark.executor.cores=8
spark.executor.memory=16g
spark.executor.memoryOverhead=4096m
spark.driver.memory=8g
```

**Memory Check**:
- Partitions per executor: 64 / 8 = 8
- Concurrent tasks per executor: min(8 cores, 8 partitions) = 8
- Memory per task: ~250MB (larger rows)
- Needed per executor: 8 × 250MB × 1.5 = 3GB
- Configured: 16GB ✅ (5.3x headroom - good)

#### Example 3: Memory-Constrained Environment

**Constraint**: Only 32GB total memory available

**Strategy**: Reduce parallelism or increase executor instances

**Option A: Lower Parallelism**
```properties
spark.default.parallelism=32
spark.executor.instances=4
spark.executor.memory=8g
# Total: 4 × 8GB = 32GB ✅
```

**Option B: More Executors, Less Memory Each**
```properties
spark.default.parallelism=32
spark.executor.instances=8
spark.executor.memory=4g
# Total: 8 × 4GB = 32GB ✅
```

**Trade-off**: Option B has more overhead but better fault tolerance

### Memory Monitoring

#### During Migration

**Check Executor Memory Usage**:
```bash
# Spark UI: http://<driver>:4040
# Go to Executors tab
# Check: Memory Used / Memory Total
```

**Target**: 60-80% memory usage (healthy)
**Warning**: >90% usage = risk of OOM

#### Check Memory Pressure

**Signs of Memory Pressure**:
- Frequent GC pauses
- Task failures with OOM errors
- Slow performance
- High memory usage in Spark UI

**Solutions**:
1. Increase `spark.executor.memory`
2. Increase `spark.executor.memoryOverhead`
3. Reduce `spark.default.parallelism`
4. Reduce `yugabyte.copyBufferSize` (smaller buffers)

### Balancing Parallelism vs Memory

#### Decision Matrix

| Scenario | Parallelism | Memory per Executor | Executor Instances |
|----------|-------------|-------------------|-------------------|
| Low memory, low latency | 16 | 4GB | 4 |
| Low memory, high latency | 32 | 4GB | 8 |
| High memory, low latency | 32 | 8GB | 4 |
| High memory, high latency | 64 | 16GB | 8 |
| Very high memory | 128 | 32GB | 4-8 |

#### Rule of Thumb

**For Remote/High Latency**:
```
spark.default.parallelism = 32-64
spark.executor.memory = 8-16GB
spark.executor.instances = 4-8
```

**Memory Formula**:
```
spark.executor.memory ≥ (parallelism / executor.instances) × 300MB × 1.5
```

### Configuration Template

#### For 32 Parallelism (Recommended for Remote)

```properties
# Parallelism
spark.default.parallelism=32
spark.sql.shuffle.partitions=32

# Executors
spark.executor.instances=8
spark.executor.cores=4
spark.executor.memory=8g
spark.executor.memoryOverhead=2048m

# Driver
spark.driver.memory=4g

# Memory Management
spark.memory.fraction=0.8
spark.memory.storageFraction=0.2

# COPY Buffer (affects memory per partition - CRITICAL!)
yugabyte.copyBufferSize=50000  # Main memory consumer per partition
yugabyte.copyFlushEvery=25000
```

**Memory Calculation**:
- Partitions per executor: 32 / 8 = 4
- Concurrent tasks per executor: min(4 cores, 4 partitions) = 4
- Memory per task: ~200MB (with 50K buffer = ~100MB buffer + 100MB overhead)
- Needed: 4 × 200MB × 1.5 = 1.2GB
- Configured: 8GB ✅ (6.7x headroom)

**Note**: If you increase `yugabyte.copyBufferSize` to 100,000, memory per task increases to ~300MB, and needed memory becomes 1.8GB (still safe with 8GB).

#### For 64 Parallelism (High Throughput)

```properties
# Parallelism
spark.default.parallelism=64
spark.sql.shuffle.partitions=64

# Executors
spark.executor.instances=8
spark.executor.cores=8
spark.executor.memory=16g
spark.executor.memoryOverhead=4096m

# Driver
spark.driver.memory=8g

# Memory Management
spark.memory.fraction=0.8
spark.memory.storageFraction=0.2

# COPY Buffer
yugabyte.copyBufferSize=50000
yugabyte.copyFlushEvery=25000
```

**Memory Calculation**:
- Partitions per executor: 64 / 8 = 8
- Concurrent tasks per executor: min(8 cores, 8 partitions) = 8
- Memory per task: ~200MB
- Needed: 8 × 200MB × 1.5 = 2.4GB
- Configured: 16GB ✅ (6.7x headroom)

### Troubleshooting Memory Issues

#### Out of Memory (OOM) Errors

**Symptoms**:
```
java.lang.OutOfMemoryError: Java heap space
ExecutorLostFailure: Executor exited
```

**Solutions**:
1. **Increase executor memory**:
   ```properties
   spark.executor.memory=16g  # Increase from 8g
   ```

2. **Increase memory overhead**:
   ```properties
   spark.executor.memoryOverhead=4096m  # Increase from 2048m
   ```

3. **Reduce concurrent tasks** (most effective):
   ```properties
   spark.executor.cores=4  # Reduce from 8 (reduces concurrent tasks)
   # OR
   spark.executor.instances=16  # More executors, fewer tasks each
   ```

4. **Reduce parallelism** (less effective if cores are the limit):
   ```properties
   spark.default.parallelism=32  # Reduce from 64
   ```

5. **Reduce COPY buffer size** (direct impact):
   ```properties
   yugabyte.copyBufferSize=25000  # Reduce from 50000
   # This directly reduces memory per task
   ```

#### High Memory Usage but No OOM

**Symptoms**: Memory usage >90% but no errors

**Solutions**:
1. Monitor GC pauses (if frequent, increase memory)
2. Check for memory leaks (unlikely in this codebase)
3. Increase memory for safety margin

### Summary: Memory and Parallelism

**The Relationship**:
```
spark.default.parallelism
  ↓
Number of partitions
  ↓
Memory needed = Partitions × Memory per partition
  ↓
spark.executor.memory must be ≥ Memory needed × Safety factor
```

**Key Formulas**:

1. **Partitions per Executor** = `parallelism / executor.instances`
   - **Note**: May be limited by Cassandra token splits

2. **Concurrent Tasks per Executor** = `min(executor.cores, partitions_per_executor)`
   - **This is what really matters** for memory pressure

3. **Memory per Task** = ~200-300MB (workload-specific, depends on):
   - Row width (columns)
   - `yugabyte.copyBufferSize` (main factor)
   - Null density
   - Collection/JSON types

4. **Required Memory per Executor** = `Concurrent Tasks × Memory per Task × 1.5`
   - **Correct formula** (emphasizes concurrent tasks)
   - Alternative: `Partitions per Executor × Memory per Partition × 1.5` (simpler, assumes partitions ≤ cores)

5. **Configured Memory** = `spark.executor.memory` (should be ≥ Required)

**Recommended Configurations**:

| Parallelism | Executor Instances | Executor Memory | Total Memory |
|-------------|-------------------|-----------------|--------------|
| 16          | 4                 | 4-6GB           | 16-24GB      |
| 32          | 8                 | 8-12GB          | 64-96GB      |
| 64          | 8                 | 16-24GB         | 128-192GB    |

**For Your Case (97K records, remote environment)**:
- **Recommended**: parallelism=32, executor.instances=8, executor.memory=8GB
- **Total Memory**: ~64GB (8 executors × 8GB)
- **Memory Check**: 32 partitions / 8 executors = 4 partitions/executor × 200MB × 1.5 = 1.2GB needed, 8GB configured ✅

## Summary

**The Relationship**:
```
spark.default.parallelism
  ↓
Number of Spark partitions
  ↓
Number of concurrent YugabyteDB connections
  ↓
Number of concurrent COPY streams
  ↓
Throughput
```

**Memory Impact**:
```
spark.default.parallelism
  ↓
Number of partitions
  ↓
Partitions per executor = parallelism / executor.instances
  ↓
Concurrent tasks per executor = min(executor.cores, partitions_per_executor)
  ↓
Memory needed per executor = Concurrent tasks × Memory per task
  ↓
spark.executor.memory must match (per executor, not cluster-wide)
```

**Why It Works**:
- More parallelism = More concurrent connections
- More connections = More concurrent COPY streams
- More streams = Better latency hiding
- Better latency hiding = Higher throughput
- **But**: More parallelism = More memory needed

**For Your Case**:
- Current: 16 parallelism → 16 connections → 3.3K records/sec → ~3.2GB memory needed
- Optimized: 32 parallelism → 32 connections → 6-7K records/sec (expected) → ~6.4GB memory needed
- **Memory Config**: 8 executors × 8GB = 64GB total (plenty of headroom)

**Important**: 
- Each connection is independent and processes one partition
- More partitions = more connections = more concurrent work
- **But** more partitions = more memory needed
- Always match `spark.executor.memory` to parallelism requirements

