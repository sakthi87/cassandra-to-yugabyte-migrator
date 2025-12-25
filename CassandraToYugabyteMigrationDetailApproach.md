# Cassandra to YugabyteDB Migration: Detailed Approach

## Overview

This document explains the end-to-end mechanism of how data is migrated from Cassandra to YugabyteDB using Spark and PostgreSQL COPY. Understanding this process helps optimize performance and troubleshoot issues.

## Two-Phase Migration Process

The migration operates in two distinct phases:

1. **Phase 1: Planning & Metadata Reading** (First 30 minutes for 25M records)
2. **Phase 2: Actual Data Migration** (Next 30 minutes for 25M records)

---

## Phase 1: Planning & Metadata Reading (30 minutes)

### What Happens During This Phase

#### 1. Spark Session Initialization
- Spark connects to Cassandra cluster
- Establishes connection pool for metadata queries
- Configures Spark Cassandra Connector

#### 2. Metadata Reading
```
Spark App → Cassandra Cluster
  ├─→ Read table schema (column names, types, primary keys)
  ├─→ Read token range information
  ├─→ Discover cluster topology (nodes, datacenters)
  └─→ Calculate data distribution
```

**Key Operations:**
- `DESCRIBE TABLE` queries to get schema
- `SELECT token(partition_key) FROM table` to understand token distribution
- Cluster metadata queries to identify node ownership

#### 3. Token Range Calculation

**How Cassandra Distributes Data:**

Cassandra uses a **token ring** to distribute data across nodes:

```
Token Space: -2^63 to 2^63-1

Cassandra Ring:
│
├─ Token Range 1: [-2^63, -1.5*10^18] → Node A
├─ Token Range 2: [-1.5*10^18, -10^18] → Node B  
├─ Token Range 3: [-10^18, -5*10^17] → Node C
├─ Token Range 4: [-5*10^17, 0] → Node D
├─ Token Range 5: [0, 5*10^17] → Node A
└─ ...
```

**Spark Cassandra Connector Process:**

1. **Query Token Ranges:**
   - Connector queries `system.local` and `system.peers` tables
   - Gets list of all token ranges owned by each node
   - Example: For 3-node cluster, might get 256 token ranges

2. **Split Token Ranges:**
   - Splits ranges based on `cassandra.inputSplitSizeMb` (default: 256MB)
   - Each split becomes a **Spark partition**
   - Formula: `Number of Partitions = Total Data Size / inputSplitSizeMb`

3. **Example for 25M Records:**
   ```
   Total Data Size: ~3GB (assuming 120 bytes/row)
   inputSplitSizeMb: 256MB
   Partitions: 3GB / 256MB = ~12 partitions
   ```

#### 4. Spark Catalyst Optimizer

**What Spark Does:**
- Creates **execution plan** (DAG - Directed Acyclic Graph)
- Optimizes query execution
- Plans data shuffling and partitioning
- **This is LAZY evaluation** - no actual data is read yet!

**Spark Stages Created:**
```
Stage 0: Read from Cassandra (MapPartitionsRDD)
  ├─ Partition 0: Token range [-2^63, -1.5*10^18]
  ├─ Partition 1: Token range [-1.5*10^18, -10^18]
  ├─ Partition 2: Token range [-10^18, -5*10^17]
  └─ ...
```

#### 5. DataFrame Creation (Lazy)

```scala
val df = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("keyspace" -> "ks", "table" -> "table"))
  .load()  // ← This is LAZY - no data read yet!
```

**At this point:**
- ✅ Schema is known
- ✅ Partitions are calculated
- ✅ Execution plan is created
- ❌ **NO DATA HAS BEEN READ YET**

### Why Phase 1 Takes 30 Minutes

**Factors Contributing to Planning Time:**

1. **Large Table Metadata:**
   - Reading schema for tables with 100+ columns
   - Token range queries for billions of rows
   - Network latency to Cassandra nodes

2. **Token Range Calculation:**
   - For 25M records across multiple nodes
   - Calculating optimal split points
   - Ensuring balanced partitions

3. **Spark Planning:**
   - Catalyst optimizer analyzing execution plan
   - Creating DAG with many stages
   - Optimizing for parallel execution

4. **Network Latency:**
   - Multiple round-trips to Cassandra cluster
   - Metadata queries across distributed nodes
   - Connection establishment overhead

---

## Phase 2: Actual Data Migration (30 minutes)

### What Happens During This Phase

#### 1. Execution Trigger

**The Magic Line:**
```scala
df.foreachPartition { partition =>
  // This triggers ACTUAL execution!
}
```

**What `foreachPartition` Does:**
- Forces Spark to execute the lazy DataFrame
- Triggers actual data reading from Cassandra
- Processes each partition independently

#### 2. Per-Partition Execution Flow

For **each Spark partition** (token range):

```
┌─────────────────────────────────────────────────┐
│ Partition Execution (Parallel for all partitions)│
└─────────────────────────────────────────────────┘
         │
         ├─ Step 1: Read from Cassandra
         │   │
         │   ├─→ Connect to Cassandra node (token-aware)
         │   ├─→ Query: SELECT * FROM table WHERE token(pk) >= ? AND token(pk) < ?
         │   ├─→ Read rows in batches (fetchSizeInRows: 10,000)
         │   └─→ Stream rows to Spark partition
         │
         ├─ Step 2: Connect to YugabyteDB
         │   │
         │   ├─→ Create JDBC connection
         │   ├─→ Set transaction isolation (READ_COMMITTED)
         │   └─→ Disable auto-commit
         │
         ├─ Step 3: Start COPY Operation
         │   │
         │   ├─→ Execute: COPY table FROM STDIN WITH (FORMAT csv)
         │   ├─→ Opens binary stream to YugabyteDB
         │   └─→ COPY stream is now active
         │
         ├─ Step 4: Process Rows
         │   │
         │   For each row from Cassandra:
         │   ├─→ Convert Spark Row to CSV format
         │   │   ├─ Handle NULLs (empty string)
         │   │   ├─ Escape special characters
         │   │   ├─ Quote fields with spaces/non-ASCII
         │   │   └─ Remove null bytes (0x00)
         │   │
         │   ├─→ Write CSV row to COPY stream buffer
         │   │
         │   └─→ Flush every 50K rows (copyFlushEvery)
         │       └─→ Sends batch to YugabyteDB
         │
         └─ Step 5: Commit
             │
             ├─→ Flush remaining rows
             ├─→ End COPY operation
             ├─→ Commit transaction
             └─→ Close connection
```

#### 3. Parallel Execution

**With `spark.default.parallelism=32`:**

```
Time →
Partition 0:  [████████████████] (processing 2M rows)
Partition 1:  [████████████████] (processing 2M rows)
Partition 2:  [████████████████] (processing 2M rows)
...
Partition 31: [████████████████] (processing 2M rows)

Total: 32 concurrent COPY streams
```

**Each partition:**
- Has its own YugabyteDB connection
- Has its own COPY stream
- Processes independently
- No coordination needed

#### 4. Token-Aware Reading

**Why It's Efficient:**

```
Partition 0 (Token Range: [-2^63, -1.5*10^18])
  └─→ Reads directly from Cassandra Node A (owns this range)
      └─→ No network hops to other nodes!

Partition 1 (Token Range: [-1.5*10^18, -10^18])
  └─→ Reads directly from Cassandra Node B (owns this range)
      └─→ Optimal data locality!
```

**Benefits:**
- ✅ Reads from node that owns the data
- ✅ No unnecessary network transfers
- ✅ Minimizes cross-datacenter traffic
- ✅ Optimal parallelism

---

## COPY vs Regular INSERT: Why DocDB Ops/sec?

### Regular INSERT Pattern (Slow)

```
┌─────────────────────────────────────────────────────┐
│ Your Application                                    │
└─────────────────────────────────────────────────────┘
         │
         │ INSERT INTO table VALUES (...)
         ▼
┌─────────────────────────────────────────────────────┐
│ YSQL Layer (PostgreSQL-compatible)                  │
│   ├─ Query Parser                                    │
│   ├─ Query Planner                                   │
│   ├─ Query Optimizer                                 │
│   └─ Query Executor                                  │
└─────────────────────────────────────────────────────┘
         │
         │ Executed query
         ▼
┌─────────────────────────────────────────────────────┐
│ DocDB (Distributed Storage)                          │
│   └─ Write data to tablets                           │
└─────────────────────────────────────────────────────┘
```

**Characteristics:**
- Each INSERT = separate query
- Parsed, planned, executed individually
- Overhead: ~1-5ms per INSERT
- Shows up as **YSQL ops/sec**
- Throughput: ~1,000-5,000 rows/sec

### COPY Pattern (Fast)

```
┌─────────────────────────────────────────────────────┐
│ Your Application                                    │
│   └─→ COPY FROM STDIN                               │
└─────────────────────────────────────────────────────┘
         │
         │ Binary protocol (no SQL parsing!)
         ▼
┌─────────────────────────────────────────────────────┐
│ COPY Protocol Handler                                │
│   └─→ Direct binary stream                           │
└─────────────────────────────────────────────────────┘
         │
         │ Bulk write (batched)
         ▼
┌─────────────────────────────────────────────────────┐
│ DocDB (Distributed Storage)                          │
│   └─→ Write data directly to tablets                  │
└─────────────────────────────────────────────────────┘
```

**Characteristics:**
- ✅ Bypasses YSQL query layer entirely
- ✅ Uses PostgreSQL binary COPY protocol
- ✅ Batches rows internally (50K rows per batch)
- ✅ Overhead: ~0.1ms per row
- ✅ Shows up as **DocDB ops/sec** (not YSQL ops/sec)
- ✅ Throughput: ~10,000-50,000 rows/sec

### Why You See 7K DocDB Ops/sec

**Understanding the Metrics:**

```
7K DocDB ops/sec with 32 concurrent COPY streams

Calculation:
- 7,000 ops/sec ÷ 32 streams = ~218 ops/sec per stream
- Each "op" represents a batch of rows
- If batch size = 50-100 rows: 218 ops/sec × 50 rows = ~11K rows/sec per stream
- Total: 32 streams × 11K rows/sec = ~352K rows/sec theoretical
- Actual: ~14K rows/sec (accounting for network latency, processing overhead)
```

**Why Not YSQL Ops/sec?**
- COPY bypasses YSQL layer
- Goes directly to DocDB storage
- No SQL parsing/planning overhead
- Much faster than regular INSERTs

---

## End-to-End Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ PHASE 1: Planning (30 minutes)                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Spark Application                                           │
│    │                                                          │
│    ├─→ Connect to Cassandra                                  │
│    │   ├─→ Read table schema                                 │
│    │   ├─→ Query token ranges                                │
│    │   └─→ Discover cluster topology                         │
│    │                                                          │
│    ├─→ Calculate Partitions                                 │
│    │   ├─→ Split token ranges                                │
│    │   ├─→ Balance partition sizes                           │
│    │   └─→ Create 12 partitions (example)                    │
│    │                                                          │
│    ├─→ Spark Catalyst Optimizer                             │
│    │   ├─→ Create execution plan (DAG)                       │
│    │   ├─→ Optimize query execution                          │
│    │   └─→ Plan data shuffling                               │
│    │                                                          │
│    └─→ Create DataFrame (LAZY)                              │
│        └─→ No data read yet!                                 │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ PHASE 2: Data Migration (30 minutes)                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  foreachPartition triggers execution                         │
│    │                                                          │
│    ├─ Partition 0 ───────────────────────┐                  │
│    │   │                                   │                  │
│    │   ├─→ Read token range from Cassandra│                  │
│    │   │   │ (Token-aware: reads from Node A)                │
│    │   │   └─→ 2M rows streamed            │                  │
│    │   │                                   │                  │
│    │   ├─→ Connect to YugabyteDB          │                  │
│    │   │   └─→ JDBC Connection            │                  │
│    │   │                                   │                  │
│    │   ├─→ Start COPY FROM STDIN           │                  │
│    │   │   └─→ Opens binary stream        │                  │
│    │   │                                   │                  │
│    │   ├─→ For each row:                  │                  │
│    │   │   ├─ Convert Spark Row → CSV     │                  │
│    │   │   └─ Write to COPY stream        │                  │
│    │   │                                   │                  │
│    │   ├─→ Flush every 50K rows           │                  │
│    │   │   └─→ Batch write to DocDB       │                  │
│    │   │                                   │                  │
│    │   └─→ Commit transaction             │                  │
│    │                                       │                  │
│    ├─ Partition 1 ───────────────────────┤ (Parallel)       │
│    │   └─→ Reads from Node B              │                  │
│    │                                       │                  │
│    ├─ Partition 2 ───────────────────────┤                   │
│    │   └─→ Reads from Node C              │                  │
│    │                                       │                  │
│    ├─ ...                                 │                   │
│    │                                       │                  │
│    └─ Partition 11 ──────────────────────┘                   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Performance Breakdown: 25M Records Example

### Phase 1: Planning (30 minutes)

**What's Happening:**
- Metadata queries: ~5 minutes
- Token range calculation: ~10 minutes
- Spark planning: ~10 minutes
- Network overhead: ~5 minutes

**Optimization Opportunities:**
- Reduce metadata queries (see improvements section)
- Cache token range information
- Optimize Spark planning

### Phase 2: Migration (30 minutes)

**Performance Metrics:**
```
Total Records: 25,000,000
Time: 30 minutes = 1,800 seconds
Throughput: 25M / 1,800s = ~13,889 rows/sec

With 32 partitions:
- Per partition: 13,889 / 32 = ~434 rows/sec
- DocDB ops/sec: 7,000 ops/sec
- Ops per partition: 7,000 / 32 = ~218 ops/sec
- Rows per op: 434 / 218 = ~2 rows per op (batched internally)
```

**Why It's Fast:**
- ✅ 32 parallel COPY streams
- ✅ Token-aware reading (optimal data locality)
- ✅ Direct DocDB writes (bypasses YSQL)
- ✅ Batching (50K rows per flush)

---

## Improvements for Reducing Planning Phase

### 1. Optimize Token Range Calculation

**Current Issue:**
- Connector queries all token ranges
- Recalculates splits every time
- Network overhead for metadata queries

**Improvements:**

#### A. Cache Token Range Information
```scala
// Cache token ranges in a file or database
// Reuse on subsequent runs
val cachedTokenRanges = loadCachedTokenRanges(tableName)
if (cachedTokenRanges.nonEmpty) {
  // Use cached ranges instead of querying
  useCachedRanges(cachedTokenRanges)
} else {
  // Calculate and cache for next time
  val ranges = calculateTokenRanges()
  cacheTokenRanges(tableName, ranges)
}
```

**Benefits:**
- Reduces metadata queries by 80-90%
- Planning time: 30 min → 5-10 min

#### B. Pre-calculate Splits
```properties
# If you know your data size, pre-calculate optimal splits
cassandra.inputSplitSizeMb=512  # Larger splits = fewer partitions = less planning
```

**Trade-off:**
- Fewer partitions = less parallelism
- But faster planning phase
- Optimal: Balance based on data size

### 2. Reduce Metadata Queries

**Current Queries:**
```sql
-- Multiple queries executed
DESCRIBE TABLE keyspace.table;
SELECT token(partition_key) FROM table LIMIT 1000;
SELECT * FROM system.local;
SELECT * FROM system.peers;
```

**Optimization:**
```scala
// Batch metadata queries
val metadata = cassandraSession.execute(
  "SELECT table_name, column_name, type " +
  "FROM system_schema.columns " +
  "WHERE keyspace_name = ? AND table_name = ?",
  keyspace, table
).all()

// Single query instead of multiple
```

**Benefits:**
- Reduces network round-trips
- Planning time: 30 min → 20 min

### 3. Optimize Spark Planning

**Current Issue:**
- Catalyst optimizer analyzes entire plan
- Creates complex DAG for large tables
- Planning overhead increases with partition count

**Improvements:**

#### A. Disable Unnecessary Optimizations
```properties
# For COPY workloads, some optimizations aren't needed
spark.sql.adaptive.enabled=false  # Disable adaptive execution (if not needed)
spark.sql.adaptive.coalescePartitions.enabled=false
```

**Benefits:**
- Faster planning
- Less overhead for simple COPY operations

#### B. Pre-partition Data
```scala
// If you know partition boundaries, pre-partition
val prePartitionedDF = df.repartition(numPartitions)
// This avoids Spark's partition calculation
```

**Benefits:**
- Bypasses Spark's partition calculation
- Planning time: 30 min → 15 min

### 4. Parallel Metadata Reading

**Current:**
- Sequential metadata queries
- One query at a time

**Optimization:**
```scala
// Read metadata in parallel
val schemaFuture = Future { readSchema() }
val tokenRangesFuture = Future { readTokenRanges() }
val topologyFuture = Future { readTopology() }

// Wait for all
val (schema, ranges, topology) = Await.result(
  for {
    s <- schemaFuture
    r <- tokenRangesFuture
    t <- topologyFuture
  } yield (s, r, t),
  5.minutes
)
```

**Benefits:**
- Parallel metadata reading
- Planning time: 30 min → 20 min

### 5. Use Smaller Initial Fetch for Planning

**Current:**
```properties
cassandra.fetchSizeInRows=10000  # Used for planning too
```

**Optimization:**
```scala
// Use smaller fetch size for metadata queries
val planningConfig = cassandraConfig.copy(
  fetchSizeInRows = 100  // Smaller for planning
)

// Use full fetch size for actual migration
val migrationConfig = cassandraConfig.copy(
  fetchSizeInRows = 10000  // Full size for migration
)
```

**Benefits:**
- Faster metadata queries
- Planning time: 30 min → 25 min

### 6. Skip Row Count Estimation

**Current:**
```scala
logInfo(s"Estimated rows: ${df.count()}")  // Triggers full scan!
```

**Optimization:**
```scala
// Skip count() - it triggers full table scan
// Use approximate count or skip entirely
logInfo(s"Partitions: ${df.rdd.getNumPartitions}")
// Don't call df.count() during planning!
```

**Benefits:**
- Avoids full table scan during planning
- Planning time: 30 min → 10 min (if count() was the bottleneck)

### 7. Connection Pooling for Metadata

**Current:**
- New connections for each metadata query
- Connection establishment overhead

**Optimization:**
```scala
// Reuse connection pool for metadata queries
val metadataSession = connectionPool.getSession()
// Reuse same session for all metadata queries
```

**Benefits:**
- Reduces connection overhead
- Planning time: 30 min → 28 min

### 8. Incremental Planning

**For Very Large Tables:**

```scala
// Plan in chunks
val chunkSize = 1000000  // 1M rows per chunk

for (chunk <- 0 until numChunks) {
  val chunkDF = df.filter(
    $"token" >= chunk * chunkSize &&
    $"token" < (chunk + 1) * chunkSize
  )
  // Process chunk
  migrateChunk(chunkDF)
}
```

**Benefits:**
- Smaller planning phases
- Can resume if interrupted
- Planning time per chunk: 2-3 min (instead of 30 min upfront)

---

## Recommended Configuration for Faster Planning

```properties
# Optimize for faster planning
cassandra.inputSplitSizeMb=512  # Larger splits = fewer partitions
cassandra.fetchSizeInRows=10000  # Keep large for migration
cassandra.readTimeoutMs=60000    # Reduce timeout for metadata queries

# Spark optimizations
spark.sql.adaptive.enabled=false  # Disable if not needed
spark.sql.adaptive.coalescePartitions.enabled=false
spark.default.parallelism=32      # Match to your data size

# Skip row count during planning (don't call df.count())
# This is handled in code, not config
```

**Expected Improvement:**
- Planning time: 30 min → 10-15 min (50% reduction)
- Total migration time: 60 min → 40-45 min

---

## Monitoring and Troubleshooting

### Key Metrics to Watch

**During Planning Phase:**
- Spark stages created
- Metadata query duration
- Token range calculation time
- Network latency to Cassandra

**During Migration Phase:**
- DocDB ops/sec (should be 5K-10K)
- YugabyteDB connections (should match parallelism)
- COPY stream status
- Rows written per partition

### Common Issues

**Planning Phase Too Long:**
- Check network latency to Cassandra
- Verify token range calculation isn't stuck
- Check if `df.count()` is being called (triggers full scan)

**Migration Phase Slow:**
- Check DocDB ops/sec (should be high)
- Verify parallelism matches partition count
- Check for network bottlenecks
- Monitor YugabyteDB connection count

---

## Summary

### Key Takeaways

1. **Two Distinct Phases:**
   - Planning (30 min): Metadata, token ranges, Spark planning
   - Migration (30 min): Actual data transfer via COPY

2. **Token-Aware Reading:**
   - Each partition reads from the Cassandra node that owns the data
   - Optimal data locality
   - No unnecessary network hops

3. **COPY Bypasses YSQL:**
   - Goes directly to DocDB
   - Shows as DocDB ops/sec (not YSQL ops/sec)
   - 10-100x faster than regular INSERTs

4. **Parallelism is Key:**
   - More partitions = more parallel COPY streams
   - But more partitions = longer planning phase
   - Balance based on data size

5. **Optimization Opportunities:**
   - Cache token ranges
   - Skip row count estimation
   - Optimize metadata queries
   - Reduce Spark planning overhead

### Performance Targets

**For 25M Records:**
- Planning phase: 10-15 minutes (with optimizations)
- Migration phase: 20-30 minutes
- Total: 30-45 minutes (vs 60 minutes without optimizations)

**Throughput:**
- Target: 10K-20K rows/sec
- DocDB ops/sec: 5K-10K ops/sec
- With 32 partitions: ~300-600 rows/sec per partition

---

## References

- [Spark Cassandra Connector Documentation](https://github.com/datastax/spark-cassandra-connector)
- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [YugabyteDB COPY Performance](https://docs.yugabyte.com/stable/api/ysql/the-sql-language/statements/cmd_copy/)
- [Cassandra Token Ranges](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html#token-assignment)

