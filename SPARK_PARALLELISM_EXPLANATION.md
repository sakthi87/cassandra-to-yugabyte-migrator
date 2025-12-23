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

**Why It Works**:
- More parallelism = More concurrent connections
- More connections = More concurrent COPY streams
- More streams = Better latency hiding
- Better latency hiding = Higher throughput

**For Your Case**:
- Current: 16 parallelism → 16 connections → 3.3K records/sec
- Optimized: 32 parallelism → 32 connections → 6-7K records/sec (expected)

**Important**: Each connection is independent and processes one partition. More partitions = more connections = more concurrent work.

