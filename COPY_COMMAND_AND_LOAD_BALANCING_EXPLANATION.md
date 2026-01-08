# COPY Command Execution and Load Balancing Explanation

## Question: When Spark creates 10 partitions, does it create multiple COPY commands or a single COPY command?

**Answer: Multiple COPY commands - one per Spark partition.**

## How It Works: End-to-End Process

### 1. Spark Partition → Connection → COPY Command Mapping

```
Spark DataFrame (10 partitions)
├─ Partition 0 → Connection 0 → COPY command 0
├─ Partition 1 → Connection 1 → COPY command 1
├─ Partition 2 → Connection 2 → COPY command 2
├─ ...
└─ Partition 9 → Connection 9 → COPY command 9
```

**Key Point:** Each Spark partition gets its **own connection** and its **own COPY command**.

### 2. Code Flow

#### Step 1: Spark DataFrame is partitioned
```scala
// TableMigrationJob.scala
val df = reader.readTable(tableConfig)  // Creates DataFrame with N partitions
```

#### Step 2: Each partition processes independently
```scala
// TableMigrationJob.scala:120
df.foreachPartition { (partition: Iterator[Row]) =>
  val partitionId = TaskContext.getPartitionId()
  
  // Create NEW connection factory per partition
  val localConnectionFactory = new YugabyteConnectionFactory(localYugabyteConfig)
  
  // Get NEW connection for this partition
  val conn = localConnectionFactory.getConnection()
  
  // Create COPY writer
  val copyWriter = new CopyWriter(connection, copySql, ...)
  
  // Start COPY command
  copyWriter.start()  // ← This executes: COPY table FROM STDIN
}
```

#### Step 3: Each partition creates its own connection
```scala
// YugabyteConnectionFactory.scala:67
def getConnection(): Connection = {
  // Creates a NEW connection using DriverManager
  val conn = DriverManager.getConnection(jdbcUrl, props)
  // ...
}
```

#### Step 4: Each connection starts its own COPY command
```scala
// CopyWriter.scala:62
def start(): Unit = {
  copyIn = Some(copyManager.copyIn(copySql))
  // ↑ This executes: COPY schema.table (col1, col2, ...) FROM STDIN WITH (FORMAT csv)
}
```

## Example: 10 Partitions = 10 COPY Commands

If Spark creates **10 partitions**, you will have:

```
Partition 0:
  Connection → jdbc:yugabytedb://node1:5433,node2:5433,node3:5433/db?loadBalance=true
  COPY command → COPY public.table (col1, col2, ...) FROM STDIN WITH (FORMAT csv)
  
Partition 1:
  Connection → jdbc:yugabytedb://node1:5433,node2:5433,node3:5433/db?loadBalance=true
  COPY command → COPY public.table (col1, col2, ...) FROM STDIN WITH (FORMAT csv)
  
Partition 2:
  Connection → jdbc:yugabytedb://node1:5433,node2:5433,node3:5433/db?loadBalance=true
  COPY command → COPY public.table (col1, col2, ...) FROM STDIN WITH (FORMAT csv)
  
... (and so on for all 10 partitions)
```

Each partition:
1. Opens its own JDBC connection
2. Starts its own COPY FROM STDIN operation
3. Streams data independently
4. Commits independently

## The Load Balancing Problem

### Issue: First Node Gets 90% CPU, Others Get 30%

**Observation:** 
- ✅ **LSM DB SEEK metrics show equal data distribution** across all 3 nodes (YugabyteDB Anywhere)
- ❌ **CPU usage is imbalanced:** Node1: 90%, Node2: 30%, Node3: 30%

**Root Cause:** Even though you provide multiple hosts in the JDBC URL and set `loadBalance=true`, **`DriverManager.getConnection()` does NOT distribute connections across nodes**. All COPY connections go to Node1, which then handles:
1. Receiving all COPY FROM STDIN streams
2. Writing to its local partitions
3. Coordinating cross-node writes to Node2 and Node3 partitions (network overhead)
4. Managing transaction coordination

This explains why:
- **Data distribution is balanced** (YugabyteDB internally distributes data evenly across nodes)
- **CPU is imbalanced** (Node1 does all the COPY coordination work, even though data ends up on all nodes)

### How JDBC URL with Multiple Hosts Works

When you configure:
```properties
yugabyte.host=node1,node2,node3
yugabyte.loadBalanceHosts=true
```

The code builds this JDBC URL:
```
jdbc:yugabytedb://node1:5433,node2:5433,node3:5433/database?loadBalance=true&...
```

### The Problem: DriverManager.getConnection() Behavior

**Critical Finding:** `DriverManager.getConnection()` with multiple hosts in the URL:

1. **May connect to the first host only** (node1 in your case)
2. **May not distribute connections evenly** across nodes
3. **Load balancing (`loadBalance=true`) works better with connection pools**, not direct DriverManager calls

### Why This Happens

The YugabyteDB JDBC driver's load balancing (`loadBalance=true`):

- **Works well with connection pools** (HikariCP, etc.) where the pool manages connection distribution
- **Does NOT work reliably with direct `DriverManager.getConnection()` calls** because:
  - Each call is independent
  - No coordination between connections
  - Driver may use first available/primary node
  - Round-robin logic might not be applied consistently

### Evidence in Your Code

Looking at `YugabyteConnectionFactory.scala`:

```scala
def getConnection(): Connection = {
  // ...
  val conn = DriverManager.getConnection(jdbcUrl, props)  // ← Direct call, no pool
  // ...
}
```

**Each partition calls this directly**, and each call may connect to the **same node** (node1).

## Solution: Use Connection Pool with Load Balancing

### Option 1: Use HikariCP Connection Pool (Recommended)

**Current implementation uses `DriverManager.getConnection()` per partition.**

**Better approach:** Use a connection pool that distributes connections across nodes.

However, there's a **trade-off** for COPY operations:

- **COPY FROM STDIN requires long-lived connections** (minutes per connection)
- Connection pools are designed for short-lived connections
- Pooling COPY connections can cause issues (connections evicted mid-COPY)

### Option 2: Round-Robin Connection Selection (Manual Load Balancing)

**Alternative approach:** Manually distribute connections across nodes.

```scala
class YugabyteConnectionFactory(yugabyteConfig: YugabyteConfig) {
  private val hosts = yugabyteConfig.hosts  // ["node1", "node2", "node3"]
  private val port = yugabyteConfig.port
  private var connectionCounter = new AtomicInteger(0)
  
  def getConnection(): Connection = {
    // Round-robin host selection
    val hostIndex = connectionCounter.getAndIncrement() % hosts.length
    val selectedHost = hosts(hostIndex)
    
    // Connect to specific host (not all hosts in URL)
    val jdbcUrl = s"jdbc:yugabytedb://$selectedHost:$port/${yugabyteConfig.database}?..."
    DriverManager.getConnection(jdbcUrl, props)
  }
}
```

**Pros:**
- Simple to implement
- Guarantees even distribution
- Works with COPY (one connection per partition)

**Cons:**
- Manual load balancing (not automatic failover)
- Less resilient to node failures
- Need to handle reconnection logic

### Option 3: Use YugabyteDB Smart Driver with Connection Pool (Best for Production)

For production environments, consider:

1. **Use YugabyteDB Smart Driver** (YBClusterAwareDataSource)
2. **Use HikariCP connection pool** (manages connection distribution)
3. **Set `loadBalance=true` and `topologyKeys`** (if multi-region)

**Note:** This is more complex and requires refactoring the current COPY-based approach.

## Current Behavior Summary

| Aspect | Current Implementation |
|--------|----------------------|
| **Partitions** | 10 partitions → 10 connections → 10 COPY commands |
| **Connection Method** | `DriverManager.getConnection()` per partition |
| **JDBC URL** | `jdbc:yugabytedb://node1,node2,node3/db?loadBalance=true` |
| **Load Balancing** | ❌ **NOT WORKING** - all connections go to first node |
| **Data Distribution (LSM DB SEEK)** | ✅ **BALANCED** - Data distributed evenly across all nodes |
| **CPU Distribution** | Node1: 90%, Node2: 30%, Node3: 30% |
| **Reason** | DriverManager doesn't distribute connections evenly. Node1 handles all COPY streams and coordinates cross-node writes, even though data is distributed to all nodes |

## Quick Fix: Round-Robin Host Selection

Here's a simple fix you can implement:

```scala
// YugabyteConfig.scala - Add hosts list
case class YugabyteConfig(
  // ... existing fields ...
  hosts: List[String],  // Add this
  // ...
)

// YugabyteConnectionFactory.scala - Add round-robin selection
class YugabyteConnectionFactory(yugabyteConfig: YugabyteConfig) {
  private val hosts = yugabyteConfig.hosts
  private val port = yugabyteConfig.port
  private val connectionCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  
  def getConnection(): Connection = {
    // Round-robin host selection
    val hostIndex = connectionCounter.getAndIncrement() % hosts.length
    val selectedHost = hosts(hostIndex)
    
    // Connect to specific host
    val jdbcUrl = s"jdbc:yugabytedb://$selectedHost:$port/${yugabyteConfig.database}?..."
    DriverManager.getConnection(jdbcUrl, props)
  }
}
```

**Expected result:**
- 10 partitions → connections distributed across 3 nodes
- Node1: ~33% of connections (3-4 partitions)
- Node2: ~33% of connections (3-4 partitions)
- Node3: ~33% of connections (3-4 partitions)
- CPU should be more balanced

## Verification

After implementing the fix, check connection distribution:

```sql
-- In YugabyteDB, check active connections per node
SELECT 
  host,
  COUNT(*) as connection_count,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM pg_stat_activity
WHERE application_name LIKE '%Migration%'
GROUP BY host
ORDER BY host;
```

You should see connections distributed across all nodes instead of concentrated on node1.

## Summary

1. **10 partitions = 10 COPY commands** (one per partition)
2. **Each partition creates its own connection**
3. **Problem:** `DriverManager.getConnection()` doesn't distribute connections evenly
4. **Key Insight:** 
   - ✅ Data distribution is balanced (LSM DB SEEK shows equal splits across nodes)
   - ❌ CPU is imbalanced (Node1: 90%, Others: 30%)
   - **Why:** All COPY connections hit Node1, which coordinates all writes (including cross-node)
5. **Solution:** Implement round-robin host selection to distribute COPY connections across nodes
6. **Expected Result After Fix:**
   - Data distribution: Still balanced (YugabyteDB handles this)
   - CPU distribution: Should be ~33% per node (each node handles ~1/3 of COPY connections)

The load imbalance (90% CPU on node1) is because all connections are going to the first host. Node1 receives all COPY streams and coordinates writes to all nodes, creating CPU hotspot. Even though YugabyteDB distributes the data evenly (hence balanced LSM DB SEEK metrics), the COPY coordination overhead is concentrated on Node1. Distributing connections across nodes will distribute this coordination overhead.

