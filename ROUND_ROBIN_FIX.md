# Round-Robin Load Balancing Fix

## Problem Identified

The round-robin load balancing was **NOT working** because each Spark partition creates a **NEW** `YugabyteConnectionFactory` instance, and each factory had its own `AtomicInteger` counter starting at 0.

### Why It Failed

```scala
// TableMigrationJob.scala:131
localDf.foreachPartition { (partition: Iterator[Row]) =>
  // NEW factory created for EACH partition
  val localConnectionFactory = new YugabyteConnectionFactory(localYugabyteConfig)
  // Each factory has: private val connectionCounter = new AtomicInteger(0)
  
  // PartitionExecutor.scala:72 also creates a new factory!
  val localConnectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
  conn = Some(localConnectionFactory.getConnection())  // Counter = 0 for ALL partitions!
}
```

**Result:** All partitions had counter=0, so ALL selected host[0] (first node).

| Partition | Factory Instance | Counter Value | Selected Host |
|-----------|------------------|---------------|---------------|
| 0 | NEW factory #1 | 0 | host[0] |
| 1 | NEW factory #2 | 0 | host[0] ❌ |
| 2 | NEW factory #3 | 0 | host[0] ❌ |
| 3 | NEW factory #4 | 0 | host[0] ❌ |

## Solution: Use Partition ID Directly

Instead of a shared counter (which doesn't work when each partition creates its own factory), use the **partition ID** directly for host selection.

### Code Changes

**1. Updated `YugabyteConnectionFactory.getConnection()`:**

```scala
// BEFORE (didn't work):
private val connectionCounter = new AtomicInteger(0)

def getConnection(): Connection = {
  val hostIndex = connectionCounter.getAndIncrement() % hosts.length
  // ❌ All partitions had counter=0
}

// AFTER (works!):
def getConnection(partitionId: Int = 0): Connection = {
  val hostIndex = partitionId % hosts.length
  // ✅ Partition 0 → host[0], Partition 1 → host[1], etc.
  val selectedHost = hosts(hostIndex)
  // ...
}
```

**2. Updated `PartitionExecutor.execute()`:**

```scala
// Get partition ID from Spark context
val actualPartitionId = TaskContext.getPartitionId()

// Pass partition ID to getConnection()
conn = Some(localConnectionFactory.getConnection(actualPartitionId))
```

### How It Works Now

| Partition ID | Calculation | Selected Host |
|--------------|-------------|---------------|
| 0 | 0 % 3 = 0 | host[0] (node1) |
| 1 | 1 % 3 = 1 | host[1] (node2) |
| 2 | 2 % 3 = 2 | host[2] (node3) |
| 3 | 3 % 3 = 0 | host[0] (node1) |
| 4 | 4 % 3 = 1 | host[1] (node2) |
| 5 | 5 % 3 = 2 | host[2] (node3) |

**For 10 partitions and 3 nodes:**
- Node1 (host[0]): Partitions 0, 3, 6, 9 = 4 partitions
- Node2 (host[1]): Partitions 1, 4, 7 = 3 partitions  
- Node3 (host[2]): Partitions 2, 5, 8 = 3 partitions

Distribution: **4-3-3** (very close to even!)

## Benefits of This Approach

1. ✅ **No shared state** - works perfectly in Spark's distributed environment
2. ✅ **Deterministic** - same partition ID always selects same host
3. ✅ **Thread-safe** - no synchronization needed
4. ✅ **Even distribution** - partitions distributed across all nodes
5. ✅ **Simple** - easy to understand and debug

## Expected Results

**Before Fix:**
- Node1: 90% CPU (all partitions)
- Node2: 30% CPU
- Node3: 30% CPU

**After Fix:**
- Node1: ~33-40% CPU (depending on partition count)
- Node2: ~30-35% CPU
- Node3: ~30-35% CPU

CPU should be much more balanced!

## Verification

To verify the fix is working:

1. **Check logs for host selection:**
   ```
   Connecting to YugabyteDB host node1 (1/3) for partition 0
   Connecting to YugabyteDB host node2 (2/3) for partition 1
   Connecting to YugabyteDB host node3 (3/3) for partition 2
   Connecting to YugabyteDB host node1 (1/3) for partition 3
   Connecting to YugabyteDB host node2 (2/3) for partition 4
   ...
   ```

2. **Check CPU usage in YugabyteDB Anywhere:**
   - Should see balanced CPU across all nodes
   - No single node at 90% CPU

3. **Check connection distribution:**
   ```sql
   SELECT 
     application_name,
     COUNT(*) as connection_count
   FROM pg_stat_activity
   WHERE application_name LIKE '%Migration%'
   GROUP BY application_name;
   ```

## Files Changed

1. `src/main/scala/com/company/migration/yugabyte/YugabyteConnectionFactory.scala`
   - Changed `getConnection()` to accept `partitionId` parameter
   - Removed `AtomicInteger connectionCounter`
   - Use `partitionId % hosts.length` for host selection

2. `src/main/scala/com/company/migration/execution/PartitionExecutor.scala`
   - Pass `actualPartitionId` to `getConnection(actualPartitionId)`

## Backward Compatibility

- `getConnection()` still works (defaults to partitionId=0)
- Non-partition code (CheckpointManager, MainApp) continues to work
- Only partition execution uses partition ID for load balancing

