# Round-Robin Load Balancing Issue Analysis

## Problem Identified

The round-robin load balancing is **NOT working** because:

1. **New factory created per partition**: Each Spark partition creates a NEW `YugabyteConnectionFactory` instance
2. **Each factory has its own counter**: Each factory has `private val connectionCounter = new AtomicInteger(0)`
3. **All counters start at 0**: Every partition's factory starts with counter=0
4. **Result**: All partitions select the first host (host[0])

## Current Code Flow

```scala
// TableMigrationJob.scala:131
localDf.foreachPartition { (partition: Iterator[Row]) =>
  // NEW factory created for EACH partition
  val localConnectionFactory = new YugabyteConnectionFactory(localYugabyteConfig)
  // Each factory has: private val connectionCounter = new AtomicInteger(0)
  // Counter starts at 0 for EVERY partition!
  
  val conn = localConnectionFactory.getConnection()
  // All partitions call getConnection() with counter=0
  // Result: hostIndex = 0 % 3 = 0 → all select host[0]
}
```

## Why It Doesn't Work

| Partition | Factory Created | Counter Value | Selected Host |
|-----------|----------------|---------------|---------------|
| 0 | NEW factory | 0 | host[0] (0 % 3 = 0) |
| 1 | NEW factory | 0 | host[0] (0 % 3 = 0) ❌ |
| 2 | NEW factory | 0 | host[0] (0 % 3 = 0) ❌ |
| 3 | NEW factory | 0 | host[0] (0 % 3 = 0) ❌ |

**All partitions select host[0]!**

## Solution: Use Partition ID for Host Selection

Instead of a shared counter (which is complex in Spark), use the **partition ID** directly:

```scala
def getConnection(partitionId: Int): Connection = {
  val hostIndex = partitionId % hosts.length
  val selectedHost = hosts(hostIndex)
  // Connect to selectedHost
}
```

This is:
- ✅ Deterministic (partition 0 → host[0], partition 1 → host[1], etc.)
- ✅ No shared state needed (works perfectly in Spark)
- ✅ Evenly distributed (if you have 10 partitions and 3 hosts: 4,3,3 distribution)
- ✅ Thread-safe (no synchronization needed)

