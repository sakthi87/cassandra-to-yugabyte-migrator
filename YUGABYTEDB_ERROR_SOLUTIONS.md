# YugabyteDB Error Solutions for INSERT Mode

## Errors Encountered

### Error 1: Snapshot Too Old
```
ERROR: Snapshot too old. Read point: { physical: 1767980302650433 }, 
earliest read time allowed: { physical: 1767980428830046 }, 
delta (usec): 126179613: kSnapshotTooOld
```

**Cause:** Transactions are taking too long, causing the read snapshot to expire.

### Error 2: Serialization Conflict
```
ERROR: could not serialize access due to concurrent update
(consider increasing the tserver gflag ysql_output_buffer_size)
```

**Cause:** Multiple concurrent transactions trying to update the same data simultaneously.

## Solutions

### Solution 1: Reduce Batch Size (IMMEDIATE FIX)

**Problem:** Batch size of 5000 is too large, causing:
- Long-running transactions (snapshot too old)
- High concurrency conflicts (serialization errors)

**Fix:** Reduce batch size significantly:

```properties
# In migration.properties, change from:
yugabyte.insertBatchSize=5000

# To:
yugabyte.insertBatchSize=500
```

**Recommended batch sizes:**
- For high-concurrency environments: 100-500
- For stable environments: 500-1000
- For 109M records with concurrency issues: Start with 200-300

### Solution 2: Reduce Spark Parallelism

**Problem:** Too many concurrent partitions causing serialization conflicts.

**Fix:** Reduce parallelism:

```properties
# Reduce from current value to:
spark.default.parallelism=50
spark.sql.shuffle.partitions=50
spark.executor.instances=2
spark.executor.cores=4
```

This reduces concurrent INSERT operations.

### Solution 3: Increase YugabyteDB TServer Flag (Requires DB Admin)

**Error message suggests:**
```
consider increasing the tserver gflag ysql_output_buffer_size
```

**Fix (requires YugabyteDB admin access):**
```bash
# On YugabyteDB nodes, increase ysql_output_buffer_size
# Default is usually 256KB, increase to 1MB or 2MB
```

**Note:** This requires YugabyteDB cluster configuration access.

### Solution 4: Reduce Transaction Duration

**Fix:** Commit more frequently by reducing batch size:

```properties
# Smaller batches = shorter transactions = less snapshot expiration
yugabyte.insertBatchSize=200
```

### Solution 5: Use READ COMMITTED Isolation (if possible)

**Current:** May be using REPEATABLE READ or SERIALIZABLE

**Fix:** Ensure using READ_COMMITTED:

```properties
yugabyte.isolationLevel=READ_COMMITTED
```

## Immediate Action Plan

### Step 1: Update Properties File (Quick Fix)

```properties
# Reduce batch size (critical)
yugabyte.insertBatchSize=300

# Reduce parallelism (if possible)
spark.default.parallelism=50

# Ensure READ_COMMITTED
yugabyte.isolationLevel=READ_COMMITTED
```

### Step 2: Verify JAR Has INSERT Mode Code

**Issue:** If JAR doesn't have INSERT mode code, you'll see different errors.

**Check:** Look for these log patterns:
- ✅ Good: "Partition X completed (INSERT mode)"
- ❌ Bad: "Partition X completed (COPY mode)" or no mode specified

**If JAR doesn't have INSERT mode:**
- The JAR in GitHub might have been built before INSERT mode was added
- Need to check when JAR was last built
- May need to build JAR locally and transfer it

### Step 3: Monitor and Adjust

After applying fixes:
1. Monitor for "snapshot too old" errors
2. If still occurring, reduce batch size further (100-200)
3. Monitor for serialization conflicts
4. If still occurring, reduce parallelism further

## Recommended Configuration for 109M Records

```properties
# INSERT Mode
yugabyte.insertMode=INSERT
yugabyte.insertBatchSize=300

# Reduced Parallelism
spark.default.parallelism=50
spark.sql.shuffle.partitions=50
spark.executor.instances=2
spark.executor.cores=4

# Isolation Level
yugabyte.isolationLevel=READ_COMMITTED

# Connection Settings
yugabyte.maxPoolSize=8
yugabyte.minIdle=2
```

## Why Batch Size Matters

**Large batch size (5000):**
- ✅ Fewer network round-trips
- ❌ Longer transactions
- ❌ Higher risk of snapshot expiration
- ❌ More serialization conflicts

**Small batch size (200-500):**
- ✅ Shorter transactions
- ✅ Less snapshot expiration risk
- ✅ Fewer serialization conflicts
- ⚠️ More network round-trips (acceptable trade-off)

## Next Steps

1. ✅ Update properties file with reduced batch size (300)
2. ✅ Reduce parallelism if possible
3. ✅ Verify READ_COMMITTED isolation level
4. ✅ Re-run migration
5. ✅ Monitor logs for improvements
6. ⚠️ If errors persist, reduce batch size further (100-200)

