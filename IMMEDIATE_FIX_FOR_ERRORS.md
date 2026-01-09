# Immediate Fix for YugabyteDB Errors

## ✅ GOOD NEWS: INSERT Mode IS Working!

The errors you're seeing confirm that **INSERT mode is being used**:
- Error: "Failed to flush INSERT batch" → This is INSERT mode!
- Error occurs in `InsertBatchWriter.flush()` → INSERT mode code path

If COPY mode was being used, you'd see different errors.

## ❌ The Problem: Batch Size Too Large

**Your current setting:**
```properties
yugabyte.insertBatchSize=5000
```

**This causes:**
1. **"Snapshot too old" errors** - Transactions take too long (>126 seconds)
2. **Serialization conflicts** - Too many concurrent long-running transactions

## ✅ Immediate Fix

### Update Your Properties File:

```properties
# Change from:
yugabyte.insertBatchSize=5000

# To:
yugabyte.insertBatchSize=300
```

This has been updated in GitHub. Pull the latest properties file, or manually change this one line.

## Why 300?

- ✅ Short transactions (avoids snapshot expiration)
- ✅ Fewer serialization conflicts
- ✅ Better for high-concurrency environments
- ✅ Still efficient (300 rows per batch)

## Additional Recommendations

If errors persist after reducing batch size, also reduce parallelism:

```properties
# Reduce parallelism to reduce concurrent transactions
spark.default.parallelism=50
spark.sql.shuffle.partitions=50
spark.executor.instances=2
spark.executor.cores=4
```

## Verification

After applying the fix, you should see:
- ✅ No more "Snapshot too old" errors
- ✅ No more serialization conflicts
- ✅ INSERT mode statements in logs: "Partition X completed (INSERT mode)"
- ✅ Successful data loading

## Summary

1. ✅ INSERT mode IS working (errors confirm it)
2. ❌ Batch size 5000 is too large
3. ✅ Fix: Change to `yugabyte.insertBatchSize=300`
4. ✅ Pull updated properties file from GitHub or manually change

