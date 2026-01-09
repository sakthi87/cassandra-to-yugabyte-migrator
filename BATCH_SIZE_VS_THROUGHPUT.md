# Batch Size vs Throughput Trade-off

## Quick Answer: **No, 300 will likely have LOWER throughput than 5000**

However, **300 is necessary to avoid errors**. We're trading throughput for stability.

---

## Batch Size Impact on Throughput

### Larger Batch Size (5000) - **Higher Throughput BUT Causes Errors**
- ✅ **Fewer network round-trips** (5,000 rows per batch = fewer batches)
- ✅ **Better throughput** (less overhead)
- ❌ **Long transactions** → "Snapshot too old" errors
- ❌ **Serialization conflicts** → Transaction failures
- ❌ **Unstable** → Frequent failures

**Result:** High throughput when it works, but frequent failures make it unusable.

### Smaller Batch Size (300) - **Lower Throughput BUT Stable**
- ✅ **Short transactions** → No "snapshot too old" errors
- ✅ **Fewer conflicts** → Stable operation
- ✅ **Error-free** → Reliable migration
- ❌ **More network round-trips** (300 rows per batch = more batches)
- ❌ **Lower throughput** (more overhead per batch)

**Result:** Lower but stable throughput.

---

## Throughput Comparison

| Batch Size | Expected Throughput | Stability | Status |
|------------|---------------------|-----------|--------|
| **5000** | ~25K-35K rows/sec | ❌ Unstable (errors) | **Unusable** |
| **1000** | ~20K-30K rows/sec | ✅ Stable | **Good** |
| **500** | ~18K-25K rows/sec | ✅ Stable | **Good** |
| **300** | ~15K-22K rows/sec | ✅ Very Stable | **Safe** |
| **100** | ~10K-15K rows/sec | ✅ Very Stable | **Very Safe (slow)** |

**Note:** These are estimates. Actual throughput depends on:
- Network latency
- YugabyteDB cluster performance
- Row size
- Concurrent partitions

---

## Finding the Optimal Batch Size

**Strategy:** Start with 300 (safe), then increase gradually if no errors.

### Step 1: Start with 300 (Current)
```properties
yugabyte.insertBatchSize=300
```
- ✅ Guaranteed to work (no errors)
- ✅ Stable operation
- ⚠️ Lower throughput (~15K-22K rows/sec)

### Step 2: If 300 works without errors, try 500
```properties
yugabyte.insertBatchSize=500
```
- ✅ Better throughput (~18K-25K rows/sec)
- ✅ Still stable (if no errors occur)
- ⚠️ Monitor for errors

### Step 3: If 500 works, try 1000
```properties
yugabyte.insertBatchSize=1000
```
- ✅ Even better throughput (~20K-30K rows/sec)
- ⚠️ Monitor closely for errors
- ⚠️ If errors occur, reduce back to 500

### Step 4: If 1000 works, try 2000 (careful!)
```properties
yugabyte.insertBatchSize=2000
```
- ✅ Higher throughput
- ⚠️ Higher risk of errors
- ⚠️ Monitor very closely

---

## Recommended Approach for 109M Records

### Option 1: Conservative (Recommended)
```properties
yugabyte.insertBatchSize=500
```
- Good balance of throughput and stability
- ~18K-25K rows/sec
- Estimated time: ~1.5-2 hours for 109M records

### Option 2: Balanced
```properties
yugabyte.insertBatchSize=1000
```
- Better throughput
- ~20K-30K rows/sec
- Estimated time: ~1-1.5 hours for 109M records
- Monitor for errors

### Option 3: Safe Start (Current)
```properties
yugabyte.insertBatchSize=300
```
- Very stable, error-free
- ~15K-22K rows/sec
- Estimated time: ~1.5-2.5 hours for 109M records

---

## Why We Can't Use 5000

**5000 causes:**
- Transactions >126 seconds → "Snapshot too old"
- High concurrency conflicts → Serialization errors
- **Unusable** → Migration fails repeatedly

**Result:** Zero throughput (fails) vs lower but stable throughput (300).

---

## Summary

| Question | Answer |
|----------|--------|
| **Does 300 increase throughput?** | ❌ No, it decreases throughput |
| **Why use 300 then?** | ✅ To avoid errors (5000 doesn't work) |
| **Can we use larger batches?** | ✅ Yes, try 500-1000 if 300 works |
| **What's the trade-off?** | Throughput vs Stability |

**Recommendation:**
1. ✅ Start with 300 (guaranteed to work)
2. ✅ If stable, increase to 500-1000 for better throughput
3. ✅ Monitor for errors and adjust accordingly

**Bottom Line:** 300 is slower but stable. 5000 is faster but unusable (errors). Find the sweet spot (500-1000) that balances throughput and stability.

