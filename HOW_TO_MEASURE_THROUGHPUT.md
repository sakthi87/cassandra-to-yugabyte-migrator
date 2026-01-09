# How to Measure INSERT Mode Throughput

## Understanding Metrics

### YBA UI: "Total YSQL Ops/Sec" vs Actual Rows/Sec

**Important Distinction:**
- **"Total YSQL Ops/Sec"** in YBA UI = Number of SQL operations per second
- **Rows/Sec (throughput)** = Number of rows inserted per second

**Relationship:**
- With batch size 300: **1 YSQL operation = up to 300 rows** (if batch is full)
- So: **YSQL Ops/Sec × batch size = approximate rows/sec**
- Example: 50 YSQL Ops/Sec × 300 rows/batch = ~15,000 rows/sec

---

## Where to Find Throughput Metrics

### 1. **Migration Logs (Most Accurate)**

The migration logs show the actual throughput in rows/sec:

**Look for this in your log file:**
```
Migration Metrics:
  Rows Read: 100000
  Rows Written: 100000
  Partitions Completed: 34
  Elapsed Time: 17 seconds
  Throughput: 5882.35 rows/sec
```

**How to find it:**
```bash
# Search for "Throughput" in log file
grep -i "Throughput" your-migration-log.log

# Or search for "Migration Summary"
grep -A 10 "Migration Summary" your-migration-log.log
```

**This is the most accurate measurement** - it's calculated from total rows processed divided by elapsed time.

---

### 2. **YBA UI: YSQL Tab**

**Location:** YugabyteDB Anywhere UI → Metrics → YSQL Tab

**Metric Name:** "Total YSQL Ops/Sec"

**What it shows:**
- Number of YSQL operations per second
- Includes INSERT operations (batched)
- **NOT** the number of rows per second

**How to interpret:**
```
Total YSQL Ops/Sec = 50 ops/sec
Batch Size = 300 rows/batch

Estimated Rows/Sec = 50 × 300 = 15,000 rows/sec
```

**Note:** This is an estimate because:
- Not all batches are full (300 rows)
- Some batches may be smaller (last batch in partition)
- Includes other YSQL operations (checkpoint writes, etc.)

---

### 3. **YBA UI: Detailed Metrics**

**Location:** YBA UI → Metrics → YSQL Tab → Detailed Metrics

**Look for:**
- **"INSERT Ops/Sec"** - More specific than "Total YSQL Ops/Sec"
- **"Transaction Rate"** - Shows transaction throughput

**Example:**
```
INSERT Ops/Sec: 45 ops/sec
Batch Size: 300 rows/batch
Estimated: 45 × 300 = 13,500 rows/sec
```

---

## How to Calculate Actual Throughput

### Method 1: From Migration Logs (Best)

**At the end of migration:**
```
Rows Written: 109,000,000
Elapsed Time: 7200 seconds (2 hours)

Throughput = 109,000,000 / 7200 = 15,139 rows/sec
```

### Method 2: From YBA UI (Estimate)

**During migration:**
```
Total YSQL Ops/Sec: 50 ops/sec
Batch Size: 300 rows/batch

Estimated Throughput = 50 × 300 = 15,000 rows/sec
```

**Accuracy:** ±10-20% (depending on batch fill rate)

### Method 3: Real-time Calculation from Logs

**Monitor partition completion:**
```
Partition 0 completed (INSERT mode): 50000 rows processed, 50000 rows inserted, 0 duplicates skipped
Partition 1 completed (INSERT mode): 52000 rows processed, 52000 rows inserted, 0 duplicates skipped
```

**Calculate:**
- Count rows processed over time window
- Divide by time elapsed

---

## Expected Values for Batch Size 300

### YBA UI Metrics

**"Total YSQL Ops/Sec":**
- Expected: **50-75 ops/sec** (for 15K-22K rows/sec)
- Calculation: 15,000 rows/sec ÷ 300 rows/batch = 50 ops/sec
- Calculation: 22,000 rows/sec ÷ 300 rows/batch = 73 ops/sec

**If you see:**
- ✅ **50-75 ops/sec** → Good throughput (15K-22K rows/sec)
- ✅ **40-50 ops/sec** → Decent throughput (12K-15K rows/sec)
- ⚠️ **< 40 ops/sec** → Lower throughput (< 12K rows/sec)

### Migration Log Throughput

**Expected in logs:**
```
Throughput: 15,000-22,000 rows/sec
```

**Range depends on:**
- Network latency
- YugabyteDB cluster performance
- Row size
- Concurrent partitions

---

## How to Monitor During Migration

### Step 1: Check YBA UI (Real-time)

1. Go to YBA UI → Metrics → YSQL Tab
2. Look for **"Total YSQL Ops/Sec"**
3. Expected: **50-75 ops/sec** (for batch size 300)
4. This gives you real-time feedback

### Step 2: Check Migration Logs (Periodic)

1. Search log file for partition completions:
   ```bash
   grep "completed (INSERT mode)" migration.log | tail -20
   ```
2. Count rows processed in recent partitions
3. Calculate approximate throughput

### Step 3: Final Verification (After Migration)

1. Check migration summary in logs:
   ```bash
   grep -A 10 "Migration Summary" migration.log
   ```
2. Look for **"Throughput: X rows/sec"**
3. This is the most accurate measurement

---

## Understanding the Numbers

### Example: Batch Size 300, Throughput 15,000 rows/sec

**YBA UI shows:**
- Total YSQL Ops/Sec: **50 ops/sec**
- (50 ops/sec × 300 rows/batch = 15,000 rows/sec)

**Migration log shows:**
- Throughput: **15,000 rows/sec**

**Both match!** ✅

### Why YSQL Ops/Sec is Lower Than Rows/Sec

**Batch Insert:**
```
INSERT INTO table VALUES 
  (row1), (row2), ..., (row300);  -- 1 YSQL operation, 300 rows
```

So:
- **1 YSQL operation** = **300 rows** (with batch size 300)
- **50 YSQL ops/sec** = **15,000 rows/sec**

---

## Troubleshooting Low Throughput

### If YSQL Ops/Sec is Low (< 40 ops/sec)

**Check:**
1. ✅ Are partitions completing? (check logs)
2. ✅ Any errors in logs? (search for "ERROR")
3. ✅ Network latency? (check YugabyteDB connection)
4. ✅ Cluster performance? (check CPU/memory in YBA UI)

### If YSQL Ops/Sec is High (> 100 ops/sec)

**Possible causes:**
1. ✅ Smaller batches (not all batches are 300 rows)
2. ✅ Other YSQL operations (checkpoint writes, etc.)
3. ✅ Multiple tables being migrated

---

## Summary

| Metric | Location | What It Shows | Expected Value (Batch 300) |
|--------|----------|---------------|---------------------------|
| **Rows/Sec** | Migration Logs | Actual throughput | 15,000-22,000 rows/sec |
| **Total YSQL Ops/Sec** | YBA UI → YSQL Tab | SQL operations/sec | 50-75 ops/sec |
| **INSERT Ops/Sec** | YBA UI → YSQL Tab (detailed) | INSERT operations/sec | 45-70 ops/sec |

**Key Points:**
- ✅ **YBA UI "Total YSQL Ops/Sec"** × batch size = approximate rows/sec
- ✅ **Migration logs "Throughput"** = most accurate rows/sec
- ✅ **Expected: 50-75 YSQL ops/sec** for batch size 300 = 15K-22K rows/sec

**To verify 15K-22K rows/sec:**
1. Check YBA UI: Should see **50-75 ops/sec** in "Total YSQL Ops/Sec"
2. Check logs: Should see **15,000-22,000 rows/sec** in migration summary
3. Calculate: YSQL Ops/Sec × 300 = estimated rows/sec

