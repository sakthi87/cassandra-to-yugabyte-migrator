# Diagnose Low Throughput (500 rows/sec vs Expected 15K-22K)

## Problem: Getting 500 rows/sec instead of 15K-22K rows/sec

**Expected:** 15K-22K rows/sec  
**Actual:** 500 rows/sec  
**Gap:** ~3% of expected (97% performance loss)

---

## Diagnostic Checklist

### 1. Check Migration Logs for Errors (CRITICAL)

**Search for errors:**
```bash
# Check for errors in log file
grep -i "ERROR\|FAILED\|Exception\|snapshot too old\|serialization" migration.log | head -50

# Check for partition failures
grep -i "partition.*failed\|partition.*error" migration.log

# Check if partitions are completing
grep "completed (INSERT mode)" migration.log | wc -l
grep "completed (COPY mode)" migration.log | wc -l
```

**What to look for:**
- ❌ "Snapshot too old" errors → Batch size too large
- ❌ "Serialization conflict" errors → Too much parallelism
- ❌ "Connection timeout" errors → Network/connection issues
- ❌ Partition failures → Check error messages
- ❌ "INSERT mode" statements → Should see these, not "COPY mode"

**If you see errors:**
- **Snapshot too old**: Reduce `yugabyte.insertBatchSize=300` (from 500)
- **Serialization conflicts**: Reduce `spark.default.parallelism=100` (from 120)
- **Connection errors**: Increase timeouts, check network

---

### 2. Check Partition Completion Status

**Are partitions completing?**
```bash
# Count completed partitions
grep "completed (INSERT mode)" migration.log | wc -l

# Check partition completion rate
grep "completed (INSERT mode)" migration.log | tail -20
```

**What to look for:**
- ✅ Partitions should be completing regularly
- ❌ If few/no completions → Partitions are stuck or failing
- ❌ If completions are slow → Performance bottleneck

**Expected:**
- With 120 parallelism → Should see partitions completing over time
- If only 3-5 partitions completed in 10 minutes → Major issue

---

### 3. Check Spark UI (If Available)

**Access Spark UI:**
- Usually at: `http://<spark-driver-node>:4040`
- Or check Spark logs for UI URL

**What to check:**
1. **Active Tasks**: How many tasks are running?
   - Expected: ~120 active tasks (matches parallelism)
   - If much lower → Partitions not starting or stuck

2. **Failed Tasks**: Any failed tasks?
   - Should be 0 or very low
   - If high → Check error messages

3. **Task Duration**: How long are tasks taking?
   - Expected: 5-15 minutes per partition (depends on data size)
   - If much longer → Performance issue

4. **Stage Progress**: Is the stage progressing?
   - Check percentage completion
   - If stuck → Partitions not processing

---

### 4. Check YugabyteDB Performance (YBA UI)

**YBA UI → Metrics → YSQL Tab:**

1. **YSQL Ops/Sec**:
   - Expected: 36-44 ops/sec (for 18K-22K rows/sec)
   - Actual: If very low (e.g., 1-2 ops/sec) → YugabyteDB bottleneck
   - Calculation: 500 rows/sec ÷ 500 batch = 1 ops/sec (matches your case!)

2. **CPU Usage**:
   - Expected: 50-70% per node
   - If very low (< 20%) → YugabyteDB not being utilized
   - If very high (> 90%) → YugabyteDB bottleneck

3. **Memory Usage**:
   - Should be reasonable (not maxed out)
   - If maxed out → Memory bottleneck

4. **Connections**:
   - Check number of active connections
   - Should match: spark.executor.instances × spark.executor.cores × connections per task

5. **Node Status**:
   - All 3 nodes should be UP
   - If any node DOWN → Performance issue

---

### 5. Check Network Latency (Cross-Region)

**Cassandra is on-prem, Spark is in Azure Central:**

**Test latency:**
```bash
# From Spark driver node, test Cassandra connectivity
ping <cassandra-host>
# Or
telnet <cassandra-host> 9042
```

**Expected latency:**
- On-prem to Azure: 20-50ms (typical)
- Higher latency = slower Cassandra reads

**Impact:**
- High latency → Slower data reading from Cassandra
- But shouldn't reduce throughput by 97% (your case suggests different issue)

---

### 6. Check Batch Size Configuration

**Verify properties file:**
```bash
grep "yugabyte.insertBatchSize" migration.properties
grep "yugabyte.insertMode" migration.properties
```

**What to check:**
- ✅ `yugabyte.insertMode=INSERT` (not COPY)
- ✅ `yugabyte.insertBatchSize=500` (or 300)
- ❌ If batch size is 50 or 100 → Too small (but still shouldn't be 500 rows/sec)

---

### 7. Check Spark Configuration

**Verify Spark settings in logs:**
```bash
# Check if Spark config is being applied
grep -i "executor\|parallelism\|spark.executor" migration.log | head -20
```

**What to check:**
- Executor instances: Should be 3
- Executor cores: Should be 6
- Parallelism: Should be 120
- If different → Configuration not being applied

---

### 8. Check Connection Pool

**YugabyteDB connection pool:**
```properties
yugabyte.maxPoolSize=12
```

**Symptoms of pool exhaustion:**
- Connection timeouts
- Slow performance
- "Connection pool exhausted" errors

**Check in logs:**
```bash
grep -i "connection\|pool\|timeout" migration.log
```

---

### 9. Check Row Processing Rate

**Calculate actual processing rate:**
```bash
# From logs, check rows processed over time
grep "rows processed" migration.log | tail -10

# Calculate: rows processed / time elapsed
```

**Example:**
- If 300,000 rows in 600 seconds = 500 rows/sec (matches your case)
- This suggests: Data is being processed, but VERY slowly

---

## Most Likely Causes (Based on 500 rows/sec)

### Cause 1: YugabyteDB Not Processing (Most Likely)

**Symptoms:**
- YSQL Ops/Sec = 1-2 ops/sec (matches 500 rows/sec ÷ 500 batch)
- Very low CPU usage (< 20%)
- Partitions completing, but slowly

**Diagnosis:**
- Check YBA UI → YSQL Ops/Sec should be 36-44 ops/sec
- If only 1-2 ops/sec → YugabyteDB is the bottleneck

**Possible reasons:**
1. **Network latency** to YugabyteDB (unlikely if in same region)
2. **Connection issues** (pool exhausted, timeouts)
3. **YugabyteDB performance** (CPU/memory bottleneck)
4. **Transaction conflicts** (many retries)

---

### Cause 2: Partitions Stuck or Failing Silently

**Symptoms:**
- Few partitions completing
- No errors in logs (or errors not visible)
- Spark UI shows tasks stuck

**Diagnosis:**
- Check Spark UI for stuck tasks
- Check partition completion count
- Check for hidden errors

**Solution:**
- Check Spark UI
- Look for task failures
- Check executor logs

---

### Cause 3: Batch Size Actually Very Small

**Symptoms:**
- Properties file says 500, but actual batch size is 50-100
- Each INSERT operation processes very few rows

**Diagnosis:**
- Check actual batch size in logs
- Check if batches are being flushed prematurely

**Solution:**
- Verify properties file is being read
- Check for batch size overrides

---

### Cause 4: Serialization/Transaction Conflicts (Silent Retries)

**Symptoms:**
- No visible errors, but very slow
- YugabyteDB shows high retry rate
- Transactions taking very long

**Diagnosis:**
- Check YugabyteDB transaction metrics
- Look for high retry counts
- Check for serialization conflicts in YBA UI

**Solution:**
- Reduce parallelism to 100
- Reduce batch size to 300
- Check isolation level

---

## Diagnostic Steps (Execute in Order)

### Step 1: Check Logs for Errors
```bash
grep -i "ERROR\|FAILED\|Exception" migration.log | head -50
```

### Step 2: Check Partition Completion
```bash
grep "completed (INSERT mode)" migration.log | wc -l
grep "completed (INSERT mode)" migration.log | tail -10
```

### Step 3: Check YBA UI - YSQL Ops/Sec
- Go to YBA UI → Metrics → YSQL Tab
- Check "Total YSQL Ops/Sec"
- **Expected:** 36-44 ops/sec
- **If you see:** 1-2 ops/sec → YugabyteDB bottleneck

### Step 4: Check YBA UI - CPU Usage
- Check CPU usage per node
- **Expected:** 50-70%
- **If you see:** < 20% → YugabyteDB not being utilized

### Step 5: Check Spark UI (if available)
- Check active tasks count
- Check failed tasks
- Check task duration

### Step 6: Verify Configuration
```bash
grep "insertMode\|insertBatchSize\|parallelism" migration.properties
```

---

## Quick Diagnostic Script

```bash
#!/bin/bash
LOG_FILE="migration.log"

echo "=== Throughput Diagnostic ==="
echo ""

echo "1. Error Count:"
grep -ci "ERROR\|FAILED\|Exception" "$LOG_FILE" || echo "No errors found"
echo ""

echo "2. Partition Completions:"
INSERT_COMPLETIONS=$(grep -c "completed (INSERT mode)" "$LOG_FILE" 2>/dev/null || echo "0")
COPY_COMPLETIONS=$(grep -c "completed (COPY mode)" "$LOG_FILE" 2>/dev/null || echo "0")
echo "  INSERT mode completions: $INSERT_COMPLETIONS"
echo "  COPY mode completions: $COPY_COMPLETIONS"
echo ""

echo "3. Recent Completions (last 5):"
grep "completed.*mode" "$LOG_FILE" | tail -5
echo ""

echo "4. Specific Errors:"
grep -i "snapshot too old\|serialization\|connection\|timeout" "$LOG_FILE" | head -10
echo ""

echo "5. Configuration Check:"
echo "  Insert Mode: $(grep 'yugabyte.insertMode=' migration.properties 2>/dev/null | cut -d'=' -f2)"
echo "  Batch Size: $(grep 'yugabyte.insertBatchSize=' migration.properties 2>/dev/null | cut -d'=' -f2)"
echo "  Parallelism: $(grep 'spark.default.parallelism=' migration.properties 2>/dev/null | cut -d'=' -f2)"
echo ""

echo "6. Throughput from logs:"
grep -i "Throughput.*rows/sec" "$LOG_FILE" | tail -1
```

---

## Most Likely Root Cause

Based on 500 rows/sec (1-2 YSQL ops/sec with batch 500), **most likely cause:**

**YugabyteDB is processing VERY slowly (1-2 operations per second)**

**Possible reasons:**
1. **Transaction conflicts** (many retries, serialization issues)
2. **Connection pool exhaustion** (not enough connections)
3. **YugabyteDB performance issue** (CPU/memory/network bottleneck)
4. **Network latency** (if YugabyteDB is far from Spark)

**Immediate actions:**
1. ✅ Check YBA UI → YSQL Ops/Sec (confirm it's 1-2 ops/sec)
2. ✅ Check YBA UI → CPU usage (if low, YugabyteDB not utilized)
3. ✅ Check logs for transaction errors (serialization, conflicts)
4. ✅ Check connection pool (increase if needed)
5. ✅ Check network latency to YugabyteDB

---

## Next Steps

1. **Run diagnostic script** (above)
2. **Check YBA UI** for YSQL Ops/Sec and CPU
3. **Check logs** for errors
4. **Report findings** to diagnose further

The fact that you're getting exactly 500 rows/sec (which = 1 ops/sec with batch 500) suggests **YugabyteDB is processing at 1 operation per second** instead of 36-44 operations per second. This is a 97% reduction in YugabyteDB processing speed.

