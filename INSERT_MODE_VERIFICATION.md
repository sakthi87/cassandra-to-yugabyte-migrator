# INSERT Mode Verification Guide

## Why You Might Not See INSERT Mode Log Statements

### 1. **CRITICAL: Rebuild JAR File**
**Most Common Issue:** You're using an OLD JAR file that doesn't have INSERT mode code!

```bash
# After pulling code from GitHub, ALWAYS rebuild:
mvn clean package -DskipTests
```

**How to verify:**
- Check JAR file date: `ls -lh target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar`
- Should be built AFTER the INSERT mode code was added (recent date)
- If using an old JAR, you won't see INSERT mode statements

### 2. **Log Statements Appear at Partition Completion**
The INSERT mode log statements appear **at the END of each partition**, not during execution:

```
Partition X completed (INSERT mode): Y rows processed, Z rows inserted, W duplicates skipped
```

**Timing:**
- If migration is still running → wait for partitions to complete
- Statements appear as each partition finishes processing
- For 109M records, it may take time before you see completion statements

### 3. **Good Sign: YSQL Metrics Showing Activity**
✅ **You mentioned seeing "20 records in YBA YSQL tab"** - This is a GOOD sign!
- YSQL metrics = INSERT mode is likely working (COPY mode shows in DocDB, not YSQL)
- COPY mode → metrics in DocDB layer
- INSERT mode → metrics in YSQL layer

### 4. **What to Check in Your Logs**

**Search for these patterns:**

```bash
# INSERT mode (what you want):
grep "completed (INSERT mode)" your-log-file.log

# COPY mode (wrong):
grep "completed (COPY mode)" your-log-file.log

# Any partition completion (older format):
grep "Partition.*completed" your-log-file.log
```

**If you see:**
- ✅ "INSERT mode" → Correct, working as expected
- ❌ "COPY mode" → Wrong, using old code or properties not loaded
- ❌ No "mode" in statement → Using very old JAR file

### 5. **Verify Properties File is Loaded**

**Check if properties file is being read:**
```bash
# In your log file, search for:
grep -i "insertMode\|INSERT\|COPY" your-log-file.log | head -20
```

**Make sure properties file has:**
```properties
yugabyte.insertMode=INSERT
yugabyte.insertBatchSize=5000
```

### 6. **Check Log File Location**
- Make sure you're looking at the correct log file
- Check both console output and log files
- Spark logs might be in a different location

### 7. **Quick Verification Steps**

**Step 1: Verify JAR is recent**
```bash
ls -lh target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar
# Should show recent build date
```

**Step 2: Check if INSERT mode code exists in JAR** (if possible)
```bash
# Try to verify JAR contains the code (may not work on all systems)
unzip -l target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep PartitionExecutor
```

**Step 3: Wait for partitions to complete**
- INSERT mode statements appear when partitions finish
- For large migrations, wait for some partitions to complete
- Check logs periodically as migration progresses

**Step 4: Verify INSERT mode is working (even without log statements)**
- ✅ YSQL metrics showing activity → INSERT mode likely working
- ✅ No duplicate key errors → INSERT mode handling duplicates
- ✅ Data loading successfully → INSERT mode working

### 8. **What Your Logs Should Show Eventually**

As partitions complete, you should see:
```
INFO PartitionExecutor: Partition 0 completed (INSERT mode): 50000 rows processed, 50000 rows inserted, 0 duplicates skipped
INFO PartitionExecutor: Partition 1 completed (INSERT mode): 52000 rows processed, 52000 rows inserted, 0 duplicates skipped
INFO PartitionExecutor: Partition 2 completed (INSERT mode): 48000 rows processed, 48000 rows inserted, 0 duplicates skipped
...
```

**Note:** For a fresh migration (no duplicates), you'll see:
- `rows processed` = total rows
- `rows inserted` = same as processed (all new)
- `duplicates skipped` = 0

**For a migration with existing data:**
- `rows processed` = total rows
- `rows inserted` = new rows inserted
- `duplicates skipped` = existing rows skipped

## Summary

**Most Likely Issues:**
1. ❌ **Old JAR file** - Must rebuild after pulling code
2. ⏳ **Migration still running** - Statements appear when partitions complete
3. ✅ **YSQL metrics = Good sign** - INSERT mode is likely working

**Action Items:**
1. Rebuild JAR: `mvn clean package -DskipTests`
2. Wait for partitions to complete (statements appear at completion)
3. Check YSQL metrics (your 20 records/sec suggests INSERT mode is working)
4. Search logs for "completed (INSERT mode)" as migration progresses

