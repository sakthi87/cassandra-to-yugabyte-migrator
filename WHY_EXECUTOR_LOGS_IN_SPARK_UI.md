# Why INSERT Mode Logs Appear in Spark UI Executor Logs But Not Your Log File

## The Problem

**You see:**
- ✅ INSERT mode statements in **Spark UI → Executor Logs**
- ❌ INSERT mode statements **NOT** in your redirected log file

**Why?** Spark executor logs are separate from driver logs.

---

## Understanding Spark Logging Architecture

### Spark Application Structure:

```
Spark Application
├── Driver (Master/Coordinator)
│   ├── Runs on: Spark driver node
│   ├── Logs go to: Your redirected output (migration.log)
│   └── Logs: Driver-level operations
│
└── Executors (Workers)
    ├── Run on: Spark worker nodes
    ├── Logs go to: Spark executor log files
    ├── Visible in: Spark UI → Executors → Logs
    └── Logs: Partition execution, data processing
```

### PartitionExecutor Runs on Executors

**The INSERT mode log statements:**
```scala
logInfo(s"Partition $actualPartitionId completed (INSERT mode): ...")
```

**Location:** `PartitionExecutor.scala`  
**Execution:** Runs on **executor nodes** (not driver)  
**Logs go to:** **Executor log files** (visible in Spark UI)

---

## Why This Happens

### 1. Driver vs Executor Logging

**Driver logs (your migration.log file):**
- Main application flow
- Configuration loading
- Job orchestration
- Final summary metrics
- Runs on: Driver node

**Executor logs (Spark UI):**
- Partition processing
- Data transformation
- INSERT/COPY operations
- Row processing
- Runs on: Executor nodes (workers)

### 2. PartitionExecutor Runs on Executors

**Code location:** `PartitionExecutor.execute()`

**Execution context:**
```scala
df.foreachPartition { partition =>
  // This code runs on EXECUTORS, not driver
  val executor = new PartitionExecutor(...)
  executor.execute(partition)  // Runs on executor node
  logInfo("Partition X completed (INSERT mode): ...")  // Goes to executor log
}
```

**Result:** The "Partition X completed (INSERT mode)" log goes to **executor logs**, not driver logs.

---

## How to See INSERT Mode Logs

### Option 1: Spark UI (Current Method - Works!)

**Steps:**
1. Open Spark UI: `http://<spark-driver-host>:4040`
2. Go to: **Executors** tab
3. Click: **stdout** or **stderr** link for any executor
4. Search for: "completed (INSERT mode)"
5. ✅ You'll see all partition completion messages

**Pros:**
- ✅ All executor logs visible
- ✅ Real-time viewing
- ✅ Per-executor breakdown

**Cons:**
- ❌ Not in your log file
- ❌ Requires Spark UI access
- ❌ Logs scattered across executors

---

### Option 2: Aggregate Executor Logs (After Job Completes)

**For YARN:**
```bash
# Get application ID
yarn application -list | grep MainApp

# Get all logs (driver + executors)
yarn logs -applicationId application_1234567890_0001 > all_logs.log

# Search for INSERT mode
grep "completed (INSERT mode)" all_logs.log
```

**For Spark Standalone:**
```bash
# Executor logs are in:
$SPARK_HOME/work/<executor-id>/stdout
$SPARK_HOME/work/<executor-id>/stderr

# Or:
$SPARK_HOME/logs/
```

**Pros:**
- ✅ All logs in one file
- ✅ Can grep/search easily
- ✅ Available after job completes

**Cons:**
- ❌ Only after job finishes (YARN)
- ❌ Need to know executor IDs (Standalone)

---

### Option 3: Change Log Level to INFO (Not Recommended)

**Current log4j2.properties:**
```properties
logger.app.name = com.company.migration
logger.app.level = WARN  # Only warnings and errors
```

**Problem:** Even if you change to INFO, executor logs still go to executor log files, not driver logs.

**Why?** Executors run in separate JVM processes with separate log streams.

---

### Option 4: Check Final Summary (Driver Logs)

**The driver logs DO show final summary:**
```
Migration Metrics:
  Rows Read: 86000000
  Rows Written: 86000000
  Throughput: 15000.00 rows/sec
```

**Location:** Driver logs (your migration.log file)

**Pros:**
- ✅ Available in your log file
- ✅ Shows overall performance
- ✅ Easy to find

**Cons:**
- ❌ Doesn't show per-partition details
- ❌ Only at end of migration

---

## Why Executor Logs Don't Go to Driver Log File

### Technical Reason:

1. **Executors are separate JVM processes**
   - Each executor runs in its own JVM
   - Has its own stdout/stderr streams
   - Logs go to executor's log files, not driver

2. **Spark architecture:**
   ```
   Driver JVM (your migration.log)
   └── Sends tasks to → Executor JVMs (separate processes)
       └── Executor logs → Executor log files (Spark UI)
   ```

3. **Output redirection:**
   ```bash
   spark-submit ... > migration.log 2>&1
   ```
   - Only redirects **driver** output
   - Does **NOT** redirect executor output
   - Executor output goes to Spark's executor log files

---

## What You CAN See in Your Log File (Driver Logs)

**Your migration.log file contains:**
- ✅ Application startup messages
- ✅ Configuration loading
- ✅ Job initialization
- ✅ **Final summary metrics** (rows written, throughput)
- ✅ Error messages (if driver encounters errors)
- ❌ **NOT** per-partition completion messages (those are in executor logs)

---

## Recommendation: Use Spark UI

**Best approach:**
1. ✅ Use Spark UI to see executor logs (real-time)
2. ✅ Check your log file for final summary metrics
3. ✅ Use `yarn logs` (if YARN) to get all logs after completion

**For diagnosing throughput:**
- Check Spark UI → Executors → Logs for partition completions
- Check your log file for final summary (throughput calculation)
- Both give you the information you need

---

## Summary

| Log Type | Location | Contains |
|----------|----------|----------|
| **Driver Logs** | Your migration.log file | Startup, config, final summary |
| **Executor Logs** | Spark UI → Executors → Logs | Partition completions, INSERT mode statements |
| **Combined Logs** | `yarn logs -applicationId <id>` | All logs (driver + executors) |

**Why INSERT mode logs are in Spark UI:**
- PartitionExecutor runs on **executors** (not driver)
- Executor logs go to **executor log files** (visible in Spark UI)
- Your redirected output only captures **driver logs**

**This is normal Spark behavior** - executor logs are separate from driver logs.

