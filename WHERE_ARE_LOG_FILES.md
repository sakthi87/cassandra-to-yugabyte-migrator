# Where Are Migration Log Files Generated?

## Important: Logs Go to Console by Default

**The application does NOT automatically create a log file.**  
Logs are written to **stdout/stderr (console)** by default.

**You need to redirect output to create a log file.**

---

## How Log Files Are Created

### Option 1: Manual Redirection (Most Common)

**When you run:**
```bash
spark-submit ... > migration.log 2>&1
```

**Log file location:**
- **Directory:** Wherever you ran the command from
- **File:** `migration.log` (or whatever name you used)
- **Example:** If you run from `/home/user/migration/`, log is at `/home/user/migration/migration.log`

---

### Option 2: Using nohup

**When you run:**
```bash
nohup spark-submit ... > migration.log 2>&1 &
```

**Log file locations:**
- **Directory:** Wherever you ran the command from
- **Files:**
  - `migration.log` (your redirect)
  - `nohup.out` (nohup's default output file)

---

### Option 3: Using tee (Scripts)

**When you run:**
```bash
spark-submit ... 2>&1 | tee migration.log
```

**Log file location:**
- **Directory:** Wherever you ran the command from
- **File:** `migration.log`
- **Also shows on console** (tee sends to both file and console)

---

### Option 4: YARN Mode

**When running on YARN:**
```bash
spark-submit --master yarn ...
```

**Log locations:**
1. **Console output:** Goes to YARN logs
2. **Access logs:**
   ```bash
   # Get application ID from Spark UI or YARN UI
   yarn logs -applicationId <app-id>
   ```
3. **YARN log directory:**
   - Usually: `/tmp/logs/<user>/logs/application_<timestamp>_<id>/`
   - Or check: `$HADOOP_LOG_DIR` environment variable

---

### Option 5: Spark Standalone Mode

**When running on Spark Standalone:**
```bash
spark-submit --master spark://master:7077 ...
```

**Log locations:**
1. **Driver logs:**
   - Usually: `$SPARK_HOME/work/<app-id>/stdout`
   - Or: `$SPARK_HOME/logs/spark-<user>-org.apache.spark.deploy.master.Master-<hostname>.out`

2. **Executor logs:**
   - Usually: `$SPARK_HOME/work/<executor-id>/stdout`
   - Or: `$SPARK_HOME/logs/` directory

---

## How to Find Your Log File

### Step 1: Check Current Directory

**If you ran spark-submit manually:**
```bash
# Check current directory
pwd

# Look for log files
ls -lt *.log 2>/dev/null | head -10
```

### Step 2: Check Common Locations

```bash
# Check project directory (if you ran from project root)
ls -lt migration*.log 2>/dev/null

# Check home directory
ls -lt ~/migration*.log 2>/dev/null

# Check /tmp (some scripts use this)
ls -lt /tmp/migration*.log 2>/dev/null
```

### Step 3: Find Recent Log Files

```bash
# Find all .log files modified in last 24 hours
find . -name "*.log" -type f -mtime -1 2>/dev/null

# Find all .log files (broader search)
find . -name "*.log" -type f 2>/dev/null | head -20
```

### Step 4: Check Spark Logs Directory

```bash
# Check Spark logs directory
ls -lt $SPARK_HOME/logs/ 2>/dev/null | head -20

# Or check common locations
ls -lt /tmp/spark-*/ 2>/dev/null | head -20
```

### Step 5: If Using YARN

```bash
# List recent YARN applications
yarn application -list -appStates FINISHED,FAILED,KILLED | head -20

# Get logs for specific application
yarn logs -applicationId application_1234567890_0001 > migration.log
```

---

## How to Create Log File (If You Haven't)

### If Migration is Currently Running:

**Find the process:**
```bash
ps aux | grep spark-submit
```

**Attach to existing output:**
- If running in terminal: Output is in that terminal
- If running in background: Check nohup.out or redirect location

### For Next Run:

**Redirect output:**
```bash
spark-submit \
  --class com.company.migration.MainApp \
  ... \
  migration.properties > migration.log 2>&1
```

**Or use nohup:**
```bash
nohup spark-submit \
  --class com.company.migration.MainApp \
  ... \
  migration.properties > migration.log 2>&1 &
```

**Or use tee (see output AND save to file):**
```bash
spark-submit \
  --class com.company.migration.MainApp \
  ... \
  migration.properties 2>&1 | tee migration.log
```

---

## Default Log Configuration

**The application uses log4j2.properties which:**
- ✅ Logs to **console (stdout/stderr)** by default
- ❌ Does **NOT** write to a file automatically
- ✅ You **must redirect** output to create a log file

**To change this:**
- Modify `src/main/resources/log4j2.properties`
- Add a FileAppender
- Rebuild JAR

---

## Quick Checklist: Where to Look

1. ✅ **Current directory** (where you ran spark-submit)
2. ✅ **Project directory** (if you ran from project root)
3. ✅ **Home directory** (if you ran from home)
4. ✅ **YARN logs** (if using YARN: `yarn logs -applicationId <id>`)
5. ✅ **Spark logs directory** (`$SPARK_HOME/logs/`)
6. ✅ **nohup.out** (if using nohup)
7. ✅ **Terminal output** (if still running in foreground)

---

## Example: Find Log File Script

```bash
#!/bin/bash
echo "=== Searching for migration log files ==="
echo ""

echo "1. Current directory:"
ls -lt *.log 2>/dev/null | head -5 || echo "  No .log files found"

echo ""
echo "2. Project directory:"
ls -lt migration*.log 2>/dev/null | head -5 || echo "  No migration*.log files found"

echo ""
echo "3. Recent log files (last 24 hours):"
find . -name "*.log" -type f -mtime -1 2>/dev/null | head -10 || echo "  No recent log files"

echo ""
echo "4. Spark logs directory:"
ls -lt $SPARK_HOME/logs/*.out 2>/dev/null | head -5 || echo "  No Spark logs found"

echo ""
echo "5. YARN applications (if using YARN):"
yarn application -list -appStates RUNNING,FINISHED 2>/dev/null | head -5 || echo "  Not using YARN or yarn command not available"
```

---

## Summary

**Answer:** The log file is created **wherever you redirect the output** when running spark-submit.

**Most common locations:**
- Same directory where you ran `spark-submit`
- Check: `pwd` to see current directory
- Check: `ls -lt *.log` to find log files

**If you didn't redirect output:**
- Logs are in console/stdout
- Check terminal where you ran the command
- Or check YARN logs if using YARN
- Or check Spark logs directory

