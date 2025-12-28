# Checkpoint Test Guide

This guide explains how to test the checkpoint/resume functionality by stopping a migration mid-run and resuming from the checkpoint.

## Prerequisites

1. **Cassandra** running with test data
2. **YugabyteDB** running and accessible
3. **Spark 3.5.1** installed and `SPARK_HOME` set
4. **Migration JAR** built (`mvn package -DskipTests`)

## Test Scenario

1. **Truncate** the target YugabyteDB table
2. **Start** a migration (will run until stopped)
3. **Stop/Kill** the migration process mid-run
4. **Check** checkpoint status
5. **Resume** from the checkpoint
6. **Verify** all data migrated successfully

## Step-by-Step Test

### Step 1: Prepare Environment

```bash
cd /Users/subhalakshmiraj/Documents/cassandra-to-yugabyte-migrator

# Set Spark home if not already set
export SPARK_HOME=$HOME/spark-3.5.1

# Verify Spark is available
$SPARK_HOME/bin/spark-submit --version
```

### Step 2: Truncate Target Table

The test script will automatically truncate the table, but you can also do it manually:

```bash
# Using the script
./scripts/test_checkpoint.sh

# Or manually using psql
psql -h localhost -p 5433 -U yugabyte -d test_keyspace \
  -c "TRUNCATE TABLE public.dda_pstd_fincl_txn_cnsmr_by_accntnbr;"
```

### Step 3: Start Migration (Will Be Stopped)

```bash
./scripts/test_checkpoint.sh
```

The script will:
- Truncate the target table
- Show current checkpoint status
- Start a migration with a unique run ID
- Display the run ID and log file location

**Important:** Let it run for 10-30 seconds, then stop it:
- Press `Ctrl+C` in the terminal, OR
- In another terminal: `pkill -f 'Cassandra-to-YugabyteDB Migration'`

### Step 4: Check Checkpoint Status

After stopping, check the checkpoint status:

```bash
# Check status for the run (use the run ID from Step 3)
./scripts/check_checkpoint_status.sh <run_id>

# Or check all recent runs
./scripts/check_checkpoint_status.sh
```

You should see:
- Some partitions with status `PASS` (completed)
- Some partitions with status `NOT_STARTED`, `STARTED`, or `FAIL` (pending)

### Step 5: Resume from Checkpoint

Resume the migration using the previous run ID:

```bash
./scripts/resume_checkpoint.sh <previous_run_id>
```

The script will:
- Verify the previous run exists
- Show pending partitions
- Start a new run with `prev_run_id` set
- Only process pending partitions
- Complete the migration

### Step 6: Verify Results

After resume completes, verify:

```bash
# Check final checkpoint status
./scripts/check_checkpoint_status.sh <new_run_id>

# Verify row counts match
psql -h localhost -p 5433 -U yugabyte -d test_keyspace \
  -c "SELECT COUNT(*) FROM public.dda_pstd_fincl_txn_cnsmr_by_accntnbr;"
```

## Manual Testing (Alternative)

If you prefer to test manually:

### 1. Start Migration

```bash
# Create properties with run ID
cp src/main/resources/migration.properties /tmp/migration_test.properties
echo "migration.runId=$(date +%s)" >> /tmp/migration_test.properties
echo "migration.prevRunId=0" >> /tmp/migration_test.properties

# Run migration
$SPARK_HOME/bin/spark-submit \
  --class com.company.migration.MainApp \
  --master "local[*]" \
  --driver-memory 4g \
  --executor-memory 4g \
  --properties-file /tmp/migration_test.properties \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  /tmp/migration_test.properties
```

### 2. Stop Migration

After 10-30 seconds, press `Ctrl+C` or kill the process.

### 3. Check Status

```sql
-- Connect to YugabyteDB
psql -h localhost -p 5433 -U yugabyte -d test_keyspace

-- Check run info
SELECT * FROM public.migration_run_info ORDER BY run_id DESC LIMIT 1;

-- Check partition status
SELECT status, COUNT(*) 
FROM public.migration_run_details 
WHERE run_id = <run_id_from_above>
GROUP BY status;
```

### 4. Resume Migration

```bash
# Create properties with new run ID and previous run ID
cp src/main/resources/migration.properties /tmp/migration_resume.properties
echo "migration.runId=$(date +%s)" >> /tmp/migration_resume.properties
echo "migration.prevRunId=<previous_run_id>" >> /tmp/migration_resume.properties

# Run resume
$SPARK_HOME/bin/spark-submit \
  --class com.company.migration.MainApp \
  --master "local[*]" \
  --driver-memory 4g \
  --executor-memory 4g \
  --properties-file /tmp/migration_resume.properties \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  /tmp/migration_resume.properties
```

## Expected Behavior

### During First Run (Stopped):
- Some partitions complete → Status: `PASS`
- Some partitions in progress → Status: `STARTED`
- Some partitions not started → Status: `NOT_STARTED`

### During Resume:
- Only pending partitions (`NOT_STARTED`, `STARTED`, `FAIL`) are processed
- Completed partitions (`PASS`) are skipped
- New run ID is created with `prev_run_id` pointing to previous run

### After Resume:
- All partitions should have status: `PASS`
- Run status should be: `ENDED`
- Row counts should match between Cassandra and YugabyteDB

## Troubleshooting

### Issue: No pending partitions found
**Solution:** The previous run may have completed. Check the run status:
```bash
./scripts/check_checkpoint_status.sh <run_id>
```

### Issue: Resume doesn't skip completed partitions
**Solution:** Verify `prev_run_id` is set correctly in properties file and checkpoint tables exist.

### Issue: Duplicate run_id error
**Solution:** Use a different run_id (timestamp-based is recommended).

### Issue: Checkpoint tables don't exist
**Solution:** Ensure `migration.checkpoint.enabled=true` in properties file. Tables are created automatically on first run.

## Verification Queries

```sql
-- Check all runs for a table
SELECT * FROM public.migration_run_info 
WHERE table_name = 'transaction_datastore.dda_pstd_fincl_txn_cnsmr_by_accntnbr'
ORDER BY run_id DESC;

-- Check partition completion rate
SELECT 
    run_id,
    COUNT(*) FILTER (WHERE status = 'PASS') as completed,
    COUNT(*) FILTER (WHERE status IN ('NOT_STARTED', 'STARTED', 'FAIL')) as pending,
    COUNT(*) as total
FROM public.migration_run_details
WHERE table_name = 'transaction_datastore.dda_pstd_fincl_txn_cnsmr_by_accntnbr'
GROUP BY run_id
ORDER BY run_id DESC;
```

