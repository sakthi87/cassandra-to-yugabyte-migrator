# Checkpoint Testing Guide

This guide provides step-by-step instructions for testing the checkpoint functionality in a new environment.

## Prerequisites

1. **Cassandra** running and accessible
2. **YugabyteDB** running and accessible
3. **Spark 3.5.1** installed and configured
4. **Java 17+** installed
5. **Migration JAR** built (`target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar`)

## Environment Setup

### 1. Update Configuration

Edit `src/main/resources/migration.properties` with your environment details:

```properties
# Cassandra Connection
cassandra.host=your-cassandra-host
cassandra.port=9043
cassandra.localDC=your-datacenter

# YugabyteDB Connection
yugabyte.host=your-yugabyte-host
yugabyte.port=5433
yugabyte.database=your-database
yugabyte.username=your-username
yugabyte.password=your-password

# Table Configuration
table.source.keyspace=your-keyspace
table.source.table=your-table
table.target.schema=public
table.target.table=your-table
```

### 2. Initialize Checkpoint Tables

Run the initialization script:

```bash
./scripts/init_checkpoint_tables.sh
```

Or manually create the tables:

```sql
-- Connect to YugabyteDB
psql -h your-yugabyte-host -p 5433 -U your-username -d your-database

-- Create checkpoint tables
CREATE TABLE IF NOT EXISTS public.migration_run_info (
    table_name      TEXT,
    run_id          BIGINT,
    run_type        TEXT,
    prev_run_id     BIGINT,
    start_time      TIMESTAMPTZ DEFAULT now(),
    end_time        TIMESTAMPTZ,
    run_info        TEXT,
    status          TEXT,
    PRIMARY KEY (table_name, run_id)
);

CREATE TABLE IF NOT EXISTS public.migration_run_details (
    table_name      TEXT,
    run_id          BIGINT,
    start_time      TIMESTAMPTZ DEFAULT now(),
    token_min       BIGINT,
    token_max       BIGINT,
    partition_id    INT,
    status          TEXT,
    run_info        TEXT,
    PRIMARY KEY ((table_name, run_id), token_min, partition_id)
);

CREATE INDEX IF NOT EXISTS idx_run_details_status ON public.migration_run_details (table_name, run_id, status);
CREATE INDEX IF NOT EXISTS idx_run_info_status ON public.migration_run_info (table_name, status);
```

## Test Scenarios

### Scenario 1: Basic Checkpoint Test (Start, Stop, Resume)

This test verifies that checkpointing works correctly by:
1. Starting a migration
2. Stopping it mid-run
3. Resuming from the checkpoint

#### Step 1: Truncate Target Table

```bash
./scripts/truncate_table.sh
```

Or manually:

```sql
TRUNCATE TABLE public.your-table;
```

#### Step 2: Start Initial Migration

```bash
./scripts/test_checkpoint.sh
```

This script will:
- Truncate the target table
- Initialize checkpoint tables (if needed)
- Start the migration with a new run ID
- Wait for you to stop it (Ctrl+C after 10-30 seconds)

**Note the Run ID** from the output, e.g.:
```
Run ID: 1703456789
```

#### Step 3: Check Checkpoint Status

```bash
./scripts/check_checkpoint_status.sh <RUN_ID>
```

Example:
```bash
./scripts/check_checkpoint_status.sh 1703456789
```

You should see:
- Run status: `STARTED`
- Partition status: Mix of `NOT_STARTED`, `STARTED`, `PASS`, or `FAIL`
- Pending partitions that can be resumed

#### Step 4: Resume Migration

```bash
./scripts/resume_checkpoint.sh <PREVIOUS_RUN_ID>
```

Example:
```bash
./scripts/resume_checkpoint.sh 1703456789
```

This will:
- Create a new run ID
- Load pending partitions from the previous run
- Process only incomplete/failed partitions
- Complete the migration

#### Step 5: Verify Results

```bash
# Check final checkpoint status
./scripts/check_checkpoint_status.sh <NEW_RUN_ID>

# Verify data in YugabyteDB
psql -h your-yugabyte-host -p 5433 -U your-username -d your-database \
  -c "SELECT COUNT(*) FROM public.your-table;"
```

### Scenario 2: Automated End-to-End Test

Use the automated test script:

```bash
./scripts/quick_checkpoint_test.sh
```

This script:
1. Truncates the target table
2. Starts a migration
3. Stops it after a delay
4. Resumes from the checkpoint
5. Verifies the results

**Note:** This script uses `timeout` command which may not be available on all systems. Modify it for your environment if needed.

### Scenario 3: Multiple Table Test

To test checkpointing with multiple tables:

1. **Table 1:**
   ```bash
   # Update migration.properties for table 1
   table.source.keyspace=keyspace1
   table.source.table=table1
   table.target.table=table1
   
   # Run migration
   ./scripts/test_checkpoint.sh
   # Stop after 20 seconds
   # Resume
   ./scripts/resume_checkpoint.sh <RUN_ID>
   ```

2. **Table 2:**
   ```bash
   # Update migration.properties for table 2
   table.source.keyspace=keyspace2
   table.source.table=table2
   table.target.table=table2
   
   # Run migration (should not interfere with table 1 checkpoints)
   ./scripts/test_checkpoint.sh
   ```

Checkpoint tables track by `table_name`, so multiple tables can run independently.

## Understanding Checkpoint Status

### Run Status Values

- `NOT_STARTED`: Run initialized but not yet started
- `STARTED`: Run is in progress
- `ENDED`: Run completed successfully

### Partition Status Values

- `NOT_STARTED`: Partition not yet processed
- `STARTED`: Partition processing started
- `PASS`: Partition completed successfully
- `FAIL`: Partition failed (will be retried on resume)

### Querying Checkpoint Tables

```sql
-- Check all runs for a table
SELECT * FROM public.migration_run_info
WHERE table_name = 'your-keyspace.your-table'
ORDER BY run_id DESC;

-- Check partition status for a run
SELECT status, COUNT(*) as count
FROM public.migration_run_details
WHERE table_name = 'your-keyspace.your-table'
  AND run_id = 1703456789
GROUP BY status;

-- Find pending partitions
SELECT partition_id, token_min, token_max, status
FROM public.migration_run_details
WHERE table_name = 'your-keyspace.your-table'
  AND run_id = 1703456789
  AND status IN ('NOT_STARTED', 'STARTED', 'FAIL')
ORDER BY partition_id;
```

## Troubleshooting

### Issue: "relation migration_run_info does not exist"

**Solution:** Run the initialization script:
```bash
./scripts/init_checkpoint_tables.sh
```

### Issue: "Task not serializable" error

**Solution:** This was fixed in the latest code. Ensure you're using the latest JAR:
```bash
mvn clean package -DskipTests
```

### Issue: Resume doesn't skip completed partitions

**Check:**
1. Verify `migration.prevRunId` is set correctly in properties
2. Check that previous run has partitions with status `PASS`
3. Verify checkpoint tables are in the correct schema (`migration.checkpoint.keyspace`)

### Issue: All partitions show as NOT_STARTED after resume

**Possible causes:**
1. Previous run was stopped before any partitions started
2. Checkpoint interval is too high (partitions update status periodically)
3. Check `migration.checkpoint.interval` in properties (default: 50000 rows)

### Issue: Migration processes all partitions on resume

**Check:**
1. Verify `migration.prevRunId` matches the previous run ID
2. Check that previous run exists in `migration_run_info`
3. Verify pending partitions exist in `migration_run_details`

## Configuration Parameters

Key checkpoint-related properties in `migration.properties`:

```properties
# Enable/disable checkpointing
migration.checkpoint.enabled=true

# Schema/keyspace for checkpoint tables
migration.checkpoint.keyspace=public

# How often to update partition status (in rows)
migration.checkpoint.interval=50000

# Run ID management
# Leave empty to auto-generate (timestamp-based)
migration.runId=

# Previous run ID for resume (0 for new run)
migration.prevRunId=0
```

## Best Practices

1. **Always initialize checkpoint tables** before first migration
2. **Use unique run IDs** for each migration attempt
3. **Set `migration.prevRunId`** correctly when resuming
4. **Monitor checkpoint status** during long-running migrations
5. **Clean up old runs** periodically to avoid table bloat:
   ```sql
   DELETE FROM public.migration_run_details
   WHERE run_id < (SELECT MAX(run_id) - 10 FROM public.migration_run_info);
   
   DELETE FROM public.migration_run_info
   WHERE run_id < (SELECT MAX(run_id) - 10 FROM public.migration_run_info);
   ```

## Performance Considerations

- **Checkpoint interval:** Lower values = more frequent updates = more database writes
- **Partition tracking:** Each partition creates a checkpoint entry
- **Resume overhead:** Resume mode queries checkpoint tables to determine pending partitions

For large tables with many partitions, consider:
- Increasing `migration.checkpoint.interval` (default: 50000)
- Using larger split sizes to reduce partition count
- Cleaning up old checkpoint data periodically

## Validation

After a successful migration with checkpointing:

1. **Row count validation:**
   ```sql
   -- Compare source and target
   SELECT COUNT(*) FROM cassandra_keyspace.table_name;
   SELECT COUNT(*) FROM public.table_name;
   ```

2. **Checkpoint completeness:**
   ```sql
   -- All partitions should be PASS
   SELECT status, COUNT(*) 
   FROM public.migration_run_details
   WHERE table_name = 'your-keyspace.your-table'
     AND run_id = <FINAL_RUN_ID>
   GROUP BY status;
   ```

3. **Run status:**
   ```sql
   -- Final run should be ENDED
   SELECT status, end_time, run_info
   FROM public.migration_run_info
   WHERE table_name = 'your-keyspace.your-table'
     AND run_id = <FINAL_RUN_ID>;
   ```

## Support

If you encounter issues:
1. Check the migration logs for errors
2. Verify checkpoint table structure matches the code
3. Ensure all configuration parameters are correct
4. Review the troubleshooting section above

For detailed logs, run migration with:
```bash
spark-submit --class com.company.migration.MainApp \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  --properties-file src/main/resources/migration.properties \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  src/main/resources/migration.properties 2>&1 | tee migration.log
```

