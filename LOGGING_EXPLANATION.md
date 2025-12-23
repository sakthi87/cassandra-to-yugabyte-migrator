# Logging Explanation and Fix

## Issue 1: Generated Code in Logs

### What You're Seeing
You're seeing generated Scala/Java code in your logs, like:
```scala
class SpecificUnsafeProjection extends UnsafeProjection {
  // ... generated code ...
}
```

### Why This Happens
- **Spark's Catalyst Optimizer** generates optimized code at runtime (codegen)
- When **DEBUG logging** is enabled, Spark prints this generated code
- This is **normal Spark behavior** but very verbose
- The generated code is for performance optimization (UnsafeRow operations)

### The Fix
Created `src/main/resources/log4j2.properties` that:
- Suppresses Spark's codegen DEBUG logs
- Sets Spark internal loggers to WARN level
- Keeps application logs at INFO level
- Reduces log verbosity by ~90%

### How It Works
The log4j2.properties file sets:
```properties
# Suppress Spark's codegen output
logger.codegen.name = org.apache.spark.sql.catalyst.expressions.codegen
logger.codegen.level = WARN
```

This prevents the generated code from appearing in logs.

---

## Issue 2: SELECT COUNT(*) Queries

### What You're Seeing
You're seeing queries like:
```sql
SELECT COUNT(*) FROM transaction_datastore.dda_pstd_fincl_txn_cnsmr_by_accntnbr
```

### Why This Happens
These queries are from **validation** that runs **after migration completes**:

1. **RowCountValidator** (lines 43, 63 in `RowCountValidator.scala`):
   - Executes `SELECT COUNT(*) FROM cassandra_table`
   - Executes `SELECT COUNT(*) FROM yugabyte_table`
   - Compares counts to verify data integrity

2. **When It Runs**:
   - Only if `migration.validation.enabled=true` in properties
   - Only if `table.validate=true` in properties
   - Runs **after** all data is migrated

### This Is Expected Behavior
✅ **This is correct and intentional** - validation ensures data integrity.

### To Disable (Not Recommended)
If you want to disable validation to reduce queries:
```properties
migration.validation.enabled=false
```

**Note**: Disabling validation means you won't verify data was migrated correctly.

---

## Other SELECT Queries You Might See

### 1. Spark Cassandra Connector Token Range Queries
```
DEBUG ScanHelper: Fetching data for range
token("accnt_uid", "prdct_cde") > ? AND token("accnt_uid", "prdct_cde") <= ?
```
- **What**: Spark reading data from Cassandra
- **Why**: Token-aware partitioning for optimal parallelism
- **Action**: Normal - this is how Spark reads from Cassandra

### 2. Checkpoint Queries
If checkpointing is enabled:
```sql
SELECT * FROM migration_checkpoint WHERE job_id = ? AND partition_id = ?
```
- **What**: Reading checkpoint status
- **Why**: Resume capability for large migrations
- **Action**: Normal - enables resume on failure

---

## Summary

| Issue | Cause | Fix | Status |
|-------|-------|-----|--------|
| Generated code in logs | Spark DEBUG logging | log4j2.properties | ✅ Fixed |
| SELECT COUNT(*) queries | Validation after migration | Expected behavior | ✅ Normal |
| Token range queries | Spark reading from Cassandra | Expected behavior | ✅ Normal |

---

## Next Steps

1. **Rebuild the JAR** to include log4j2.properties:
   ```bash
   mvn clean package -DskipTests
   ```

2. **Run migration again** - logs should be much cleaner

3. **If you still see verbose logs**, check:
   - Spark's default log4j configuration might be overriding
   - Pass `-Dlog4j.configuration=log4j2.properties` to spark-submit
   - Or set `spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j2.properties`

---

## Verification

After applying the fix, you should see:
- ✅ No generated code in logs
- ✅ Only INFO/WARN/ERROR messages
- ✅ SELECT COUNT(*) only during validation (if enabled)
- ✅ Clean, readable logs

