# Source Duplicate Detection - Explanation and Implementation

## What Does "Source Duplicate Detection" Mean?

**Source duplicate detection** means checking if your **Cassandra source table** contains duplicate primary keys **before** starting the migration to YugabyteDB.

---

## Why Is This Important?

### The Problem
If the source Cassandra table has duplicate primary keys, when you migrate to YugabyteDB:
- YugabyteDB will reject the duplicate keys (primary key constraint violation)
- Your migration will fail with: `ERROR: duplicate key value violates unique constraint`

### Why Would Cassandra Have Duplicates? 🤔

**In theory, Cassandra should NEVER have duplicate primary keys** because:
- Primary keys in Cassandra are enforced by the database
- Same primary key = same partition/clustering key = same row (updated, not duplicated)

**BUT duplicates can occur due to:**

1. **Data Corruption**
   - Disk corruption
   - Bug in Cassandra version
   - Manual data manipulation errors

2. **Schema Changes**
   - Primary key definition changed over time
   - Historical data with old primary key structure

3. **Replication Issues**
   - Split-brain scenarios
   - Replication inconsistencies
   - Node failures during writes

4. **Manual Data Manipulation**
   - Direct SSTable manipulation (dangerous!)
   - CQL COPY issues
   - Migration tool bugs

5. **Timing/Race Conditions**
   - Concurrent writes with same primary key
   - Clock skew issues
   - Tombstone handling bugs

---

## What Does the Validation Step Do?

The validation step would:

1. **Scan the source Cassandra table**
   - Check for duplicate primary keys
   - Count how many duplicates exist
   - Identify which keys are duplicated

2. **Report Findings**
   - Log warnings/errors if duplicates found
   - Provide statistics (total duplicates, affected keys)
   - Optionally fail the migration if duplicates detected

3. **Help Diagnose Issues**
   - Understand if the problem is in source data
   - Determine if you need to clean source data first
   - Decide if you need special handling during migration

---

## How to Detect Duplicates in Cassandra

### Method 1: Using CQL (Limited - Not Practical for Large Tables)

```cql
-- This doesn't work well for large tables due to ALLOW FILTERING performance
SELECT pk_col1, pk_col2, COUNT(*) 
FROM keyspace.table 
GROUP BY pk_col1, pk_col2 
ALLOW FILTERING
HAVING COUNT(*) > 1;
```

**Problem:** `ALLOW FILTERING` is very slow on large tables (108M rows) and may timeout.

---

### Method 2: Using Spark (Recommended for Large Tables)

**Approach:** Read the table in Spark, group by primary key, count occurrences, filter for counts > 1.

**Implementation:**

```scala
// Read Cassandra table into Spark DataFrame
val df = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(cassandraOptions)
  .load()

// Get primary key columns from schema
val primaryKeyColumns = Seq("cmpny_id", "accnt_nbr", "prdct_cde", "pstd_dt", "txn_seq")

// Group by primary key and count
val duplicateCheck = df
  .groupBy(primaryKeyColumns.head, primaryKeyColumns.tail: _*)
  .count()
  .filter($"count" > 1)

// Count duplicates
val duplicateCount = duplicateCheck.count()

if (duplicateCount > 0) {
  logWarn(s"⚠️ WARNING: Found $duplicateCount duplicate primary keys in source table!")
  duplicateCheck.show(100) // Show first 100 duplicates
} else {
  logInfo("✅ No duplicate primary keys found in source table")
}
```

---

### Method 3: Using nodetool and SSTable Tools (Advanced)

**For very large tables, you could:**
- Use `sstablekeys` to extract all keys
- Sort and deduplicate to find duplicates
- Very efficient but requires access to SSTables

---

## When Should You Run This Validation?

### Option A: Pre-Migration Validation (Recommended)

**Run before starting migration:**
- Catches issues early
- Allows you to fix source data before migration
- Saves time (better to fail early than after 50M rows migrated)

**Implementation:**
```bash
# Add validation step before migration starts
./validate_source_data.sh  # Check for duplicates
./run_migration.sh          # Run migration if validation passes
```

---

### Option B: On-Demand Validation

**Run when you encounter duplicate key errors:**
- Helps diagnose the problem
- Understand if source data is the issue
- Decide on fix strategy

---

### Option C: Automated Validation (Integrated)

**Built into migration tool:**
- Optional flag: `migration.validate.sourceDuplicates=true`
- Runs automatically before migration
- Logs warnings or fails migration based on configuration

---

## Implementation Strategy

### Option 1: Simple Spark-Based Validation (Recommended)

Create a validation utility that:
1. Reads source table using Spark
2. Groups by primary key columns
3. Counts occurrences
4. Reports duplicates

**Pros:**
- ✅ Works with existing Spark infrastructure
- ✅ Handles large tables (108M rows)
- ✅ Fast (parallel processing)

**Cons:**
- ⚠️ Requires reading entire table (but only once)
- ⚠️ Uses Spark resources

---

### Option 2: Sampling-Based Validation (Faster)

**For very large tables, sample a percentage:**
- Check 1-10% of data for duplicates
- If samples are clean, assume table is clean
- Trade-off: Not 100% guaranteed, but much faster

---

### Option 3: Integration with Existing Validation Framework

**Add to existing `RowCountValidator` or create new `SourceDataValidator`:**

```scala
object SourceDataValidator {
  def validateNoDuplicatePrimaryKeys(
    spark: SparkSession,
    cassandraConfig: CassandraConfig,
    tableConfig: TableConfig,
    primaryKeyColumns: List[String]
  ): ValidationResult = {
    // Read source table
    // Group by primary key
    // Check for duplicates
    // Return ValidationResult
  }
}
```

---

## Expected Results

### Scenario 1: No Duplicates (Normal Case)

```
✅ Source duplicate validation passed
   - Total rows scanned: 108,000,000
   - Duplicate primary keys found: 0
   - Status: CLEAN
```

### Scenario 2: Duplicates Found (Problem Case)

```
⚠️ WARNING: Source duplicate validation found issues!
   - Total rows scanned: 108,000,000
   - Duplicate primary keys found: 1,234
   - Affected unique keys: 456
   - Status: DUPLICATES_DETECTED
   
   Example duplicates:
   Key: (COMP001, ACC123, PRD456, 2024-01-15, SEQ789) - Count: 2
   Key: (COMP002, ACC456, PRD789, 2024-01-16, SEQ012) - Count: 3
   
   Recommendation: Clean source data before migration
```

---

## What to Do If Duplicates Are Found?

### Option 1: Clean Source Data (Recommended)

**Before migrating:**
1. Identify duplicate records
2. Decide which record to keep (newest? specific criteria?)
3. Delete duplicates from Cassandra
4. Re-run validation to confirm clean
5. Then migrate

### Option 2: Use INSERT ... ON CONFLICT DO UPDATE

**During migration:**
- Use `INSERT ... ON CONFLICT DO UPDATE` instead of `DO NOTHING`
- Last record wins (or use specific logic)
- Migrates all data, resolves duplicates automatically

### Option 3: Skip Duplicates During Migration

**Handle in migration code:**
- Use `INSERT ... ON CONFLICT DO NOTHING`
- Log skipped duplicates
- Continue migration

---

## Integration with Migration Tool

### Configuration Option

```properties
# Source data validation
migration.validation.sourceDuplicates.enabled=true
migration.validation.sourceDuplicates.failOnDuplicates=false  # true = fail migration, false = warn only
migration.validation.sourceDuplicates.samplePercent=100        # 100 = full scan, 10 = 10% sample
```

### Code Integration

```scala
// In MainApp.scala, before migration starts:
if (validationConfig.sourceDuplicatesEnabled) {
  logInfo("Running source duplicate detection...")
  val validationResult = SourceDataValidator.validateNoDuplicatePrimaryKeys(
    spark,
    cassandraConfig,
    tableConfig,
    primaryKeyColumns
  )
  
  if (validationResult.hasDuplicates) {
    if (validationConfig.failOnDuplicates) {
      throw new RuntimeException(s"Source table has ${validationResult.duplicateCount} duplicate primary keys. Migration aborted.")
    } else {
      logWarn(s"⚠️ Source table has ${validationResult.duplicateCount} duplicate primary keys. Migration will continue but may fail.")
    }
  }
}
```

---

## Performance Considerations

### For 108M Records:

| Method | Time Estimate | Resource Usage |
|--------|---------------|----------------|
| Full Scan (Spark) | 5-15 minutes | High (reads entire table) |
| 10% Sample | 1-2 minutes | Low |
| 1% Sample | 10-30 seconds | Very Low |

**Recommendation:** Use sampling for very large tables (10% sample is usually sufficient).

---

## Summary

**Source duplicate detection** = **Check if Cassandra source table has duplicate primary keys before migration**

**Why it matters:**
- Helps identify if source data is the problem
- Prevents wasting time migrating known-bad data
- Helps diagnose duplicate key errors

**Implementation:**
- Use Spark to group by primary key and count
- Can be full scan or sampling-based
- Integrated as optional validation step

**When to use:**
- Pre-migration validation (recommended)
- Diagnostic tool when errors occur
- Continuous monitoring for data quality

---

**Status:** Explanation complete - Ready for implementation if needed

