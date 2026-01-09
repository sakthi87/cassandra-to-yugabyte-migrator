# Duplicate Key Violation Analysis

## Problem Statement

When migrating 108M records from Cassandra to YugabyteDB, duplicate key violations occur intermittently. The error is:
```
ERROR: duplicate key value violates unique constraint "<table>_pkey"
```

## Root Causes Identified

### 1. **COPY FROM STDIN Doesn't Support Conflict Resolution** ⚠️ CRITICAL

**Current Implementation:**
- Uses `COPY FROM STDIN` command
- No `ON CONFLICT` clause support in COPY
- Any duplicate key causes the entire COPY operation to fail

**Why This Causes Intermittent Failures:**
- If source data has duplicates (rare but possible)
- If resume logic re-inserts already-migrated data
- If multiple partitions process overlapping token ranges

**Location:** `CopyStatementBuilder.scala`, `CopyWriter.scala`

---

### 2. **Resume Logic Re-inserts Partial Data** ⚠️ CRITICAL

**Problem:**
- When a partition fails partway through, checkpoint marks it as `FAIL`
- On resume, the entire partition is retried from the beginning
- If some data was committed before failure, retry causes duplicates

**Example Scenario:**
1. Partition processes 50K rows, commits successfully
2. Partition fails at row 51K (network error, timeout, etc.)
3. Transaction rolls back ONLY the current batch/transaction
4. Resume retries entire partition (100K rows)
5. First 50K rows cause duplicate key errors

**Location:** `TableMigrationJob.scala` lines 58-77, `PartitionExecutor.scala` lines 132-149

---

### 3. **No Idempotency/Deduplication** ⚠️ HIGH PRIORITY

**Missing Features:**
- No `ON CONFLICT DO NOTHING` equivalent for COPY
- No pre-check to skip already-inserted records
- No upsert logic (INSERT ... ON CONFLICT DO UPDATE)

**Impact:**
- Retries always cause duplicates
- Cannot safely resume failed migrations
- Multiple runs insert same data repeatedly

---

### 4. **Transaction Boundary Issues** ⚠️ MEDIUM PRIORITY

**Current Behavior:**
- Each partition uses a single transaction
- COPY operation is atomic per partition
- If partition fails, all data in that transaction rolls back
- BUT: If failure happens AFTER commit but BEFORE checkpoint update, data remains

**Race Condition:**
1. Partition completes COPY, commits transaction
2. Checkpoint update fails (network error, DB error)
3. Partition marked as FAIL in checkpoint
4. Resume retries partition → duplicates

**Location:** `PartitionExecutor.scala` lines 132-134, 139-148

---

### 5. **Source Data Duplication** ⚠️ LOW PRIORITY (Less Likely)

**Possible Causes:**
- Source Cassandra table has duplicate primary keys (very rare in Cassandra)
- Data corruption or manual inserts with duplicate keys
- Token range overlap in Spark partitions

**How to Check:**
```sql
-- In YugabyteDB, check for existing duplicates before migration
SELECT <primary_key_columns>, COUNT(*) 
FROM <target_table> 
GROUP BY <primary_key_columns> 
HAVING COUNT(*) > 1;
```

---

## Recommended Solutions

### Solution 1: Use INSERT ... ON CONFLICT DO NOTHING (Best for Resume Safety) ✅ RECOMMENDED

**Approach:**
- Replace `COPY FROM STDIN` with batched `INSERT ... ON CONFLICT DO NOTHING`
- Allows safe retries (skips duplicates)
- Maintains high throughput with batching

**Pros:**
- ✅ Idempotent (safe for retries)
- ✅ No duplicate key errors
- ✅ Works with resume logic
- ✅ Can handle source duplicates

**Cons:**
- ⚠️ Slightly slower than COPY (but still fast with batching)
- ⚠️ Requires changing from COPY to INSERT

**Implementation:**
```sql
INSERT INTO schema.table (col1, col2, ...) 
VALUES (val1, val2, ...), (val3, val4, ...), ...
ON CONFLICT (pk_col1, pk_col2, ...) DO NOTHING;
```

---

### Solution 2: Pre-delete Before Resume (Quick Fix) ⚠️ PARTIAL FIX

**Approach:**
- Before resuming a failed partition, delete its data from target
- Then retry the partition

**Pros:**
- ✅ Simple to implement
- ✅ Works with current COPY implementation
- ✅ No schema changes needed

**Cons:**
- ⚠️ Requires identifying which records belong to a partition (complex)
- ⚠️ Token range tracking needed
- ⚠️ Deletes can be slow for large partitions
- ⚠️ Doesn't handle source duplicates

---

### Solution 3: Temporary Table + MERGE (Alternative) ⚠️ COMPLEX

**Approach:**
1. COPY to temporary table (no constraints)
2. Use `INSERT ... ON CONFLICT DO UPDATE` from temp table to final table
3. Drop temp table

**Pros:**
- ✅ Fast COPY to temp table
- ✅ Handles duplicates in merge step
- ✅ Idempotent

**Cons:**
- ⚠️ Requires extra storage (temp table)
- ⚠️ Two-step process (slower overall)
- ⚠️ More complex implementation

---

### Solution 4: Enhanced Checkpoint Tracking (Complementary) ✅ RECOMMENDED

**Approach:**
- Track last successfully committed row/token per partition
- Resume from last committed position (not from beginning)
- Only retry uncommitted data

**Pros:**
- ✅ Minimizes re-processing
- ✅ Reduces duplicate risk
- ✅ More efficient resumes

**Cons:**
- ⚠️ Complex to implement (token range tracking)
- ⚠️ Requires fine-grained checkpointing
- ⚠️ Doesn't solve source duplicates

**Location:** `CheckpointManager.scala`, `TableMigrationJob.scala`

---

## Immediate Actions (Quick Fixes)

### 1. **Truncate Before Each Run** (Current Behavior)
- ✅ Already implemented in `MainApp.scala` line 125
- ✅ Prevents duplicates between runs
- ❌ Doesn't help with resume scenarios

### 2. **Check Source for Duplicates**
```sql
-- Run in Cassandra to check for duplicates
SELECT <partition_key>, <clustering_key>, COUNT(*) 
FROM <keyspace>.<table> 
GROUP BY <partition_key>, <clustering_key> 
ALLOW FILTERING 
HAVING COUNT(*) > 1;
```

### 3. **Verify Resume Logic**
- Check checkpoint status before resuming
- Ensure failed partitions are properly identified
- Consider manual cleanup of partially completed partitions

---

## Best Approach: Hybrid Solution ✅

**Recommended Combination:**

1. **Primary:** Switch to `INSERT ... ON CONFLICT DO NOTHING` with batching
   - Provides idempotency
   - Handles all duplicate scenarios
   - Safe for resume

2. **Secondary:** Enhance checkpoint tracking
   - Track last committed token per partition
   - Resume from last position (minimize re-processing)

3. **Tertiary:** Add deduplication option
   - Pre-check existing keys before insert
   - Skip already-migrated records

---

## Implementation Priority

1. **HIGH PRIORITY:** Implement `INSERT ... ON CONFLICT DO NOTHING`
   - Solves the core problem
   - Safe for production
   - Works with resume logic

2. **MEDIUM PRIORITY:** Enhance checkpoint tracking
   - Reduces unnecessary re-processing
   - Improves resume efficiency

3. **LOW PRIORITY:** Source duplicate detection
   - Validate data quality
   - Understand root cause

---

## Code Changes Required

### 1. Create UpsertStatementBuilder (similar to CopyStatementBuilder)
### 2. Modify PartitionExecutor to use INSERT batches instead of COPY
### 3. Add ON CONFLICT DO NOTHING clause
### 4. Implement batch INSERT logic (maintain throughput)
### 5. Update checkpoint logic for finer-grained tracking

---

## Performance Considerations

- **Batch INSERT with ON CONFLICT:** ~80-90% of COPY throughput
- **Benefits:** Idempotency, safety, no duplicate errors
- **Trade-off:** Slight performance reduction for reliability

---

## Testing Strategy

1. Test with duplicate source data
2. Test resume after partial completion
3. Test concurrent partition execution
4. Load test with 108M records
5. Verify no duplicate key errors

---

**Status:** Analysis complete - Ready for implementation decision

