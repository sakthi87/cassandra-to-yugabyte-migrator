# COPY WITH REPLACE Approach for YugabyteDB

## What You Found

YugabyteDB supports `COPY ... WITH (FORMAT csv, REPLACE)` option that provides **upsert semantics**:
- If a row with the same primary key exists → **REPLACE** it with new data
- If a row doesn't exist → **INSERT** it

This is different from:
- **COPY without REPLACE**: Fails on duplicate primary keys
- **INSERT ... ON CONFLICT DO NOTHING**: Skips duplicates (doesn't update)

---

## Current Approaches Comparison

### Approach 1: COPY (Current)

**What it does:**
- Fast bulk loading using `COPY FROM STDIN`
- Direct streaming to YugabyteDB
- High throughput (30K+ IOPS observed)

**Limitations:**
- ❌ Fails on duplicate primary keys
- ❌ Not idempotent (can't retry safely)
- ❌ No conflict resolution

**Best for:**
- Fresh data loads (no duplicates expected)
- One-time migrations
- When you can truncate table first

---

### Approach 2: INSERT ... ON CONFLICT DO NOTHING (Current)

**What it does:**
- Uses JDBC batched INSERTs
- Idempotent (skips duplicates)
- Can retry safely

**Limitations:**
- ❌ Much slower (500-15K rows/sec vs 30K+ IOPS)
- ❌ Higher latency per batch
- ❌ More database connections/transactions

**Best for:**
- Resumable migrations
- Handling duplicates gracefully
- When retries are needed

---

### Approach 3: COPY WITH REPLACE (New Option)

**What it does:**
- Fast bulk loading like COPY
- Handles duplicates by **replacing** existing rows
- Idempotent (can retry safely)

**Advantages:**
- ✅ COPY performance (fast!)
- ✅ Idempotent (handles duplicates)
- ✅ Can retry/resume safely
- ✅ Best of both worlds (speed + safety)

**Limitations:**
- ⚠️ **REPLACES existing rows** (updates them, doesn't skip)
- ⚠️ Not suitable if you want to **skip** duplicates
- ⚠️ May cause issues with secondary indexes (per YugabyteDB docs)
- ⚠️ May not work on tables with multiple unique constraints

**Best for:**
- Resumable migrations where updates are acceptable
- When duplicates should update existing data
- When you want COPY performance with idempotency

---

## Key Differences

| Feature | COPY | INSERT ... ON CONFLICT DO NOTHING | COPY WITH REPLACE |
|---------|------|-----------------------------------|-------------------|
| **Speed** | ⭐⭐⭐⭐⭐ Very Fast | ⭐⭐ Slow | ⭐⭐⭐⭐⭐ Very Fast |
| **Handles Duplicates** | ❌ Fails | ✅ Skips (doesn't update) | ✅ Replaces (updates) |
| **Idempotent** | ❌ No | ✅ Yes | ✅ Yes |
| **Retry Safe** | ❌ No | ✅ Yes | ✅ Yes |
| **Updates Existing Rows** | ❌ N/A | ❌ No | ✅ Yes |
| **Throughput** | 30K+ IOPS | 500-15K rows/sec | 30K+ IOPS (expected) |

---

## When to Use COPY WITH REPLACE

### ✅ Good Use Cases

1. **Resumable migrations with updates acceptable**
   - You want COPY performance
   - You can tolerate updating existing rows
   - You need idempotency for retries

2. **Data refresh scenarios**
   - You're reloading data and want latest values
   - Updates are preferred over skipping

3. **Initial migration with checkpoint/resume**
   - Fast bulk load
   - Can handle retries safely
   - Don't mind if duplicates update existing data

### ❌ NOT Suitable For

1. **When you need to SKIP duplicates**
   - If you want `ON CONFLICT DO NOTHING` behavior
   - If duplicates should be ignored, not updated

2. **Tables with secondary indexes**
   - YugabyteDB docs warn about potential inconsistencies
   - May need workaround (copy to temp table, then INSERT ... ON CONFLICT)

3. **Tables with multiple unique constraints**
   - REPLACE may not work correctly
   - Need to use INSERT ... ON CONFLICT instead

4. **Audit/audit trail requirements**
   - If you need to preserve original data
   - If updates should be tracked separately

---

## Implementation Approach (High-Level)

### Option 1: Add as Third Mode

**Configuration:**
```properties
yugabyte.insertMode=COPY_REPLACE
# OR
yugabyte.insertMode=COPY|INSERT|COPY_REPLACE
```

**Logic:**
- If `COPY_REPLACE`: Use `COPY ... WITH (FORMAT csv, REPLACE)`
- If `COPY`: Use `COPY ... WITH (FORMAT csv)` (current)
- If `INSERT`: Use `INSERT ... ON CONFLICT DO NOTHING` (current)

**Benefits:**
- Clear separation of modes
- Easy to switch between modes
- Backward compatible

---

### Option 2: Add REPLACE Flag to COPY Mode

**Configuration:**
```properties
yugabyte.insertMode=COPY
yugabyte.copyReplace=true  # Enable REPLACE option
```

**Logic:**
- If `insertMode=COPY` and `copyReplace=true`: Use `COPY ... WITH (FORMAT csv, REPLACE)`
- If `insertMode=COPY` and `copyReplace=false`: Use `COPY ... WITH (FORMAT csv)` (current)
- If `insertMode=INSERT`: Use `INSERT ... ON CONFLICT DO NOTHING` (current)

**Benefits:**
- Simpler configuration (one mode, one flag)
- Backward compatible (default: false)

---

## Recommended Approach

### For Your Current Situation (86M records, duplicate issues)

**Recommendation: COPY WITH REPLACE**

**Why:**
1. ✅ **Fast performance** (COPY speed, not INSERT speed)
2. ✅ **Handles duplicates** (replaces them, making it idempotent)
3. ✅ **Resumable** (can retry safely)
4. ✅ **Solves your duplicate error** (no more primary key violations)

**Considerations:**
1. ⚠️ Verify your table doesn't have secondary indexes (check YugabyteDB docs warning)
2. ⚠️ Ensure REPLACE semantics are acceptable (updates existing rows)
3. ⚠️ Test with a small dataset first to verify behavior

---

## Testing Strategy

### Phase 1: Verify REPLACE Works

1. Test COPY WITH REPLACE on a small table (1000 rows)
2. Insert duplicate primary keys
3. Verify existing rows are **replaced** (not skipped, not failed)
4. Verify performance is similar to COPY (not INSERT)

### Phase 2: Test with Real Data

1. Truncate target table
2. Run migration with COPY_REPLACE mode
3. Verify:
   - No duplicate key errors
   - Performance similar to COPY mode
   - All rows loaded correctly
   - Duplicate rows updated (if any)

### Phase 3: Test Resume/Retry

1. Start migration with COPY_REPLACE
2. Stop it mid-way
3. Resume from checkpoint
4. Verify:
   - No duplicate key errors on resume
   - Duplicate rows handled correctly (replaced)
   - All data loaded correctly

---

## Comparison: Which Mode to Use?

### Use **COPY** when:
- ✅ Fresh data load (no duplicates expected)
- ✅ You can truncate table first
- ✅ One-time migration
- ✅ Maximum speed needed

### Use **INSERT ... ON CONFLICT DO NOTHING** when:
- ✅ You need to **skip** duplicates (not update them)
- ✅ Table has secondary indexes
- ✅ You need audit trail (preserve original data)
- ✅ Speed is less critical

### Use **COPY WITH REPLACE** when:
- ✅ You want COPY performance
- ✅ You need idempotency for retries
- ✅ **Replacing** duplicates is acceptable (updating existing rows)
- ✅ Table has no secondary indexes (or can work around them)
- ✅ Resumable migrations with high performance

---

## Migration Path

### Current State:
- COPY mode: Fast but fails on duplicates
- INSERT mode: Handles duplicates but slow (500 rows/sec issue)

### With COPY WITH REPLACE:
- COPY_REPLACE mode: Fast AND handles duplicates

### Recommended Transition:
1. **Keep COPY mode** (for fresh loads where duplicates aren't expected)
2. **Keep INSERT mode** (for cases where you need to skip, not update)
3. **Add COPY_REPLACE mode** (for resumable migrations with updates)

This gives you **three options** to choose from based on your use case.

---

## Summary

**COPY WITH REPLACE is a great option because:**
- ✅ Gives you COPY performance (fast!)
- ✅ Handles duplicates (idempotent)
- ✅ Solves your duplicate key violation errors
- ✅ Makes retries/resumes safe

**But consider:**
- ⚠️ REPLACE **updates** existing rows (doesn't skip them)
- ⚠️ May have issues with secondary indexes
- ⚠️ Not suitable if you need to preserve original data

**For your 86M record migration:**
- COPY WITH REPLACE seems like the **best fit**
- Fast performance (like COPY)
- Handles duplicates (unlike COPY)
- Resumable (unlike COPY)

**Next step:** Test COPY WITH REPLACE on a small dataset to verify behavior and performance before using it for the full migration.

