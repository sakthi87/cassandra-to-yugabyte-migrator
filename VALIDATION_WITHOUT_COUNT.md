# Validation Without COUNT Queries

## Problem

Running `SELECT COUNT(*)` queries on distributed databases (Cassandra/YugabyteDB) can **timeout** on large tables because:
- COUNT requires scanning all partitions/nodes
- Distributed coordination overhead
- Network latency across multiple nodes
- Can take minutes or hours on billion-row tables

## Solution: Use Migration Metrics (CDM Pattern)

Following the `cassandra-data-migrator-main` approach, we **track rows during migration** instead of running COUNT queries at the end.

### How It Works

1. **During Migration**:
   - `Metrics.rowsRead` - Incremented for each row read from Cassandra
   - `Metrics.rowsWritten` - Incremented for each row written to YugabyteDB
   - `Metrics.rowsSkipped` - Incremented for each row skipped (null PK, etc.)

2. **Validation**:
   - Compare `rowsWritten` vs `(rowsRead - rowsSkipped)`
   - No COUNT queries needed!
   - Instant validation (no database queries)

### Code Changes

**Before** (caused timeouts):
```scala
// ❌ This times out on large distributed tables
val cassandraCount = session.execute("SELECT COUNT(*) FROM ...").one().getLong(0)
val yugabyteCount = stmt.executeQuery("SELECT COUNT(*) FROM ...").getLong(1)
```

**After** (uses migration metrics):
```scala
// ✅ Uses counters accumulated during migration
val rowsRead = metrics.getRowsRead
val rowsWritten = metrics.getRowsWritten
val rowsSkipped = metrics.getRowsSkipped
val expectedWritten = rowsRead - rowsSkipped
val matchResult = rowsWritten == expectedWritten
```

## Benefits

1. **No Timeouts**: No COUNT queries
2. **Fast**: Instant validation (just comparing counters)
3. **Accurate**: Counters are accumulated during migration
4. **Production-Grade**: Same pattern as `cassandra-data-migrator-main`

## Validation Logic

```scala
val rowsRead = metrics.getRowsRead        // From Cassandra
val rowsWritten = metrics.getRowsWritten  // To YugabyteDB
val rowsSkipped = metrics.getRowsSkipped  // Skipped (null PK, etc.)

// Expected: All read rows (minus skipped) should be written
val expectedWritten = rowsRead - rowsSkipped
val matchResult = rowsWritten == expectedWritten
```

### Mismatch Scenarios

- **`rowsWritten < expectedWritten`**: Possible write failures (check error logs)
- **`rowsWritten > expectedWritten`**: Possible duplicates or retries
- **`rowsWritten == expectedWritten`**: ✅ Perfect match!

## Optional: COUNT Query Validation (Not Recommended)

If you absolutely need COUNT queries (e.g., for manual verification), use:

```scala
val validator = new RowCountValidator(metrics)
val (cassandraCount, yugabyteCount, matchResult) = 
  validator.validateRowCountWithCountQuery(spark, cassandraConfig, yugabyteConfig, tableConfig)
```

**Warning**: This will timeout on large tables. Use only for small tables or testing.

## Configuration

Validation is controlled by:

```properties
# Enable/disable validation
migration.validation.enabled=true

# Per-table validation
table.validate=true
```

## Example Output

```
WARN: Running validation using migration metrics (no COUNT queries)...
WARN: Row count validation (from metrics): Read=1000000, Written=999950, Skipped=50, Expected=999950, Match=true
WARN: Row count validation passed: 999950 rows migrated successfully
```

## Comparison with cassandra-data-migrator-main

| Feature | cassandra-data-migrator | Our Implementation |
|--------|------------------------|-------------------|
| Row Counting | `JobCounter.READ` / `JobCounter.WRITE` | `Metrics.rowsRead` / `Metrics.rowsWritten` |
| Aggregation | Spark Accumulators (`CDMMetricsAccumulator`) | Spark Accumulators (`LongAccumulator`) |
| Validation | Uses counters, no COUNT queries | Uses counters, no COUNT queries ✅ |
| Timeout Risk | None (no COUNT queries) | None (no COUNT queries) ✅ |

## Summary

✅ **No more COUNT query timeouts!**

The validation now uses migration metrics (counters accumulated during migration) instead of running COUNT queries. This is:
- **Faster**: Instant validation
- **More reliable**: No timeouts
- **Production-grade**: Same pattern as `cassandra-data-migrator-main`

