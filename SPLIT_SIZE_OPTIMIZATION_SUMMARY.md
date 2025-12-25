# Split Size Optimization - Summary & Implementation Plan

## Executive Summary

**Goal:** Automatically determine `cassandra.inputSplitSizeMb` at runtime based on table characteristics, cluster health, and Spark capacity to optimize planning phase performance.

**Key Insight:** Split size is a **Spark-side planning optimization**, NOT a Cassandra memory setting. Larger splits = fewer partitions = faster planning, but requires stable clusters and low data skew.

---

## Understanding `cassandra.inputSplitSizeMb`

### What It Controls

- **Spark-side partitioning**: How Spark slices Cassandra data into work units
- **Planning complexity**: Fewer splits = faster DAG construction
- **Task granularity**: Each split becomes one Spark partition → one task

### What It Does NOT Control

- ❌ Cassandra heap usage
- ❌ Cassandra read buffers
- ❌ Cassandra memory consumption
- ❌ Spark executor memory (directly)

### Memory Impact

Memory usage per Spark partition is **independent** of split size:

| Component | Approx Memory |
|-----------|---------------|
| Cassandra fetch buffer | 10-50 MB |
| CSV encoding buffer | 5-10 MB |
| Spark task overhead | 100-200 MB |
| JDBC/COPY buffers | 1-5 MB |
| **Total per partition** | **~150-300 MB** |

Split size affects **how many partitions** are created, not memory per partition.

---

## Impact Analysis

### Planning Time vs Split Size (for 25M rows, ~2-3 KB/row)

| Split Size | Approx Partitions | Planning Time | Use Case |
|------------|-------------------|---------------|----------|
| 128 MB | 400-600 | ~30 min | Conservative, high skew |
| 256 MB | 200-300 | 18-22 min | Balanced (current default) |
| 512 MB | 100-150 | 8-12 min | **Recommended starting point** |
| 1024 MB | 50-80 | 5-8 min | Stable cluster, low skew |
| 2048 MB | 25-40 | 3-5 min | Too risky, not recommended |

### Trade-offs

**Larger Splits (512-1024 MB):**
- ✅ Faster planning (10-15 min saved for 25M rows)
- ✅ Fewer Spark tasks
- ✅ Lower scheduler overhead
- ⚠️ Longer-running tasks
- ⚠️ Bigger retry cost on failure
- ⚠️ More visible skew if present

**Smaller Splits (128-256 MB):**
- ✅ Better for unstable clusters
- ✅ Better for high skew
- ✅ Faster task retries
- ❌ Slower planning
- ❌ More overhead

---

## Runtime Decision Criteria

### Factors to Consider

1. **Table Size** (estimated)
   - Small (< 50 GB): 256 MB
   - Medium (50-200 GB): 512 MB
   - Large (> 200 GB): 512-1024 MB

2. **Cluster Stability**
   - Stable: Can use larger splits (512-1024 MB)
   - Unstable: Use smaller splits (256 MB)

3. **Data Skew**
   - Low skew (< 1.5x variance): Can use larger splits
   - High skew (> 1.5x variance): Use smaller splits

4. **Executor Memory**
   - < 8 GB: Max 512 MB splits
   - ≥ 8 GB: Can use 1024 MB splits

5. **Row Width**
   - Narrow rows: Can use larger splits
   - Wide rows: May need smaller splits

---

## Implementation Strategy

### Architecture

```
Spark Application (ONE job)
 ├─ Phase 1: Planning & Decision (Driver-only, no Spark tasks)
 │    ├─ Read Cassandra metadata (system_schema)
 │    ├─ Estimate table size
 │    ├─ Sample token ranges for skew detection
 │    ├─ Check cluster health
 │    ├─ Decide split size
 │    └─ Set Spark configs
 │
 └─ Phase 2: Data Migration (Spark DAG created)
      ├─ Cassandra read (.load()) ← Split size locked here
      ├─ Transform to CSV
      ├─ COPY streaming
      └─ Validation
```

### Key Rules

1. ✅ **Configs must be set BEFORE `.load()`**
2. ✅ **Metadata-only queries are safe** (no Spark tasks)
3. ❌ **Never call `.count()` during planning**
4. ✅ **One Spark job is sufficient**

---

## Decision Algorithm

### Pseudo-code

```scala
def determineSplitSizeMb(
  tableSizeGb: Double,
  executorMemoryGb: Int,
  isClusterStable: Boolean,
  skewLevel: Double,
  rowWidthBytes: Int
): Int = {
  
  // Safety checks first
  if (!isClusterStable || skewLevel > 1.5) {
    return 256  // Conservative
  }
  
  // Size-based decision
  if (tableSizeGb < 50) {
    return 256
  } else if (tableSizeGb < 200) {
    return 512
  } else {
    // Large table - check executor capacity
    if (executorMemoryGb >= 8 && skewLevel < 1.2) {
      return 1024
    } else {
      return 512
    }
  }
}
```

### Decision Matrix

| Table Size | Executor Memory | Cluster Stable | Skew Level | Split Size |
|------------|----------------|----------------|------------|------------|
| < 50 GB | Any | Any | Any | 256 MB |
| 50-200 GB | < 8 GB | Any | Any | 512 MB |
| 50-200 GB | ≥ 8 GB | Yes | < 1.5 | 512 MB |
| > 200 GB | < 8 GB | Any | Any | 512 MB |
| > 200 GB | ≥ 8 GB | Yes | < 1.2 | 1024 MB |
| > 200 GB | ≥ 8 GB | Yes | 1.2-1.5 | 512 MB |
| Any | Any | No | Any | 256 MB |
| Any | Any | Any | > 1.5 | 256 MB |

---

## Next Steps

### Phase 1: Basic Implementation
1. Create `SplitSizeDecider` module
2. Implement metadata reading (table size estimation)
3. Add basic decision logic
4. Integrate into `MainApp` before `.load()`

### Phase 2: Skew Detection
1. Sample token ranges (metadata-only)
2. Estimate row counts per range
3. Calculate skew ratio
4. Use in decision logic

### Phase 3: Cluster Health Check
1. Check node status
2. Check pending compactions
3. Check read latency
4. Use in decision logic

### Phase 4: Auto-fallback
1. Monitor task failures
2. Auto-reduce split size on retry
3. Log optimization decisions

---

## Configuration Options

### Properties File Settings

```properties
# Enable/disable runtime split size determination
migration.splitSize.autoDetermine=true

# Override: force a specific split size (disables auto-determination)
# migration.splitSize.override=512

# Safety limits
migration.splitSize.min=128
migration.splitSize.max=1024
migration.splitSize.default=256

# Skew detection
migration.splitSize.skewThreshold=1.5
migration.splitSize.sampleTokenRanges=10
```

---

## Expected Benefits

### For 25M Row Table

| Scenario | Current (256 MB) | Optimized (512 MB) | Optimized (1024 MB) |
|----------|------------------|-------------------|---------------------|
| Planning Time | 18-22 min | 8-12 min | 5-8 min |
| Time Saved | - | **10-14 min** | **13-17 min** |
| Partitions | 200-300 | 100-150 | 50-80 |
| Risk Level | Low | Medium | Medium-High |

### For 100K Row Table (Current Test)

| Scenario | Current (256 MB) | Optimized (512 MB) |
|----------|------------------|-------------------|
| Planning Time | ~3 sec | ~2 sec |
| Partitions | 34 | ~17 |
| Time Saved | - | ~1 sec |

**Note:** Benefits are more significant for larger tables.

---

## Safety Considerations

### Guardrails

1. **Never exceed 1024 MB** (too risky)
2. **Always fallback to 256 MB** if cluster unstable
3. **Monitor first run** with larger splits
4. **Log all decisions** for audit trail
5. **Allow manual override** via properties

### Failure Handling

- If task failures > 5%: Auto-reduce split size
- If planning takes > 30 min: Log warning
- If skew detected > 2.0: Force 256 MB

---

## References

- Spark Cassandra Connector: `spark.cassandra.input.split.sizeInMB`
- Datastax Migrator: Uses conservative fixed splits
- Production best practices: Adaptive tuning > static values

---

## Conclusion

Runtime split size determination is a **high-impact, low-risk optimization** that can save **10-15 minutes of planning time** for large tables while maintaining safety through intelligent decision-making.

**Implementation priority:** High (significant performance gain with minimal risk)

