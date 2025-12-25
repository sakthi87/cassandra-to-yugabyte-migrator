# Split Size Parameter Configuration Guide

## Is `cassandra.inputSplitSizeMb` Optional?

**Answer: ✅ YES, it's optional** - but it serves different purposes depending on configuration.

---

## Parameter Hierarchy (Priority Order)

### 1. **`cassandra.inputSplitSizeMb.override`** (Highest Priority)
- **Type:** Optional (commented out by default)
- **Purpose:** Force a specific split size, ignoring all auto-determination
- **When to use:** When you want to manually control split size
- **Example:**
  ```properties
  cassandra.inputSplitSizeMb.override=512
  ```

### 2. **Auto-Determination** (When `autoDetermine=true`)
- **Type:** Enabled by default
- **Purpose:** Dynamically determine optimal split size at runtime
- **Uses:** Table statistics, executor memory, data skew
- **Fallback:** Uses `cassandra.inputSplitSizeMb` if metadata queries fail

### 3. **`cassandra.inputSplitSizeMb`** (Fallback/Initial Hint)
- **Type:** Optional (has default of 64MB in code, but 256MB in properties file)
- **Purpose:** 
  - **When `autoDetermine=true`:** Used as fallback if metadata queries fail
  - **When `autoDetermine=false`:** Used as the actual split size
- **Default in code:** 64MB (if not specified)
- **Default in properties:** 256MB (recommended value)

---

## Current Configuration in `migration.properties`

```properties
# Split size optimization (can be auto-determined at runtime)
# Set cassandra.inputSplitSizeMb.override to force a specific value
# Set cassandra.inputSplitSizeMb.autoDetermine=false to disable auto-determination
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
# cassandra.inputSplitSizeMb.override=512
```

### What This Means:

1. **`cassandra.inputSplitSizeMb=256`**
   - ✅ Set to 256MB
   - Used as fallback when auto-determination fails
   - Could be removed (would default to 64MB in code)

2. **`cassandra.inputSplitSizeMb.autoDetermine=true`**
   - ✅ Auto-determination is enabled
   - Runtime will try to determine optimal split size
   - Falls back to `cassandra.inputSplitSizeMb` (256MB) if metadata unavailable

3. **`cassandra.inputSplitSizeMb.override=512`** (commented out)
   - ❌ Not set (commented)
   - If uncommented, would force 512MB regardless of auto-determination

---

## How It Works in Code

### Code Flow:

```scala
// 1. Check for override (highest priority)
overrideSize match {
  case Some(size) => return size  // Use override, ignore everything else
  case None => // Continue
}

// 2. Check if auto-determination is disabled
if (!autoDetermine) {
  return cassandraConfig.inputSplitSizeMb  // Use property value (or default 64MB)
}

// 3. Try to auto-determine
val tableStats = gatherTableStats(...)
val splitSize = if (tableStats.isDefined) {
  decideBasedOnStats(tableStats, executorMemoryGb)  // Use real data
} else {
  decideBasedOnHeuristic(executorMemoryGb)  // Use heuristic (falls back to 256MB or 512MB)
}
```

### Default Values:

| Parameter | Code Default | Properties Default | Required? |
|-----------|--------------|-------------------|-----------|
| `cassandra.inputSplitSizeMb` | 64MB | 256MB | ❌ Optional |
| `cassandra.inputSplitSizeMb.autoDetermine` | `true` | `true` | ❌ Optional |
| `cassandra.inputSplitSizeMb.override` | `None` | Not set | ❌ Optional |

---

## Recommendations

### ✅ Recommended Configuration (Current)

```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
```

**Why:**
- Auto-determination will try to optimize for each table
- 256MB is a good fallback if metadata queries fail
- Works well for most scenarios

### ✅ Minimal Configuration (Optional Parameters Removed)

```properties
# cassandra.inputSplitSizeMb=256  # Optional - defaults to 64MB in code
cassandra.inputSplitSizeMb.autoDetermine=true
```

**Why:**
- Auto-determination enabled
- If metadata fails, falls back to code default (64MB) - less optimal but still works

### ✅ Manual Override (Disable Auto-Determination)

```properties
cassandra.inputSplitSizeMb=512
cassandra.inputSplitSizeMb.autoDetermine=false
```

**Why:**
- Forces 512MB for all tables
- No runtime overhead
- Use when you know the optimal value

### ✅ Force Specific Value (Override)

```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
cassandra.inputSplitSizeMb.override=1024
```

**Why:**
- Override takes precedence
- Forces 1024MB regardless of auto-determination
- Use for testing or specific requirements

---

## Summary

### Is `cassandra.inputSplitSizeMb` Optional?

**✅ YES** - It's optional, but:

1. **When `autoDetermine=true` (default):**
   - Used as **fallback** if metadata queries fail
   - Recommended to set (256MB is good default)
   - If not set, defaults to 64MB (less optimal)

2. **When `autoDetermine=false`:**
   - Used as **actual split size**
   - Should be set explicitly
   - If not set, defaults to 64MB

3. **When `override` is set:**
   - Ignored completely
   - Override value is used instead

### Best Practice:

✅ **Keep it set to 256MB** as a sensible fallback, even with auto-determination enabled.

---

## Code References

- **Default in code:** `CassandraConfig.scala` line 60: `getIntProperty("cassandra.inputSplitSizeMb", 64)`
- **Auto-determination:** `MainApp.scala` lines 78-94
- **Decision logic:** `SplitSizeDecider.scala` lines 40-95

