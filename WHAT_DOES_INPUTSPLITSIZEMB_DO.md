# What Does `cassandra.inputSplitSizeMb=256` Do?

## Simple Answer

**`cassandra.inputSplitSizeMb=256`** sets a **fallback value** of **256 MB** for the Cassandra input split size.

**What it actually does depends on `cassandra.inputSplitSizeMb.autoDetermine`:**

---

## Scenario 1: When `autoDetermine=true` (Your Current Setup)

```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
```

### What Happens:

1. **Runtime tries to auto-determine** optimal split size
   - Queries Cassandra metadata (`system_schema.tables`, `system.size_estimates`)
   - Analyzes table size, partitions, data skew
   - Considers executor memory

2. **If auto-determination succeeds:**
   - ✅ Uses the **determined value** (could be 256MB, 512MB, or 1024MB)
   - ❌ **`cassandra.inputSplitSizeMb=256` is IGNORED**

3. **If auto-determination fails:**
   - ⚠️ Falls back to **`cassandra.inputSplitSizeMb=256`**
   - This is your **safety net**

### Example Flow:

```
Step 1: Try to get table statistics
  ↓ Success → Use determined value (e.g., 512MB)
  ↓ Failure → Use fallback (256MB from properties)
```

**In your test case:**
- Auto-determination tried but metadata queries failed
- Fell back to 256MB (but actually used `decideBasedOnStats` with zero stats, which also returned 256MB)

---

## Scenario 2: When `autoDetermine=false`

```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=false
```

### What Happens:

1. **Auto-determination is skipped**
2. **Uses `cassandra.inputSplitSizeMb=256` directly**
3. **No metadata queries, no runtime analysis**

**Result:** Always uses 256MB, regardless of table size or characteristics.

---

## Scenario 3: When `override` is Set

```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
cassandra.inputSplitSizeMb.override=512  # Uncomment this line
```

### What Happens:

1. **Override takes precedence** (highest priority - checked FIRST)
2. **Uses 512MB** (the override value)
3. **`cassandra.inputSplitSizeMb=256` is IGNORED**
4. **Auto-determination is SKIPPED** (never runs)
5. **No metadata queries** (saves ~2-3 seconds)

**Result:** Always uses 512MB, no matter what table size or characteristics.

### When to Use Override:

✅ **Use when:**
- You know the optimal split size for your tables
- You want to skip auto-determination overhead
- You're testing specific split sizes
- You have consistent table sizes across migrations

❌ **Don't use when:**
- Tables vary significantly in size
- You want automatic optimization per table
- You're unsure of the optimal value

---

## Visual Decision Tree

```
Start
  ↓
Is cassandra.inputSplitSizeMb.override set?
  ├─ YES → Use override value (e.g., 512MB) ✅ DONE
  │        (Ignores everything else - highest priority)
  └─ NO
      ↓
Is cassandra.inputSplitSizeMb.autoDetermine=true?
  ├─ YES → Try to auto-determine
  │   ├─ Success → Use determined value (e.g., 512MB) ✅ DONE
  │   └─ Failure → Use cassandra.inputSplitSizeMb (256MB) ✅ DONE
  └─ NO → Use cassandra.inputSplitSizeMb (256MB) ✅ DONE
```

## Understanding `# cassandra.inputSplitSizeMb.override=512`

### What the `#` Means:

The `#` at the beginning means the line is **commented out** (disabled).

```properties
# cassandra.inputSplitSizeMb.override=512  ← Currently DISABLED
```

### To Enable It:

Simply **remove the `#`**:

```properties
cassandra.inputSplitSizeMb.override=512  ← Now ENABLED
```

### What It Does When Enabled:

1. **Highest Priority** - Checked FIRST, before anything else
2. **Forces 512MB** - Always uses 512MB, no exceptions
3. **Skips Auto-Determination** - Never runs metadata queries
4. **Ignores Regular Value** - `cassandra.inputSplitSizeMb=256` is completely ignored
5. **No Runtime Overhead** - Saves ~2-3 seconds (no metadata queries)

### Comparison Table:

| Parameter | Priority | When Used | Can Be Overridden? |
|-----------|----------|-----------|-------------------|
| `cassandra.inputSplitSizeMb.override` | **1 (Highest)** | If set (uncommented) | ❌ No - it IS the override |
| Auto-determination | **2** | If `autoDetermine=true` AND override not set | ✅ Yes - by override |
| `cassandra.inputSplitSizeMb` | **3 (Lowest)** | Fallback if auto-determination fails | ✅ Yes - by override or auto-determination |

### Example Scenarios:

#### Scenario A: Override Enabled
```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
cassandra.inputSplitSizeMb.override=512  # Uncommented
```
**Result:** Always uses **512MB** (override wins)

#### Scenario B: Override Disabled (Current)
```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
# cassandra.inputSplitSizeMb.override=512  # Commented out
```
**Result:** Tries auto-determination, falls back to **256MB** if it fails

#### Scenario C: Override with Auto-Determination Disabled
```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=false
cassandra.inputSplitSizeMb.override=512
```
**Result:** Always uses **512MB** (override still wins, auto-determination setting ignored)

---

## What "Split Size" Actually Means

**`cassandra.inputSplitSizeMb`** controls:
- **How much data** one Spark task reads from Cassandra
- **Number of Spark partitions** created
- **Planning time** (fewer splits = faster planning)

### Example:

| Split Size | Approx Partitions (for 50GB table) | Planning Time |
|------------|-------------------------------------|---------------|
| 128 MB | ~400 partitions | 30 min |
| 256 MB | ~200 partitions | 18-22 min |
| 512 MB | ~100 partitions | 8-12 min |
| 1024 MB | ~50 partitions | 5-8 min |

---

## In Your Current Configuration

```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
```

### What This Means:

1. **Primary:** Runtime will try to determine optimal split size
2. **Fallback:** If determination fails, use 256MB
3. **Purpose:** 256MB is a **safe, conservative default** that works for most tables

### Why 256MB is a Good Fallback:

- ✅ **Balanced:** Not too small (avoids excessive partitions)
- ✅ **Not too large:** Safe for most table sizes
- ✅ **Proven:** Works well for tables from 1GB to 200GB

---

## Can You Remove It?

### Option 1: Keep It (Recommended)

```properties
cassandra.inputSplitSizeMb=256  # Good fallback
cassandra.inputSplitSizeMb.autoDetermine=true
```

**Why:** Provides a sensible fallback if metadata queries fail.

### Option 2: Remove It

```properties
# cassandra.inputSplitSizeMb=256  # Removed
cassandra.inputSplitSizeMb.autoDetermine=true
```

**What happens:**
- If auto-determination fails, falls back to **code default: 64MB**
- 64MB is less optimal (creates more partitions, slower planning)
- **Not recommended** for production

---

## Summary

### What `cassandra.inputSplitSizeMb=256` Does:

| Scenario | Role |
|----------|------|
| **`autoDetermine=true`** | **Fallback value** (used only if auto-determination fails) |
| **`autoDetermine=false`** | **Actual value** (used directly, no auto-determination) |
| **`override` set** | **Ignored** (override takes precedence) |

### What `cassandra.inputSplitSizeMb.override=512` Does:

| Status | Effect |
|--------|--------|
| **Commented (`#`)** | **Disabled** - Has no effect, normal flow continues |
| **Uncommented** | **Forces 512MB** - Highest priority, ignores everything else |

### In Your Current Case:

```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
# cassandra.inputSplitSizeMb.override=512  ← Commented (disabled)
```

- ✅ **Auto-determination enabled** → Tries to optimize at runtime
- ✅ **256MB as fallback** → Safe default if optimization fails
- ✅ **Override disabled** → Normal auto-determination flow
- ✅ **Good configuration** → Best of both worlds (optimization + safety net)

### If You Uncomment Override:

```properties
cassandra.inputSplitSizeMb=256
cassandra.inputSplitSizeMb.autoDetermine=true
cassandra.inputSplitSizeMb.override=512  ← Uncommented (enabled)
```

- ⚠️ **Override enabled** → Always uses 512MB
- ❌ **Auto-determination skipped** → No runtime optimization
- ❌ **256MB ignored** → Override takes precedence
- ⚡ **Faster startup** → No metadata queries (~2-3 seconds saved)

---

## Key Takeaways

> **`cassandra.inputSplitSizeMb=256`** is your "safety net" - it's used when auto-determination can't determine an optimal value, ensuring you always have a reasonable split size.

> **`cassandra.inputSplitSizeMb.override=512`** (when uncommented) is your "force override" - it bypasses everything and always uses the specified value. Use it when you know the optimal value and want to skip auto-determination overhead.

