# What is `decideBasedOnHeuristic`?

## Simple Answer

**`decideBasedOnHeuristic`** is a **fallback decision-making function** that chooses a split size when we **cannot get actual table statistics** from Cassandra.

Think of it as a **"best guess"** based on what we DO know (executor memory), when we DON'T know the table size.

---

## The Two Decision Paths

The code has **two ways** to decide split size:

### 1. **`decideBasedOnStats`** (Preferred - Uses Real Data)
- ✅ **Uses:** Actual table size, partition count, data skew
- ✅ **When:** Metadata queries succeed (`system_schema.tables` or `system.size_estimates`)
- ✅ **Result:** Data-driven, optimal decision

### 2. **`decideBasedOnHeuristic`** (Fallback - Uses Rules of Thumb)
- ⚠️ **Uses:** Only executor memory (what we know for sure)
- ⚠️ **When:** All metadata queries fail or return no data
- ⚠️ **Result:** Conservative "safe" choice

---

## The Actual Code

```scala
private def decideBasedOnHeuristic(executorMemoryGb: Int): Int = {
  logWarn("Using heuristic-based decision (table stats unavailable)")
  
  if (executorMemoryGb >= 8) {
    512  // Conservative for unknown tables
  } else {
    256  // DEFAULT_SPLIT_SIZE (more conservative)
  }
}
```

---

## What "Heuristic" Means

**Heuristic** = A **rule of thumb** or **educated guess** based on experience, not exact data.

### In This Case:

| What We Know | What We Don't Know | Heuristic Rule |
|--------------|-------------------|----------------|
| ✅ Executor has 8GB memory | ❌ Table size | "If executor ≥ 8GB → use 512MB split" |
| ✅ Executor has < 8GB memory | ❌ Table size | "If executor < 8GB → use 256MB split" |

**Why these numbers?**
- **512MB** for 8GB+ executors: Assumes larger tables can handle bigger splits
- **256MB** for smaller executors: More conservative, safer for unknown scenarios

---

## Why It's Called "Heuristic"

### Real-World Analogy

Imagine you're choosing a car:
- **Data-driven approach:** You know the exact distance, road conditions, cargo weight → choose optimal car
- **Heuristic approach:** You only know "I have a big garage" → choose a "safe" car size

### In Our Code

- **`decideBasedOnStats`:** "Table is 200GB, low skew, 8GB executor → use 1024MB split"
- **`decideBasedOnHeuristic`:** "Unknown table size, but executor is 8GB → use 512MB split (safe choice)"

---

## When Is It Used?

### Flow Diagram

```
1. Try system_schema.tables
   ↓ (fails - column not available)
2. Try system.size_estimates
   ↓ (fails - returns 0 or query fails)
3. Try sampling-based estimation
   ↓ (fails - returns 0 or query fails)
4. ✅ Use decideBasedOnHeuristic
   → Returns 256MB or 512MB based on executor memory
```

### In Your Test Case

```
✅ Executor memory: 8GB
❌ Table size: Unknown (all metadata queries failed)
→ But wait... the log shows 256MB, not 512MB?
```

---

## Why Your Test Got 256MB (Not 512MB)

**Important Discovery:** The code actually used `decideBasedOnStats`, NOT `decideBasedOnHeuristic`!

### What Actually Happened:

1. **`gatherTableStats` succeeded** (returned a `TableStats` object)
   - BUT with all zeros: `estimatedSizeGb = 0.0`, `partitionsCount = 0`

2. **Because `tableStats.isDefined = true`**, the code called:
   - `decideBasedOnStats(stats, 8)` ← **NOT** `decideBasedOnHeuristic`

3. **`decideBasedOnStats` logic:**
   ```scala
   if (tableSizeGb < 50) {
     DEFAULT_SPLIT_SIZE  // Returns 256MB
   }
   ```
   - Since `estimatedSizeGb = 0.0`, and `0.0 < 50` is true
   - It returned **256MB** (DEFAULT_SPLIT_SIZE)

### Why This Happened:

The metadata queries (`system_schema.tables`, `system.size_estimates`, sampling) all returned **zero or failed**, but `gatherTableStats` still returned a `TableStats` object (with zeros) instead of `None`.

**This is a subtle bug:** When all stats are zero, we should treat it as "stats unavailable" and use the heuristic instead.

### The Fix (Already Applied):

The code was updated to check if `estimatedSizeGb > 0` before saying "table statistics":

```scala
val decisionMethod = if (tableStats.isDefined && tableStats.get.estimatedSizeGb > 0) {
  "table statistics"
} else {
  "heuristic-based (fallback - metadata unavailable)"
}
```

**But the decision logic still uses `decideBasedOnStats` when `tableStats.isDefined`, even if all values are zero.**

**For your test:** The result was 256MB because `decideBasedOnStats` saw `tableSizeGb = 0.0 < 50GB` and returned `DEFAULT_SPLIT_SIZE = 256MB`.

---

## The Decision Logic (Complete)

```scala
// Step 1: Try to get real statistics
val tableStats = Try(gatherTableStats(...)) match {
  case Success(stats) => Some(stats)  // ✅ Got real data
  case Failure(e) => None             // ❌ Failed, use heuristic
}

// Step 2: Choose decision method
val splitSize = tableStats match {
  case Some(stats) =>
    decideBasedOnStats(stats, executorMemoryGb)  // ✅ Data-driven
  case None =>
    decideBasedOnHeuristic(executorMemoryGb)    // ⚠️ Heuristic fallback
}

// Step 3: Ensure within bounds
val finalSize = math.max(128, math.min(1024, splitSize))
```

---

## Summary

### What `decideBasedOnHeuristic` Does:

1. **Takes:** Executor memory (the only thing we know for sure)
2. **Returns:** A conservative split size (256MB or 512MB)
3. **When:** All metadata queries fail
4. **Why:** Better than a random guess - uses what we know (memory capacity)

### The Logic:

```
If executor memory ≥ 8GB:
  → Use 512MB split (assume larger tables can handle it)
Else:
  → Use 256MB split (more conservative for smaller executors)
```

### Why It's "Heuristic":

- ❌ **Not based on actual table data** (size, partitions, skew)
- ✅ **Based on experience/rules of thumb** (memory capacity → safe split size)
- ✅ **Conservative** (chooses smaller splits when uncertain)

---

## Key Takeaway

> **`decideBasedOnHeuristic` = "When we don't know the table size, make a safe guess based on executor memory"**

It's a **fallback safety mechanism** that ensures we always have a reasonable split size, even when metadata queries fail.

