# YugabyteDB Session Parameters Analysis

## Parameters in Question

1. **`yb_enable_upsert_mode = on`**
2. **`yb_disable_transactional_mode = on`**

These are YugabyteDB-specific session parameters that can be set before executing COPY or INSERT operations.

---

## Parameter 1: `yb_enable_upsert_mode`

### What It Does

**Enables upsert mode** at the session level for YugabyteDB operations.

**Effect:**
- Converts `INSERT` statements into upsert semantics
- Similar to `INSERT ... ON CONFLICT DO UPDATE` behavior
- Can improve performance for upsert operations

### Usage

```sql
SET yb_enable_upsert_mode = on;
INSERT INTO table (col1, col2) VALUES (1, 'value');
-- This becomes an upsert (insert or update if exists)
```

### Impact on Operations

#### COPY Operations
- **COPY (standard)**: Not directly affected (COPY has its own semantics)
- **COPY WITH REPLACE**: Already provides upsert semantics, may be redundant
- **Note**: COPY command semantics are independent of session parameters

#### INSERT Operations
- **INSERT statements**: Becomes upsert behavior
- **INSERT ... ON CONFLICT DO NOTHING**: May have different behavior
- **Batched INSERTs**: Each INSERT becomes an upsert

### Performance Implications
- Can improve performance for upsert scenarios
- Avoids explicit `ON CONFLICT` clauses
- Reduces SQL complexity

---

## Parameter 2: `yb_disable_transactional_mode`

### What It Does

**Disables transactional mode** for the session, enabling faster bulk operations.

**Effect:**
- Disables ACID transaction guarantees for performance
- Faster bulk loading operations
- Reduced transaction overhead
- **Trade-off**: Less data safety guarantees

### Usage

```sql
SET yb_disable_transactional_mode = on;
COPY table FROM STDIN WITH (FORMAT csv);
-- Faster but with reduced transactional guarantees
```

### Impact on Operations

#### COPY Operations
- **Performance**: Significantly faster COPY operations
- **Trade-off**: Reduced transactional guarantees
- **Use case**: Bulk data loading where speed is more important than transaction safety

#### INSERT Operations
- **Performance**: Faster INSERT operations
- **Trade-off**: No transaction rollback capability
- **Risk**: Partial failures may leave inconsistent state

### Performance Implications
- ⭐⭐⭐⭐⭐ **Much faster** bulk operations
- ⚠️ **Reduced safety**: No transaction guarantees
- ⚠️ **Use carefully**: Only for bulk loads where speed > safety

---

## Combination Analysis

### Using Both Parameters Together

```sql
SET yb_enable_upsert_mode = on;
SET yb_disable_transactional_mode = on;
COPY table FROM STDIN WITH (FORMAT csv, REPLACE);
```

**Effect:**
- Upsert semantics enabled
- Transactional mode disabled (faster)
- COPY WITH REPLACE (explicit upsert)

**Potential Conflicts:**
- `yb_enable_upsert_mode` may be redundant with `COPY WITH REPLACE`
- Both aim for similar goals (upsert behavior)
- `yb_disable_transactional_mode` is the performance booster

---

## How to Use in Migration Tool

### Option 1: Set Per Connection (Recommended)

**Before COPY/INSERT operations, execute:**
```sql
SET yb_disable_transactional_mode = on;
-- Optionally: SET yb_enable_upsert_mode = on; (if using INSERT, not COPY WITH REPLACE)
```

**Where to set:**
- After connection creation
- Before COPY/INSERT execution
- Per connection (each Spark partition has its own connection)

### Option 2: Set via JDBC Connection Properties

**JDBC URL parameters:**
```
jdbc:postgresql://host:port/database?yb_enable_upsert_mode=true&yb_disable_transactional_mode=true
```

**Or set via connection properties:**
```java
Properties props = new Properties();
props.setProperty("yb_enable_upsert_mode", "on");
props.setProperty("yb_disable_transactional_mode", "on");
Connection conn = DriverManager.getConnection(url, props);
```

---

## Recommendations by Operation Type

### COPY (Standard)

**Recommended settings:**
```sql
SET yb_disable_transactional_mode = on;
-- yb_enable_upsert_mode: NOT needed (COPY has no upsert by default)
```

**Benefits:**
- ✅ Faster COPY operations
- ✅ Reduced transaction overhead
- ✅ Better performance for bulk loads

**Trade-offs:**
- ⚠️ Reduced transactional guarantees
- ⚠️ Partial failures may leave inconsistent state

---

### COPY WITH REPLACE

**Recommended settings:**
```sql
SET yb_disable_transactional_mode = on;
-- yb_enable_upsert_mode: NOT needed (REPLACE already provides upsert)
```

**Benefits:**
- ✅ Faster COPY WITH REPLACE operations
- ✅ REPLACE already provides upsert semantics
- ✅ Best performance for idempotent bulk loads

**Trade-offs:**
- ⚠️ Reduced transactional guarantees
- ⚠️ Use only when speed > transaction safety

---

### INSERT ... ON CONFLICT DO NOTHING

**Recommended settings:**
```sql
SET yb_disable_transactional_mode = on;
-- yb_enable_upsert_mode: NOT needed (ON CONFLICT already provides upsert semantics)
```

**Benefits:**
- ✅ Faster INSERT operations
- ✅ ON CONFLICT already provides conflict handling
- ✅ Better performance than transactional mode

**Trade-offs:**
- ⚠️ Reduced transactional guarantees
- ⚠️ Partial batch failures may leave inconsistent state

---

### INSERT (Plain, with yb_enable_upsert_mode)

**Recommended settings:**
```sql
SET yb_enable_upsert_mode = on;
SET yb_disable_transactional_mode = on;
```

**Benefits:**
- ✅ Upsert behavior without explicit ON CONFLICT clause
- ✅ Faster than transactional mode
- ✅ Simpler SQL (no ON CONFLICT needed)

**Trade-offs:**
- ⚠️ Less explicit than ON CONFLICT clause
- ⚠️ Reduced transactional guarantees

---

## Performance Impact Summary

| Operation | Standard Mode | With yb_disable_transactional_mode | Performance Gain |
|-----------|---------------|-----------------------------------|------------------|
| **COPY** | Baseline | Much faster | ⭐⭐⭐⭐⭐ High |
| **COPY WITH REPLACE** | Baseline | Much faster | ⭐⭐⭐⭐⭐ High |
| **INSERT ... ON CONFLICT** | Baseline | Faster | ⭐⭐⭐⭐ Medium-High |
| **INSERT (plain)** | Baseline | Faster | ⭐⭐⭐ Medium |

**Key Insight:**
- `yb_disable_transactional_mode` provides the biggest performance boost
- `yb_enable_upsert_mode` is more about semantics than performance
- Both parameters trade safety for speed

---

## Safety Considerations

### When to Use `yb_disable_transactional_mode`

**✅ Safe to use:**
- Bulk data loading (one-time migrations)
- When you can truncate and reload on failure
- When speed is more important than transaction safety
- When you have checkpoint/resume capability

**❌ NOT safe to use:**
- Production transactional workloads
- When you need ACID guarantees
- When partial failures are unacceptable
- When data consistency is critical

### When to Use `yb_enable_upsert_mode`

**✅ Safe to use:**
- When you want upsert semantics for INSERT
- When replacing explicit ON CONFLICT clauses
- With INSERT operations (not needed for COPY WITH REPLACE)

**❌ NOT needed:**
- With COPY WITH REPLACE (redundant)
- With INSERT ... ON CONFLICT (redundant)

---

## Implementation Approach

### Step 1: Add Configuration Properties

```properties
# YugabyteDB session parameters
yugabyte.disableTransactionalMode=false
yugabyte.enableUpsertMode=false
```

### Step 2: Set Parameters After Connection

**In YugabyteConnectionFactory or PartitionExecutor:**
```scala
// After getting connection
if (yugabyteConfig.disableTransactionalMode) {
  val stmt = connection.createStatement()
  stmt.execute("SET yb_disable_transactional_mode = on;")
  stmt.close()
}

if (yugabyteConfig.enableUpsertMode && insertMode == "INSERT") {
  val stmt = connection.createStatement()
  stmt.execute("SET yb_enable_upsert_mode = on;")
  stmt.close()
}
```

### Step 3: Apply Before Operations

**Set parameters:**
- After connection creation
- Before COPY/INSERT execution
- Once per connection (not per batch)

---

## Recommended Configuration by Use Case

### Use Case 1: Fast Bulk Load (COPY)

```properties
yugabyte.insertMode=COPY
yugabyte.disableTransactionalMode=true
yugabyte.enableUpsertMode=false
```

**Result:**
- Fastest COPY performance
- No upsert (fails on duplicates)
- Best for fresh data loads

---

### Use Case 2: Fast Idempotent Load (COPY WITH REPLACE)

```properties
yugabyte.insertMode=COPY_REPLACE  # (to be implemented)
yugabyte.disableTransactionalMode=true
yugabyte.enableUpsertMode=false  # Not needed (REPLACE handles it)
```

**Result:**
- Fastest COPY performance with idempotency
- REPLACE handles duplicates
- Best for resumable migrations

---

### Use Case 3: Safe Idempotent Load (INSERT ... ON CONFLICT)

```properties
yugabyte.insertMode=INSERT
yugabyte.disableTransactionalMode=true  # Optional: for performance
yugabyte.enableUpsertMode=false  # Not needed (ON CONFLICT handles it)
```

**Result:**
- Idempotent INSERT operations
- Faster with disableTransactionalMode
- Safe for resumable migrations

---

## Summary

### Key Findings

1. **`yb_disable_transactional_mode = on`**:
   - ⭐⭐⭐⭐⭐ **Biggest performance boost** for bulk operations
   - ⚠️ **Trades safety for speed** (no transaction guarantees)
   - ✅ **Recommended for bulk migrations** (if acceptable trade-off)

2. **`yb_enable_upsert_mode = on`**:
   - Provides upsert semantics for INSERT
   - ⚠️ **Redundant with COPY WITH REPLACE**
   - ⚠️ **Redundant with INSERT ... ON CONFLICT**
   - ✅ **Useful for plain INSERT statements**

### Recommended Approach

**For COPY WITH REPLACE:**
- ✅ Use: `SET yb_disable_transactional_mode = on;`
- ❌ Skip: `yb_enable_upsert_mode` (REPLACE already provides upsert)

**For INSERT ... ON CONFLICT DO NOTHING:**
- ✅ Use: `SET yb_disable_transactional_mode = on;` (optional, for performance)
- ❌ Skip: `yb_enable_upsert_mode` (ON CONFLICT already provides upsert)

**For Plain INSERT:**
- ✅ Use: `SET yb_enable_upsert_mode = on;` (if you want upsert behavior)
- ✅ Use: `SET yb_disable_transactional_mode = on;` (for performance)

### Bottom Line

**`yb_disable_transactional_mode = on` is the key parameter for performance.**
- Use it for bulk migrations where speed > transaction safety
- Set it per connection before COPY/INSERT operations
- Provides significant performance boost

**`yb_enable_upsert_mode = on` is less critical.**
- Only needed for plain INSERT statements (without ON CONFLICT)
- Redundant with COPY WITH REPLACE or INSERT ... ON CONFLICT
- Provides upsert semantics but not significant performance boost

---

## Next Steps

1. **Test `yb_disable_transactional_mode`** with COPY operations
2. **Measure performance impact** (should be significant)
3. **Verify behavior** with COPY WITH REPLACE
4. **Add configuration option** to enable/disable per migration
5. **Document trade-offs** (speed vs. safety)

