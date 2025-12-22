# HikariCP "Too Many Clients" Error - Fix Explanation

## ❌ The Error You're Seeing

```
ERROR YugabyteSession: Failed to initialize YugabyteDB connection pool
com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: Failed to initialize pool: FATAL: sorry, too many clients already
```

**This error indicates you're using the OLD codebase**, not the new one!

## 🔍 Root Cause Analysis

### The Error Stack Trace Shows:

```
at com.datastax.cdm.yugabyte.YugabyteSession.initConnectionPool(YugabyteSession.java:451)
at com.datastax.cdm.job.YugabyteCopyJobSession.<init>(YugabyteCopyJobSession.java:96)
at com.datastax.cdm.job.YugabyteMigrate$.$anonfun$execute$3(YugabyteMigrate.scala:49)
```

**These classes are from the OLD codebase:**
- `com.datastax.cdm.yugabyte.YugabyteSession` ❌ (old codebase)
- `com.datastax.cdm.job.YugabyteCopyJobSession` ❌ (old codebase)
- `com.datastax.cdm.job.YugabyteMigrate` ❌ (old codebase)

### The NEW Codebase Uses:

- `com.company.migration.yugabyte.YugabyteConnectionFactory` ✅ (new codebase)
- `com.company.migration.MainApp` ✅ (new codebase)
- **NO HikariCP** ✅ (removed completely)

## ✅ What We Fixed in the New Codebase

### 1. Removed HikariCP Completely

**OLD Codebase (cassandra-data-migrator-main):**
```java
// Uses HikariCP connection pool
HikariDataSource dataSource = new HikariDataSource(config);
// Problem: Each partition creates a pool → 260-300 connections!
```

**NEW Codebase (cassandra-to-yugabyte-migrator):**
```scala
// Direct connections, NO pooling
class YugabyteConnectionFactory {
  def getConnection(): Connection = {
    driver.connect(jdbcUrl, props)  // ✅ Direct connection per partition
  }
}
```

### 2. One Connection Per Partition (Not Per Row)

**OLD Codebase Problem:**
- Each Spark partition creates a HikariCP pool
- Pool size: 3-5 connections per partition
- 80 partitions × 3 connections = 240 connections
- Plus overhead = 260-300 connections ❌

**NEW Codebase Solution:**
- Each Spark partition gets ONE connection
- Connection used for entire COPY operation
- 80 partitions × 1 connection = 80 connections ✅
- Connections closed after partition completes

### 3. No Connection Pooling

**Why No Pooling?**
- ✅ COPY FROM STDIN is long-lived (minutes per connection)
- ✅ Spark already parallelizes at partition level
- ✅ COPY streams are not multiplexable (one stream = one connection)
- ✅ Pooling causes "too many clients" errors
- ✅ Direct connections are simpler and more reliable

## 🔍 How to Verify You're Using the Correct Codebase

### Check 1: JAR File Contents

```bash
# Check for HikariCP (should return NOTHING)
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep -i hikari

# Check for old classes (should return NOTHING)
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep "com/datastax/cdm"

# Check for new classes (should return results)
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep "com/company/migration"
```

### Check 2: Main Class

**OLD Codebase:**
```bash
spark-submit --class com.datastax.cdm.job.YugabyteMigrate ...
```

**NEW Codebase:**
```bash
spark-submit --class com.company.migration.MainApp ...
```

### Check 3: Error Stack Trace

**If you see:**
- `com.datastax.cdm.yugabyte.YugabyteSession` → ❌ OLD codebase
- `com.zaxxer.hikari.pool.HikariPool` → ❌ OLD codebase

**If you see:**
- `com.company.migration.yugabyte.YugabyteConnectionFactory` → ✅ NEW codebase
- `com.company.migration.MainApp` → ✅ NEW codebase

## ✅ Solution: Use the Correct Codebase

### Step 1: Verify JAR File

```bash
# Make sure you're using the NEW JAR
ls -lh cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar

# Check it doesn't have HikariCP
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep -i hikari
# Should return nothing
```

### Step 2: Use Correct Main Class

```bash
# ✅ CORRECT: New codebase
spark-submit \
  --class com.company.migration.MainApp \
  cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties

# ❌ WRONG: Old codebase
spark-submit \
  --class com.datastax.cdm.job.YugabyteMigrate \
  ...
```

### Step 3: Check Connection Count

With the new codebase:
- **Expected connections**: ~80-100 (one per partition)
- **Old codebase**: 260-300 (multiple per partition with pooling)

## 📊 Connection Count Comparison

| Scenario | Old Codebase (HikariCP) | New Codebase (Direct) |
|----------|------------------------|----------------------|
| **80 partitions** | 240-300 connections | 80-100 connections |
| **Connection per partition** | 3-5 (pooled) | 1 (direct) |
| **Connection management** | HikariCP pool | Direct, closed after use |
| **"Too many clients" error** | ❌ Common | ✅ Avoided |

## 🎯 Key Differences

### Old Codebase (cassandra-data-migrator-main)
- ❌ Uses HikariCP connection pooling
- ❌ Each partition creates a pool
- ❌ 260-300 connections for 80 partitions
- ❌ "Too many clients" errors

### New Codebase (cassandra-to-yugabyte-migrator)
- ✅ NO HikariCP (removed completely)
- ✅ Direct connections per partition
- ✅ 80-100 connections for 80 partitions
- ✅ No "too many clients" errors

## Summary

**The error you're seeing means you're using the OLD codebase!**

**To fix:**
1. ✅ Use JAR from `cassandra-to-yugabyte-migrator` (not `cassandra-data-migrator-main`)
2. ✅ Use main class: `com.company.migration.MainApp`
3. ✅ Verify no HikariCP in JAR
4. ✅ Connection count will be much lower (80-100 vs 260-300)

**The new codebase completely eliminates this issue** by removing HikariCP and using direct connections.

