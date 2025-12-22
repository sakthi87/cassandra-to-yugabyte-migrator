# Connection Management - "Too Many Clients" Fix

## ❌ The Error You're Seeing

```
ERROR YugabyteSession: Failed to initialize YugabyteDB connection pool
com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: Failed to initialize pool: FATAL: sorry, too many clients already
```

**This error indicates you're using the OLD codebase**, not the new one!

## 🔍 Critical Issue: Wrong Codebase

### The Error Stack Trace Shows OLD Classes:

```
at com.datastax.cdm.yugabyte.YugabyteSession.initConnectionPool  ❌ OLD
at com.datastax.cdm.job.YugabyteCopyJobSession.<init>            ❌ OLD
at com.datastax.cdm.job.YugabyteMigrate$.$anonfun$execute$3     ❌ OLD
```

**These are from `cassandra-data-migrator-main` (OLD codebase), NOT `cassandra-to-yugabyte-migrator` (NEW codebase)!**

### NEW Codebase Uses Different Classes:

```
com.company.migration.yugabyte.YugabyteConnectionFactory  ✅ NEW
com.company.migration.MainApp                              ✅ NEW
```

## ✅ What We Fixed in the New Codebase

### 1. Removed HikariCP Completely

**OLD Codebase (cassandra-data-migrator-main):**
- ❌ Uses HikariCP connection pooling
- ❌ Each partition creates a HikariCP pool (3-5 connections)
- ❌ 80 partitions × 3-5 connections = 240-400 connections
- ❌ Exceeds YugabyteDB connection limit → "too many clients"

**NEW Codebase (cassandra-to-yugabyte-migrator):**
- ✅ **NO HikariCP** (removed from code AND dependencies)
- ✅ Direct connections per partition (1 connection)
- ✅ 80 partitions × 1 connection = 80 connections
- ✅ Well within connection limits

### 2. Connection Management Strategy

**OLD Codebase:**
```java
// Each partition creates a HikariCP pool
HikariDataSource pool = new HikariDataSource(config);
pool.setMaximumPoolSize(3);  // 3 connections per partition
// 80 partitions × 3 = 240 connections ❌
```

**NEW Codebase:**
```scala
// One direct connection per partition
def getConnection(): Connection = {
  driver.connect(jdbcUrl, props)  // Direct connection
}
// 80 partitions × 1 = 80 connections ✅
```

### 3. Why No Pooling?

- ✅ **COPY FROM STDIN is long-lived** (minutes per connection)
- ✅ **Spark already parallelizes** at partition level
- ✅ **COPY streams are not multiplexable** (one stream = one connection)
- ✅ **Pooling causes "too many clients"** errors
- ✅ **Direct connections are simpler** and more reliable

## 📊 Connection Count Comparison

| Scenario | Old Codebase (HikariCP) | New Codebase (Direct) |
|----------|------------------------|----------------------|
| **80 partitions** | 240-300 connections | 80-100 connections |
| **Connection per partition** | 3-5 (pooled) | 1 (direct) |
| **Connection management** | HikariCP pool | Direct, closed after use |
| **"Too many clients" error** | ❌ Common (260-300 connections) | ✅ Avoided (80-100 connections) |
| **XCluster scenario** | ❌ 260-300 connections | ✅ 80-100 connections |

## 🔍 How to Verify You're Using the Correct Codebase

### Check 1: JAR File Contents

```bash
# Should return NOTHING (no HikariCP)
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep -i hikari

# Should return NOTHING (no old classes)
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep "com/datastax/cdm"

# Should return results (new classes)
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep "com/company/migration"
```

### Check 2: Main Class

**❌ OLD Codebase:**
```bash
spark-submit --class com.datastax.cdm.job.YugabyteMigrate ...
```

**✅ NEW Codebase:**
```bash
spark-submit --class com.company.migration.MainApp ...
```

### Check 3: Error Stack Trace

**If you see these in error:**
- `com.datastax.cdm.yugabyte.YugabyteSession` → ❌ OLD codebase
- `com.zaxxer.hikari.pool.HikariPool` → ❌ OLD codebase

**If you see these in logs:**
- `com.company.migration.yugabyte.YugabyteConnectionFactory` → ✅ NEW codebase
- `com.company.migration.MainApp` → ✅ NEW codebase

## ✅ Solution: Use the Correct Codebase

### Step 1: Use Correct JAR

```bash
# ✅ Use this JAR
cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar

# ❌ NOT this JAR
cassandra-data-migrator-5.5.2-SNAPSHOT.jar
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
  cassandra-data-migrator-5.5.2-SNAPSHOT.jar \
  ...
```

### Step 3: Verify Connection Count

With the new codebase, you should see:
- **Connection count**: 80-100 (one per partition)
- **No "too many clients" errors**
- **Logs show**: `YugabyteConnectionFactory` (not `YugabyteSession`)

## Summary

| Issue | Old Codebase | New Codebase |
|-------|-------------|--------------|
| **Connection Pool** | HikariCP | ❌ None (direct connections) |
| **Connections (80 partitions)** | 260-300 | 80-100 |
| **"Too many clients" error** | ❌ Common | ✅ Fixed |
| **Main Class** | `com.datastax.cdm.job.YugabyteMigrate` | `com.company.migration.MainApp` |
| **Connection Factory** | `YugabyteSession` (HikariCP) | `YugabyteConnectionFactory` (direct) |

**The new codebase completely eliminates this issue** by:
1. ✅ Removing HikariCP completely
2. ✅ Using direct connections (one per partition)
3. ✅ Reducing connection count from 260-300 to 80-100
4. ✅ Avoiding "too many clients" errors

