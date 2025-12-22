# Remote Environment Issues - Fix Guide

## Issues Identified

1. **"too many clients already"** - HikariCP connection pool error
2. **"Ignoring non-Spark config property"** - Using Spark's `--properties-file` flag
3. **loadBalanceHosts** - Works without topology keys for single region

## Root Cause

The errors suggest you might be:
1. Using the wrong JAR file (old codebase with HikariCP)
2. Using Spark's `--properties-file` flag (which only accepts Spark properties)
3. The new codebase doesn't use HikariCP - it uses direct connections

## Solution

### 1. Verify You're Using the Correct JAR

```bash
# Check JAR doesn't contain HikariCP classes
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep -i hikari
# Should return nothing (no HikariCP in new codebase)

# Check it has the correct main class
jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep MainApp
# Should show: com/company/migration/MainApp.class
```

### 2. Correct Way to Run (DO NOT use --properties-file)

**❌ WRONG (causes "Ignoring non-Spark config property" warnings):**
```bash
spark-submit \
  --properties-file migration.properties \  # ❌ This only accepts Spark properties
  --class com.company.migration.MainApp \
  cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar
```

**✅ CORRECT:**
```bash
spark-submit \
  --class com.company.migration.MainApp \
  --master 'local[4]' \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties  # ✅ Pass as argument, not --properties-file
```

### 3. Properties File Location

The properties file can be:
- **In the same directory** as the JAR file
- **Absolute path**: `/path/to/migration.properties`
- **In classpath**: `src/main/resources/migration.properties` (packaged in JAR)

**Example:**
```bash
# Option 1: Same directory
spark-submit ... cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar migration.properties

# Option 2: Absolute path
spark-submit ... cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar /home/user/migration.properties

# Option 3: From classpath (default)
spark-submit ... cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar
# Will use src/main/resources/migration.properties from JAR
```

### 4. Fix "too many clients already" Error

This error should **NOT occur** in the new codebase because:
- ✅ No HikariCP (removed to avoid this exact issue)
- ✅ Direct connections per partition (not pooled)
- ✅ Connections closed after each partition

**If you still see this error:**
1. Check you're using the correct JAR (see step 1)
2. Check YugabyteDB connection limits:
   ```sql
   SHOW max_connections;
   ```
3. Reduce parallelism if needed:
   ```properties
   spark.default.parallelism=8  # Reduce from 16
   ```

### 5. loadBalanceHosts Configuration

**For Single Region (most common):**
```properties
yugabyte.loadBalanceHosts=true
# topologyKeys not needed - works without it
```

**For Multi-Region/Stretch Cluster:**
```properties
yugabyte.loadBalanceHosts=true
yugabyte.topologyKeys=region1.zone1,region2.zone1
```

**The code now handles this correctly** - topology keys are only added if explicitly configured.

## Complete Example Command

```bash
export SPARK_HOME=/path/to/spark-3.5.1

$SPARK_HOME/bin/spark-submit \
  --class com.company.migration.MainApp \
  --master 'local[4]' \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --conf spark.default.parallelism=16 \
  --conf spark.sql.shuffle.partitions=16 \
  cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties
```

## Verification Checklist

- [ ] Using correct JAR: `cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar`
- [ ] NOT using `--properties-file` flag
- [ ] Properties file passed as argument: `migration.properties`
- [ ] Properties file exists and is readable
- [ ] No HikariCP classes in JAR
- [ ] YugabyteDB connection limits sufficient
- [ ] loadBalanceHosts=true (works without topology keys for single region)

## If Issues Persist

1. **Check JAR contents:**
   ```bash
   jar -tf cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar | grep -E "(YugabyteSession|HikariPool)"
   # Should return nothing
   ```

2. **Rebuild JAR:**
   ```bash
   cd /Users/subhalakshmiraj/Documents/cassandra-to-yugabyte-migrator
   mvn clean package -DskipTests
   ```

3. **Check logs for actual error:**
   ```bash
   # Look for "Loading configuration from" message
   # Should show file path or classpath
   ```

## Key Differences from Old Codebase

| Feature | Old Codebase | New Codebase |
|---------|-------------|--------------|
| Connection Pool | HikariCP | Direct connections (no pool) |
| Properties Loading | Spark `--properties-file` | Custom loader (file or classpath) |
| Main Class | `com.datastax.cdm.job.YugabyteMigrate` | `com.company.migration.MainApp` |
| Connection Factory | `YugabyteSession` (HikariCP) | `YugabyteConnectionFactory` (direct) |

