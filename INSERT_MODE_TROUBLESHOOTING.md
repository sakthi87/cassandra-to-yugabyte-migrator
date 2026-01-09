# INSERT Mode Troubleshooting Guide

## Issue: INSERT mode statements not appearing in logs

If you're not seeing "Partition X completed (INSERT mode)" statements, check:

### 1. Rebuild JAR File (CRITICAL)
**You MUST rebuild the JAR after pulling code changes!**

```bash
mvn clean package -DskipTests
```

Old JAR files don't have the INSERT mode code.

### 2. Verify Properties File
Make sure your properties file has:
```properties
yugabyte.insertMode=INSERT
yugabyte.insertBatchSize=5000
```

### 3. Check Log Level
INSERT mode statements are logged at INFO level:
```properties
# In log4j2.properties or log4j.properties
logger.partitionExecutor.name = com.company.migration.execution.PartitionExecutor
logger.partitionExecutor.level = INFO
```

### 4. Verify JAR Contains INSERT Mode Code
Check if the JAR was built after INSERT mode was added:
```bash
# Check JAR build date
ls -lh target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar

# Should be recent (after INSERT mode code was added)
```

### 5. Properties File Location
Make sure the properties file is:
- In the classpath, OR
- Passed as first argument: `java -jar app.jar migration.properties`

### 6. What You Should See
**INSERT Mode (Correct):**
```
Partition X completed (INSERT mode): Y rows processed, Z rows inserted, W duplicates skipped
```

**COPY Mode (Wrong - old code):**
```
Partition X completed (COPY mode): Y rows written, Z rows skipped, W rows copied by COPY
```

Or older format (no mode specified):
```
Partition completed: Y rows written, Z rows skipped
```

### 7. If Still Not Working
Check the actual log file for:
- Any error messages
- What mode is actually being used
- Whether partitions are completing at all

Look for lines containing "Partition" and "completed" to see what format is being used.
