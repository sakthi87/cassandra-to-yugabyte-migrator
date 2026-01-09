# INSERT Mode Optimization Guide

## What is Batch Insert?

**Batch Insert** groups multiple INSERT statements together to send them to the database in one network round-trip instead of multiple separate calls.

### Without Batching (Slow):
```sql
INSERT INTO table VALUES (row1);  -- Network call 1
INSERT INTO table VALUES (row2);  -- Network call 2
INSERT INTO table VALUES (row3);  -- Network call 3
```
**Problem:** 109M rows = 109M network round-trips = VERY SLOW

### With Batching (Fast):
```sql
INSERT INTO table VALUES (row1), (row2), (row3), ..., (row1000);  -- Network call 1 (1000 rows)
INSERT INTO table VALUES (row1001), (row1002), ..., (row2000);   -- Network call 2 (1000 rows)
```
**Benefit:** 109M rows with batchSize=5000 = 21,800 network round-trips = MUCH FASTER

### How It Works in Our Code:

1. **Accumulate rows in memory** (up to `batchSize` rows)
2. **Bind all values** to PreparedStatement
3. **Call `addBatch()`** for each row (doesn't send to DB yet)
4. **When batchSize reached**: Call `executeBatch()` to send all rows at once
5. **Database processes** all rows in the batch together

---

## Configuration Parameters

### INSERT Mode Configuration

```properties
# INSERT mode (idempotent, handles duplicates)
yugabyte.insertMode=INSERT

# Batch size for INSERT mode (number of rows to accumulate before sending to database)
# Higher = fewer network calls but more memory usage
# Lower = more network calls but less memory
yugabyte.insertBatchSize=5000

# JDBC parameter (already enabled in code)
# reWriteBatchedInserts=true - Converts batched INSERTs into multi-row INSERT statements
# Provides 10-50x improvement!
```

**Batch Size Tuning Guidelines:**
- **Small rows (< 1KB)**: 5000-10000
- **Medium rows (1-10KB)**: 1000-5000 (recommended: 5000)
- **Large rows (10-100KB)**: 500-1000
- **Very large rows (> 100KB)**: 100-500

---

### Spark Parallelism Configuration

```properties
# Number of parallel partitions/tasks
# Formula: parallelism ≈ (table_size_in_GB / 10) × 10
spark.default.parallelism=200

# Shuffle partitions (should match default.parallelism)
spark.sql.shuffle.partitions=200
```

**Parallelism Guidelines:**
- **Small tables (< 10GB)**: 50-100
- **Medium tables (10-100GB)**: 100-200
- **Large tables (100-500GB)**: 200-400
- **Very large tables (> 500GB)**: 400-800

---

### Spark Executor Configuration

```properties
# CPU cores per executor (controls parallelism within executor)
spark.executor.cores=8

# Memory per executor (heap size)
# Used for data caching, shuffles, and task execution
spark.executor.memory=16g

# Memory overhead per executor (for JVM, OS, etc.)
spark.executor.memoryOverhead=4g

# Number of executor instances
# Total parallel tasks = executor.cores × executor.instances
spark.executor.instances=4

# Driver memory (coordinator/master)
spark.driver.memory=8g
```

**Total Resources Calculation:**
```
Total parallel tasks = executor.cores × executor.instances
                    = 8 × 4 = 32 parallel tasks

Total executor memory = executor.memory × executor.instances
                     = 16g × 4 = 64GB

Total memory = executor memory + driver memory + overhead
            = 64GB + 8GB + (4GB × 4) = 88GB
```

---

### Cassandra Read Configuration

```properties
# Number of rows to fetch from Cassandra in each query
# Larger = fewer queries but more memory usage
cassandra.fetchSizeInRows=50000

# Maximum concurrent reads from Cassandra
# Higher = better parallelism but more connections
cassandra.concurrentReads=4096

# Split size optimization (auto-determined at runtime)
# Controls partition size - larger = fewer partitions
cassandra.inputSplitSizeMb.autoDetermine=true
# Or override:
# cassandra.inputSplitSizeMb.override=512

# Read timeout (milliseconds)
cassandra.readTimeoutMs=120000
```

**Cassandra Read Tuning:**
- **fetchSizeInRows**: 10000-50000 (higher for small rows)
- **concurrentReads**: 2048-8192 (depends on cluster capacity)
- **inputSplitSizeMb**: 256-1024 (auto-determine recommended)

---

### YugabyteDB Connection Configuration

```properties
# Connection pool settings
yugabyte.maxPoolSize=8
yugabyte.minIdle=2
yugabyte.connectionTimeout=30000
yugabyte.idleTimeout=300000
yugabyte.maxLifetime=1800000

# Transaction settings
yugabyte.isolationLevel=READ_COMMITTED
yugabyte.autoCommit=false

# JDBC parameters (in code - already configured)
# reWriteBatchedInserts=true - Converts batch INSERTs to multi-row INSERTs
# preferQueryMode=simple - Avoids server-side prepare overhead
# socketTimeout=0 - No timeout for long-running COPY streams
```

---

## Performance Expectations

### Throughput Estimates

**INSERT Mode:**
- **Batch size 1000**: 10,000-15,000 rows/sec
- **Batch size 5000**: 15,000-25,000 rows/sec (recommended)
- **Batch size 10000**: 20,000-30,000 rows/sec (for small rows only)

**COPY Mode (for comparison):**
- **Throughput**: 25,000-35,000 rows/sec
- **Trade-off**: Faster but fails on duplicates

---

## Configuration Tables by Record Count

### Table 1: 100 Million Records

| Parameter | Value | Notes |
|-----------|-------|-------|
| **INSERT Mode** |
| `yugabyte.insertMode` | `INSERT` | Idempotent mode |
| `yugabyte.insertBatchSize` | `5000` | Optimal for medium rows |
| **Spark Parallelism** |
| `spark.default.parallelism` | `100` | 100 partitions |
| `spark.sql.shuffle.partitions` | `100` | Match parallelism |
| **Spark Executors** |
| `spark.executor.cores` | `8` | 8 cores per executor |
| `spark.executor.memory` | `16g` | 16GB per executor |
| `spark.executor.memoryOverhead` | `4g` | Overhead allocation |
| `spark.executor.instances` | `4` | 4 executors |
| `spark.driver.memory` | `8g` | Driver memory |
| **Cassandra Reads** |
| `cassandra.fetchSizeInRows` | `50000` | Large fetch size |
| `cassandra.concurrentReads` | `4096` | High concurrency |
| `cassandra.inputSplitSizeMb` | `512` (auto) | Auto-determined |
| **Expected Performance** |
| Throughput | 15,000-25,000 rows/sec | Batch size 5000 |
| Duration | ~1.5-2 hours | Estimated time |
| Total Memory | ~88GB | Executors + driver |
| Total Parallel Tasks | 32 | cores × instances |

---

### Table 2: 300 Million Records

| Parameter | Value | Notes |
|-----------|-------|-------|
| **INSERT Mode** |
| `yugabyte.insertMode` | `INSERT` | Idempotent mode |
| `yugabyte.insertBatchSize` | `5000` | Optimal for medium rows |
| **Spark Parallelism** |
| `spark.default.parallelism` | `200` | 200 partitions |
| `spark.sql.shuffle.partitions` | `200` | Match parallelism |
| **Spark Executors** |
| `spark.executor.cores` | `8` | 8 cores per executor |
| `spark.executor.memory` | `16g` | 16GB per executor |
| `spark.executor.memoryOverhead` | `4g` | Overhead allocation |
| `spark.executor.instances` | `6-8` | 6-8 executors (scale up) |
| `spark.driver.memory` | `8g` | Driver memory |
| **Cassandra Reads** |
| `cassandra.fetchSizeInRows` | `50000` | Large fetch size |
| `cassandra.concurrentReads` | `4096` | High concurrency |
| `cassandra.inputSplitSizeMb` | `512` (auto) | Auto-determined |
| **Expected Performance** |
| Throughput | 15,000-25,000 rows/sec | Batch size 5000 |
| Duration | ~3.5-5.5 hours | Estimated time |
| Total Memory | ~128-160GB | Executors + driver |
| Total Parallel Tasks | 48-64 | cores × instances |

---

### Table 3: 500 Million Records

| Parameter | Value | Notes |
|-----------|-------|-------|
| **INSERT Mode** |
| `yugabyte.insertMode` | `INSERT` | Idempotent mode |
| `yugabyte.insertBatchSize` | `5000` | Optimal for medium rows |
| **Spark Parallelism** |
| `spark.default.parallelism` | `300` | 300 partitions |
| `spark.sql.shuffle.partitions` | `300` | Match parallelism |
| **Spark Executors** |
| `spark.executor.cores` | `8` | 8 cores per executor |
| `spark.executor.memory` | `16g` | 16GB per executor |
| `spark.executor.memoryOverhead` | `4g` | Overhead allocation |
| `spark.executor.instances` | `8-10` | 8-10 executors (scale up) |
| `spark.driver.memory` | `8g` | Driver memory |
| **Cassandra Reads** |
| `cassandra.fetchSizeInRows` | `50000` | Large fetch size |
| `cassandra.concurrentReads` | `4096-8192` | Higher concurrency |
| `cassandra.inputSplitSizeMb` | `512-1024` (auto) | Auto-determined |
| **Expected Performance** |
| Throughput | 15,000-25,000 rows/sec | Batch size 5000 |
| Duration | ~5.5-9 hours | Estimated time |
| Total Memory | ~160-200GB | Executors + driver |
| Total Parallel Tasks | 64-80 | cores × instances |

---

### Table 4: 800 Million Records

| Parameter | Value | Notes |
|-----------|-------|-------|
| **INSERT Mode** |
| `yugabyte.insertMode` | `INSERT` | Idempotent mode |
| `yugabyte.insertBatchSize` | `5000` | Optimal for medium rows |
| **Spark Parallelism** |
| `spark.default.parallelism` | `400` | 400 partitions |
| `spark.sql.shuffle.partitions` | `400` | Match parallelism |
| **Spark Executors** |
| `spark.executor.cores` | `8` | 8 cores per executor |
| `spark.executor.memory` | `16g` | 16GB per executor |
| `spark.executor.memoryOverhead` | `4g` | Overhead allocation |
| `spark.executor.instances` | `10-12` | 10-12 executors (scale up) |
| `spark.driver.memory` | `8-16g` | Increase driver memory |
| **Cassandra Reads** |
| `cassandra.fetchSizeInRows` | `50000` | Large fetch size |
| `cassandra.concurrentReads` | `8192` | Maximum concurrency |
| `cassandra.inputSplitSizeMb` | `512-1024` (auto) | Auto-determined |
| **Expected Performance** |
| Throughput | 15,000-25,000 rows/sec | Batch size 5000 |
| Duration | ~9-15 hours | Estimated time |
| Total Memory | ~200-240GB | Executors + driver |
| Total Parallel Tasks | 80-96 | cores × instances |

---

## Memory Calculation Formula

### Per Partition Memory:
```
Memory per partition = (batchSize × avg_row_size) + overhead
                     = (5000 × 2KB) + 10MB
                     = 10MB + 10MB = 20MB per partition
```

### Total Memory Required:
```
Total Memory = (parallelism × memory_per_partition) + 
               (executor.memory × executor.instances) +
               driver.memory +
               (executor.memoryOverhead × executor.instances)
```

### Examples:

**100M records (parallelism=100):**
```
Total = (100 × 20MB) + (16GB × 4) + 8GB + (4GB × 4)
     = 2GB + 64GB + 8GB + 16GB = 90GB
```

**300M records (parallelism=200):**
```
Total = (200 × 20MB) + (16GB × 6) + 8GB + (4GB × 6)
     = 4GB + 96GB + 8GB + 24GB = 132GB
```

**500M records (parallelism=300):**
```
Total = (300 × 20MB) + (16GB × 8) + 8GB + (4GB × 8)
     = 6GB + 128GB + 8GB + 32GB = 174GB
```

**800M records (parallelism=400):**
```
Total = (400 × 20MB) + (16GB × 10) + 16GB + (4GB × 10)
     = 8GB + 160GB + 16GB + 40GB = 224GB
```

---

## Tuning Strategy

### Step 1: Start Conservative
- Begin with recommended settings for your record count
- Monitor performance metrics

### Step 2: Identify Bottlenecks
- **CPU < 60%**: Increase parallelism or executor cores
- **CPU = 100%**: Reduce parallelism or batch size
- **Memory pressure**: Reduce batch size or parallelism
- **Low throughput**: Increase batch size (if memory allows)

### Step 3: Optimize Gradually
- Increase one parameter at a time
- Monitor after each change
- Stop when no improvement

### Step 4: Validate
- Test with sample data first
- Monitor resource usage
- Verify data integrity

---

## Key Takeaways

1. **Batch Insert is Critical**: Reduces network round-trips by 1000-5000x
2. **Parallelism Matters**: More partitions = faster migration (up to a point)
3. **Memory is Important**: Ensure sufficient memory for batching
4. **Monitor Resources**: Watch CPU, memory, and network usage
5. **Start Conservative**: Begin with recommended settings and tune gradually

---

## Comparison: INSERT vs COPY Mode

| Metric | INSERT Mode | COPY Mode |
|--------|-------------|-----------|
| **Throughput** | 15,000-25,000 rows/sec | 25,000-35,000 rows/sec |
| **Duplicate Handling** | ✅ Handles duplicates (ON CONFLICT DO NOTHING) | ❌ Fails on duplicates |
| **Idempotent** | ✅ Yes (safe for retries/resumes) | ❌ No (fails on retry if duplicates) |
| **Performance** | 60-80% of COPY mode | 100% (baseline) |
| **Use Case** | Production migrations, retry scenarios | Initial migrations, clean data |

**Recommendation:**
- Use **INSERT mode** for production migrations with retry/resume requirements
- Use **COPY mode** for one-time migrations with guaranteed clean data

