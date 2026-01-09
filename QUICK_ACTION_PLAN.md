# Quick Action Plan - 86M Row Migration

## 🎯 RECOMMENDATION: Use COPY Mode (Not INSERT)

**Why:**
- ✅ **3-5x faster** than INSERT mode (25K-35K vs 15K-22K rows/sec)
- ✅ **Fresh load** = no duplicates = COPY mode is safe
- ✅ **40-60 minutes** vs 1.5-2 hours for INSERT mode

---

## 📝 Properties File Changes

**Update these settings:**

```properties
# Switch to COPY mode (faster for fresh loads)
yugabyte.insertMode=COPY

# COPY settings (increase buffer for better performance)
yugabyte.copyBufferSize=200000
yugabyte.copyFlushEvery=100000

# Spark config (optimized for your 3 workers × 8 cores × 32GB)
spark.executor.instances=3
spark.executor.cores=6
spark.executor.memory=24g
spark.executor.memoryOverhead=4g
spark.driver.memory=8g
spark.default.parallelism=150
spark.sql.shuffle.partitions=150

# Cross-region timeout (Cassandra on-prem to Azure)
cassandra.readTimeoutMs=180000
```

---

## 🚀 Spark Submit Command

**For YARN:**
```bash
spark-submit \
  --class com.company.migration.MainApp \
  --master yarn \
  --deploy-mode client \
  --driver-memory 8g \
  --executor-memory 24g \
  --executor-cores 6 \
  --num-executors 3 \
  --conf spark.default.parallelism=150 \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties
```

**For Standalone:**
```bash
spark-submit \
  --class com.company.migration.MainApp \
  --master spark://<master-host>:7077 \
  --driver-memory 8g \
  --executor-memory 24g \
  --executor-cores 6 \
  --total-executor-cores 18 \
  --conf spark.default.parallelism=150 \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties
```

---

## ✅ Checklist: What to Check in YBA UI

1. **YSQL Ops/Sec**: Should be **80-120 ops/sec** (COPY mode)
2. **CPU Usage**: **50-80%** per node (good utilization)
3. **Memory Usage**: Reasonable (not maxed out)
4. **Node Status**: All 3 nodes **UP** and healthy
5. **Network I/O**: Check for bottlenecks
6. **Cross-Region Latency**: Cassandra → Azure (20-50ms expected)

---

## 📊 Expected Performance

**COPY Mode:**
- Throughput: **25K-35K rows/sec**
- Time for 86M rows: **40-60 minutes**
- YSQL Ops/Sec: **80-120 ops/sec**

---

## ⚠️ If You Must Use INSERT Mode

**Properties:**
```properties
yugabyte.insertMode=INSERT
yugabyte.insertBatchSize=500  # Increased from 300
```

**Expected:**
- Throughput: **15K-22K rows/sec**
- Time: **1-1.5 hours**
- YSQL Ops/Sec: **50-75 ops/sec**

---

## 🎯 Bottom Line

1. **Use COPY mode** for fresh 86M row load (faster)
2. **Update properties file** with optimized settings
3. **Monitor YBA UI** for 80-120 YSQL ops/sec
4. **Expected time: 40-60 minutes** (COPY mode)

