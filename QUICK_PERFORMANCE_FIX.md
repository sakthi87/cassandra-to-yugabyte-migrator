# Quick Performance Fix for 3.3K records/sec

## Current Situation
- ✅ 3 YugabyteDB nodes already configured
- ❌ Throughput: 3.3K records/sec (50% slower than local 6-7K)
- Environment: App node → Azure YugabyteDB + On-premise Cassandra

## Top 3 Bottlenecks (Most Likely)

### 1. **Spark Parallelism Too Low** (HIGHEST IMPACT)
**Problem**: Local parallelism works, but remote needs more to hide network latency

**Current (likely)**:
```properties
spark.default.parallelism=16
spark.executor.instances=4
```

**Fix**:
```properties
spark.default.parallelism=32-64
spark.sql.shuffle.partitions=32-64
spark.executor.instances=8-16
spark.executor.cores=4-8
```

**Expected**: +30-50% improvement (4.3-5K records/sec)

### 2. **Cassandra Fetch/Split Size Too Large** (MEDIUM IMPACT)
**Problem**: Large batches = slower with network latency

**Current (likely)**:
```properties
cassandra.fetchSizeInRows=10000
cassandra.inputSplitSizeMb=256
```

**Fix**:
```properties
cassandra.fetchSizeInRows=5000
cassandra.inputSplitSizeMb=128
cassandra.concurrentReads=2048
```

**Expected**: +10-20% improvement (3.6-4K records/sec)

### 3. **Network Latency** (CHECK FIRST)
**Problem**: High latency between app node and databases

**Check**:
```bash
ping -c 10 <yugabyte-node>
ping -c 10 <cassandra-node>
```

**If latency >20ms**: Network is the bottleneck
**If latency <10ms**: Configuration is the bottleneck

## Immediate Action Plan

### Step 1: Run Diagnostic
```bash
./scripts/diagnose_slow_performance.sh migration.properties
```

### Step 2: Apply Quick Fixes

Add to your `migration.properties`:

```properties
# =============================================================================
# Performance Optimization for Remote Environment
# =============================================================================

# Spark - Increase parallelism (CRITICAL!)
spark.default.parallelism=32
spark.sql.shuffle.partitions=32
spark.executor.instances=8
spark.executor.cores=4
spark.executor.memory=8g
spark.network.timeout=1200s

# Cassandra - Optimize for network latency
cassandra.fetchSizeInRows=5000
cassandra.inputSplitSizeMb=128
cassandra.concurrentReads=2048
cassandra.readTimeoutMs=180000

# YugabyteDB - Verify settings
yugabyte.loadBalanceHosts=true
yugabyte.copyBufferSize=50000
yugabyte.copyFlushEvery=25000
```

### Step 3: Test and Measure
1. Run migration with new config
2. Measure throughput
3. Check Spark UI for task times
4. Compare with baseline (3.3K)

## Expected Results

| Optimization | Expected Throughput | Cumulative |
|-------------|---------------------|------------|
| Baseline | 3.3K records/sec | 3.3K |
| + Parallelism (32) | 4.3-5K records/sec | 4.3-5K |
| + Fetch/Split optimization | 4.7-6K records/sec | 4.7-6K |
| + Network optimization | 6-8K records/sec | 6-8K |

**Target**: 6-8K records/sec (2x improvement)

## Monitoring During Migration

### Spark UI
- URL: `http://<app-node>:4040`
- Check: Task execution times, shuffle operations
- Look for: Tasks taking >5 seconds (indicates latency issue)

### Network Monitoring
```bash
# Monitor network usage
iftop -i <interface>
# or
nethogs
```

### YugabyteDB Connections
Verify connections are distributed across all 3 nodes:
```sql
SELECT host, COUNT(*) as connections 
FROM pg_stat_activity 
WHERE datname = '<your-database>'
GROUP BY host;
```

## If Still Slow After Fixes

1. **Check Network Bandwidth**: May be saturated
2. **Check App Node Resources**: CPU/Memory constraints
3. **Check YugabyteDB Node Resources**: May be under-resourced
4. **Check Cassandra Node Resources**: May be bottleneck

## Quick Diagnostic Commands

```bash
# Network latency
ping -c 10 <yugabyte-node> | tail -1
ping -c 10 <cassandra-node> | tail -1

# CPU usage
top -p $(pgrep -f spark-submit)

# Memory usage
free -h

# Network bandwidth (if iperf available)
iperf3 -c <target-host>
```

## Summary

**Most Likely Issue**: Spark parallelism too low for remote environment

**Quick Fix**: Increase `spark.default.parallelism` to 32-64

**Expected Improvement**: 2x (from 3.3K to 6-7K records/sec)

