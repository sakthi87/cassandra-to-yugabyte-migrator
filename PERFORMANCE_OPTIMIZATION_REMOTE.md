# Performance Optimization for Remote/Production Environment

## Current Performance Issue

- **Local**: 6-7K records/sec
- **Remote/Production**: 3.3K records/sec (50% slower)
- **Environment**: 
  - Migration app: 1 node
  - YugabyteDB: Azure (multiple nodes)
  - Cassandra: On-premise CBC (multiple nodes)

## Root Causes (Likely)

### 1. Network Latency
- **Local**: <1ms latency (same machine/Docker)
- **Remote**: 10-50ms+ latency (cross-network)
- **Impact**: Each query/operation adds latency overhead

### 2. Network Bandwidth
- **Local**: High bandwidth (local network)
- **Remote**: Limited by WAN/VPN bandwidth
- **Impact**: Data transfer bottleneck

### 3. Connection Overhead
- **Local**: Fast connection establishment
- **Remote**: Slower connection establishment, more overhead
- **Impact**: Connection pool inefficiency

### 4. Parallelism Not Optimized
- **Local**: Lower parallelism works fine
- **Remote**: Needs higher parallelism to compensate for latency
- **Impact**: Underutilized resources

## Optimization Strategy

### Phase 1: Network Optimization

#### 1.1 Measure Network Latency
```bash
# Test Cassandra latency
ping -c 10 <cassandra-host>
time nc -zv <cassandra-host> <port>

# Test YugabyteDB latency  
ping -c 10 <yugabyte-host>
time nc -zv <yugabyte-host> <port>
```

**Target**: <10ms latency for optimal performance

#### 1.2 Network Bandwidth
```bash
# Test bandwidth (if iperf available)
iperf3 -c <target-host>
```

**Target**: Sufficient bandwidth for data transfer

### Phase 2: Configuration Optimization

#### 2.1 Spark Configuration (Remote-Optimized)

**Current (Local)**:
```properties
spark.executor.instances=4
spark.executor.cores=4
spark.executor.memory=8g
spark.default.parallelism=16
```

**Recommended (Remote)**:
```properties
# Increase parallelism to compensate for latency
spark.executor.instances=8-16
spark.executor.cores=4-8
spark.executor.memory=8g-16g
spark.default.parallelism=32-64
spark.sql.shuffle.partitions=32-64

# Network timeout adjustments
spark.network.timeout=1200s
spark.executor.heartbeatInterval=120s
```

**Rationale**: 
- Higher parallelism = more concurrent operations
- More operations in parallel = better latency hiding
- Larger executor memory = fewer GC pauses

#### 2.2 Cassandra Configuration (Remote-Optimized)

**Current**:
```properties
cassandra.fetchSizeInRows=10000
cassandra.inputSplitSizeMb=256
cassandra.concurrentReads=2048
```

**Recommended (Remote)**:
```properties
# Reduce fetch size for high latency (fewer round trips per query)
cassandra.fetchSizeInRows=5000-10000

# Smaller splits for better load balancing across network
cassandra.inputSplitSizeMb=128-256

# Increase concurrent reads to compensate for latency
cassandra.concurrentReads=2048-4096

# Increase timeouts for remote connections
cassandra.readTimeoutMs=180000
cassandra.connection.timeoutMs=90000
```

**Rationale**:
- Smaller fetch size = faster individual queries (less data per round trip)
- More concurrent reads = better latency hiding
- Longer timeouts = handle network variability

#### 2.3 YugabyteDB Configuration (Remote-Optimized)

**Current**:
```properties
yugabyte.copyBufferSize=100000
yugabyte.copyFlushEvery=50000
```

**Recommended (Remote)**:
```properties
# Larger buffers for network efficiency
yugabyte.copyBufferSize=50000-100000
yugabyte.copyFlushEvery=25000-50000

# CRITICAL: Use all YugabyteDB nodes for load balancing
yugabyte.host=node1,node2,node3,node4
yugabyte.loadBalanceHosts=true

# Connection settings for remote
yugabyte.connectionTimeout=60000
yugabyte.socketTimeout=0
```

**Rationale**:
- Multiple nodes = better load distribution
- Load balancing = reduces per-node connection overhead
- Larger buffers = fewer network round trips

### Phase 3: Architecture Optimization

#### 3.1 Use Multiple YugabyteDB Nodes

**Current** (likely):
```properties
yugabyte.host=single-node
```

**Recommended**:
```properties
yugabyte.host=yb-node1.azure.com,yb-node2.azure.com,yb-node3.azure.com
yugabyte.loadBalanceHosts=true
```

**Impact**: 2-3x improvement by distributing load

#### 3.2 Connection Pooling Strategy

For remote environments:
- **One connection per Spark partition** (current approach is correct)
- **No connection pooling** (COPY streams are long-lived)
- **Direct connections** (avoids pool overhead)

#### 3.3 Batch Size Optimization

**For High Latency**:
- Smaller batches = faster individual operations
- More batches = better parallelism
- Balance: 25K-50K rows per flush

### Phase 4: Monitoring & Diagnostics

#### 4.1 Run Diagnostic Script
```bash
./scripts/diagnose_performance.sh migration.properties
```

#### 4.2 Monitor During Migration

**Spark UI**: `http://<app-node>:4040`
- Check task execution times
- Identify slow partitions
- Monitor shuffle operations

**Network Monitoring**:
```bash
# Monitor network usage
iftop -i <interface>
# or
nethogs
```

**YugabyteDB Metrics**:
- Check connection count
- Monitor COPY operation latency
- Check node distribution

## Expected Performance Improvements

| Optimization | Expected Improvement | Priority |
|-------------|---------------------|----------|
| Multiple YugabyteDB nodes | +50-100% | HIGH |
| Increase parallelism (32-64) | +30-50% | HIGH |
| Optimize fetch/split sizes | +10-20% | MEDIUM |
| Network optimization | +20-40% | MEDIUM |
| Increase executor memory | +10-15% | LOW |

**Target**: 8-12K records/sec (2-3x current)

## Quick Fix Configuration

Create `migration-remote.properties`:

```properties
# =============================================================================
# Remote/Production Optimized Configuration
# =============================================================================

# Cassandra (On-premise)
cassandra.host=<onprem-cassandra-host>
cassandra.port=9042
cassandra.fetchSizeInRows=5000
cassandra.inputSplitSizeMb=128
cassandra.concurrentReads=2048
cassandra.readTimeoutMs=180000

# YugabyteDB (Azure - USE ALL NODES!)
yugabyte.host=yb-node1.azure.com,yb-node2.azure.com,yb-node3.azure.com
yugabyte.port=5433
yugabyte.loadBalanceHosts=true
yugabyte.copyBufferSize=50000
yugabyte.copyFlushEvery=25000

# Spark (Increased for Remote)
spark.executor.instances=8
spark.executor.cores=4
spark.executor.memory=8g
spark.default.parallelism=32
spark.sql.shuffle.partitions=32
spark.network.timeout=1200s
```

## Testing the Optimizations

1. **Baseline**: Run with current config, measure throughput
2. **Test 1**: Add multiple YugabyteDB nodes
3. **Test 2**: Increase parallelism to 32
4. **Test 3**: Optimize fetch/split sizes
5. **Compare**: Measure improvement at each step

## Troubleshooting

### If Still Slow After Optimizations

1. **Check Network Latency**:
   ```bash
   ping -c 100 <cassandra-host> | tail -1
   ping -c 100 <yugabyte-host> | tail -1
   ```
   If >50ms, network is the bottleneck

2. **Check Network Bandwidth**:
   - Monitor during migration
   - If saturated, bandwidth is the bottleneck

3. **Check CPU Usage**:
   ```bash
   top -p $(pgrep -f spark-submit)
   ```
   If <50%, increase parallelism

4. **Check Memory Usage**:
   ```bash
   free -h
   ```
   If high, increase executor memory

5. **Check YugabyteDB Connections**:
   - Verify load balancing is working
   - Check connection distribution across nodes

## Summary

**Key Actions**:
1. ✅ Use multiple YugabyteDB nodes (CRITICAL)
2. ✅ Increase Spark parallelism to 32-64
3. ✅ Optimize fetch/split sizes for latency
4. ✅ Monitor network latency and bandwidth
5. ✅ Test incrementally and measure improvements

**Expected Result**: 8-12K records/sec (2-3x improvement)

