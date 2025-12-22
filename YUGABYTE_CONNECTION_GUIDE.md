# YugabyteDB Connection Configuration Guide

## Topology Keys Requirements

### ✅ When Topology Keys Are Needed

| Deployment Type | Topology Keys Required? | Example |
|----------------|----------------------|---------|
| **Single Region** | ❌ **NO** | Single datacenter, all nodes in same region |
| **XCluster (Replication)** | ❌ **NO** | Cross-cluster replication, but connections to one cluster |
| **Stretch Cluster** | ✅ **YES** | Multi-region deployment (e.g., US-East, US-West, EU) |

### Single Region Deployment

**Configuration:**
```properties
yugabyte.host=node1,node2,node3
yugabyte.loadBalanceHosts=true
# topologyKeys NOT needed - leave empty or don't set
```

**What Happens:**
- ✅ Load balancing works across all nodes
- ✅ Driver automatically discovers all nodes
- ✅ Connections distributed evenly
- ✅ No topology keys needed

### XCluster (Cross-Cluster Replication)

**Configuration:**
```properties
# Connect to PRIMARY cluster only
yugabyte.host=primary-node1,primary-node2,primary-node3
yugabyte.loadBalanceHosts=true
# topologyKeys NOT needed - single cluster connection
```

**What Happens:**
- ✅ Connect to one cluster (primary)
- ✅ Load balancing works within that cluster
- ✅ Replication handled by YugabyteDB (not JDBC)
- ✅ No topology keys needed

### Stretch Cluster (Multi-Region)

**Configuration:**
```properties
# All nodes from all regions
yugabyte.host=us-east-1,us-west-1,eu-west-1
yugabyte.loadBalanceHosts=true
yugabyte.topologyKeys=aws.us-east-1.zone1,aws.us-west-1.zone1,aws.eu-west-1.zone1
```

**What Happens:**
- ✅ Load balancing with region awareness
- ✅ Driver prioritizes connections to specified regions
- ✅ Topology keys required for optimal performance

## YugabyteDB Host Configuration

### ✅ Provide All Nodes (Recommended)

**For Load Balancing:**
```properties
# Single region - all nodes
yugabyte.host=node1,node2,node3

# Multi-region - all nodes from all regions
yugabyte.host=us-east-1,us-west-1,eu-west-1
```

**Benefits:**
- ✅ Driver discovers all nodes automatically
- ✅ Load balancing works optimally
- ✅ Automatic failover if node fails
- ✅ Better connection distribution

### Single Host (Not Recommended)

```properties
# Single host - works but not optimal
yugabyte.host=node1
```

**Limitations:**
- ⚠️ No automatic node discovery
- ⚠️ No load balancing
- ⚠️ Single point of failure
- ⚠️ Driver must discover other nodes manually

### How the Code Handles It

```scala
// In YugabyteConfig.scala
val host = getProperty("yugabyte.host", "localhost")
val hosts = host.split(",").map(_.trim)  // ✅ Splits comma-separated hosts

val baseUrl = if (hosts.length > 1) {
  // Multiple hosts - use jdbc:yugabytedb:// format
  val hostPorts = hosts.map(h => s"$h:$port").mkString(",")
  s"jdbc:yugabytedb://$hostPorts/$database"
} else {
  // Single host
  s"jdbc:yugabytedb://$host:$port/$database"
}
```

## Complete Configuration Examples

### Example 1: Single Region (3 nodes)

```properties
yugabyte.host=yb-node-1,yb-node-2,yb-node-3
yugabyte.port=5433
yugabyte.database=transaction_datastore
yugabyte.loadBalanceHosts=true
# topologyKeys not needed - leave empty
```

**JDBC URL Generated:**
```
jdbc:yugabytedb://yb-node-1:5433,yb-node-2:5433,yb-node-3:5433/transaction_datastore?loadBalance=true&...
```

### Example 2: XCluster (Primary Cluster)

```properties
yugabyte.host=primary-yb-1,primary-yb-2,primary-yb-3
yugabyte.port=5433
yugabyte.database=transaction_datastore
yugabyte.loadBalanceHosts=true
# topologyKeys not needed - single cluster
```

**JDBC URL Generated:**
```
jdbc:yugabytedb://primary-yb-1:5433,primary-yb-2:5433,primary-yb-3:5433/transaction_datastore?loadBalance=true&...
```

### Example 3: Stretch Cluster (Multi-Region)

```properties
yugabyte.host=us-east-1,us-west-1,eu-west-1
yugabyte.port=5433
yugabyte.database=transaction_datastore
yugabyte.loadBalanceHosts=true
yugabyte.topologyKeys=aws.us-east-1.zone1,aws.us-west-1.zone1,aws.eu-west-1.zone1
```

**JDBC URL Generated:**
```
jdbc:yugabytedb://us-east-1:5433,us-west-1:5433,eu-west-1:5433/transaction_datastore?loadBalance=true&topologyKeys=aws.us-east-1.zone1,aws.us-west-1.zone1,aws.eu-west-1.zone1&...
```

## Best Practices

### ✅ DO

1. **Provide all nodes** for load balancing:
   ```properties
   yugabyte.host=node1,node2,node3
   ```

2. **Use loadBalanceHosts=true** for multi-node:
   ```properties
   yugabyte.loadBalanceHosts=true
   ```

3. **Add topology keys only for stretch clusters**:
   ```properties
   # Only if multi-region
   yugabyte.topologyKeys=region1.zone1,region2.zone1
   ```

### ❌ DON'T

1. **Don't use topology keys for single region**:
   ```properties
   # ❌ Not needed
   yugabyte.topologyKeys=...
   ```

2. **Don't use single host if you have multiple nodes**:
   ```properties
   # ❌ Not optimal
   yugabyte.host=node1
   ```

3. **Don't mix deployment types**:
   ```properties
   # ❌ Confusing
   yugabyte.host=single-node
   yugabyte.topologyKeys=multi-region-keys
   ```

## Summary

| Question | Answer |
|----------|--------|
| **Topology keys for single region?** | ❌ NO - Not needed |
| **Topology keys for XCluster?** | ❌ NO - Not needed (connect to one cluster) |
| **Topology keys for stretch cluster?** | ✅ YES - Required for optimal performance |
| **Provide all nodes in yugabyte.host?** | ✅ YES - Recommended for load balancing |
| **Comma-separated format?** | ✅ YES - `node1,node2,node3` |

## Quick Reference

```properties
# Single Region or XCluster
yugabyte.host=node1,node2,node3
yugabyte.loadBalanceHosts=true
# topologyKeys not needed

# Stretch Cluster (Multi-Region)
yugabyte.host=region1-node1,region2-node1,region3-node1
yugabyte.loadBalanceHosts=true
yugabyte.topologyKeys=region1.zone1,region2.zone1,region3.zone1
```

