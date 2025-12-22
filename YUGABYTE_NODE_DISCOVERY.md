# YugabyteDB Node Discovery - How It Works

## ✅ Yes, YugabyteDB Driver Discovers All Nodes Automatically

**Similar to Cassandra Driver**, the YugabyteDB JDBC Smart Driver automatically discovers all nodes in the cluster, even if you only provide a subset.

## How It Works

### Initial Contact Points

When you provide nodes in the connection string:
```properties
yugabyte.host=node1,node2,node3
```

**What Happens:**
1. ✅ Driver uses these as **initial contact points**
2. ✅ Tries to connect to each node in sequence
3. ✅ Once connected, **queries the cluster** for full node list
4. ✅ **Discovers all 6 nodes** automatically (even if you only provided 3)
5. ✅ Manages connections to **all discovered nodes**

### Example Scenario

**Cluster Setup:**
- Total nodes: 6 (node1, node2, node3, node4, node5, node6)
- You provide: 3 nodes (node1, node2, node3)

**Driver Behavior:**
```
1. Connect to node1 (or node2, node3 if node1 fails)
2. Query cluster: "What are all the nodes?"
3. Cluster responds: [node1, node2, node3, node4, node5, node6]
4. Driver now knows about ALL 6 nodes
5. Load balancing works across ALL 6 nodes
```

## Comparison: Cassandra vs YugabyteDB

| Feature | Cassandra Driver | YugabyteDB Driver |
|---------|------------------|-------------------|
| **Node Discovery** | ✅ Automatic | ✅ Automatic |
| **Initial Contact Points** | ✅ Seed nodes | ✅ Initial hosts |
| **Full Cluster Discovery** | ✅ Yes | ✅ Yes |
| **Load Balancing** | ✅ Token-aware | ✅ Uniform/Topology-aware |
| **Failover** | ✅ Automatic | ✅ Automatic |

## Configuration

### Minimal Configuration (Recommended)

```properties
# Provide 2-3 nodes as initial contact points
yugabyte.host=node1,node2,node3
yugabyte.loadBalanceHosts=true
```

**Result:**
- ✅ Driver discovers all 6 nodes automatically
- ✅ Load balancing across all nodes
- ✅ Automatic failover if any node fails

### Providing All Nodes (Also Works)

```properties
# Provide all nodes explicitly
yugabyte.host=node1,node2,node3,node4,node5,node6
yugabyte.loadBalanceHosts=true
```

**Result:**
- ✅ Same behavior - driver still queries cluster
- ✅ Redundant but harmless
- ✅ More resilient if some initial nodes are down

## Best Practices

### ✅ Recommended Approach

**Provide 2-3 nodes as initial contact points:**
```properties
# Single region - 2-3 nodes sufficient
yugabyte.host=node1,node2,node3
yugabyte.loadBalanceHosts=true
```

**Why:**
- ✅ Driver discovers all nodes automatically
- ✅ Resilient if 1-2 nodes are down
- ✅ Simpler configuration
- ✅ Same performance as providing all nodes

### Multi-Region Example

```properties
# Provide 1-2 nodes per region as contact points
yugabyte.host=us-east-1,us-west-1,eu-west-1
yugabyte.loadBalanceHosts=true
yugabyte.topologyKeys=aws.us-east-1.zone1,aws.us-west-1.zone1,aws.eu-west-1.zone1
```

**Result:**
- ✅ Driver discovers all nodes in each region
- ✅ Topology-aware load balancing
- ✅ Connections prioritized by region

## How to Verify Node Discovery

### Check Driver Logs

Look for connection logs:
```
INFO: Connecting to YugabyteDB cluster...
INFO: Discovered 6 nodes: [node1, node2, node3, node4, node5, node6]
```

### Query YugabyteDB

Check active connections from your application:
```sql
SELECT datname, usename, client_addr, count(*) 
FROM pg_stat_activity 
WHERE datname = 'your_database'
GROUP BY datname, usename, client_addr;
```

You should see connections distributed across **all 6 nodes**, not just the 3 you provided.

## Summary

| Question | Answer |
|----------|--------|
| **Does YugabyteDB driver discover all nodes?** | ✅ **YES** - Automatically, like Cassandra |
| **Do I need to provide all 6 nodes?** | ❌ **NO** - 2-3 nodes sufficient as contact points |
| **Will it use all 6 nodes?** | ✅ **YES** - Driver discovers and uses all nodes |
| **What if some initial nodes are down?** | ✅ **OK** - Driver tries next node, then discovers all |

## Key Takeaway

**You only need to provide 2-3 nodes as initial contact points.** The YugabyteDB Smart Driver will:
1. Connect to one of the provided nodes
2. Query the cluster for the full node list
3. Discover all nodes automatically
4. Use all nodes for load balancing

**This is exactly like Cassandra driver behavior** - you provide seed nodes, and the driver discovers the rest.

