# loadBalanceHosts Configuration Explanation

## ✅ How It Works

### For Single Region (Most Common)

**Configuration:**
```properties
yugabyte.loadBalanceHosts=true
# topologyKeys not needed - leave empty or don't set
```

**What Happens:**
- ✅ `loadBalance=true` is added to JDBC URL
- ✅ Driver distributes connections across all nodes in the cluster
- ✅ Works perfectly without topology keys
- ✅ Topology keys are **optional** for single region

**JDBC URL Generated:**
```
jdbc:yugabytedb://node1:5433,node2:5433,node3:5433/database?loadBalance=true&...
```

### For Multi-Region/Stretch Cluster

**Configuration:**
```properties
yugabyte.loadBalanceHosts=true
yugabyte.topologyKeys=region1.zone1,region2.zone1
```

**What Happens:**
- ✅ `loadBalance=true` is added to JDBC URL
- ✅ `topologyKeys=region1.zone1,region2.zone1` is added
- ✅ Driver prioritizes connections to specified regions/zones

**JDBC URL Generated:**
```
jdbc:yugabytedb://node1:5433,node2:5433,node3:5433/database?loadBalance=true&topologyKeys=region1.zone1,region2.zone1&...
```

## Code Implementation

The code correctly handles both scenarios:

```scala
// In YugabyteConfig.scala - buildJdbcParams()
val loadBalance = props.getProperty("yugabyte.loadBalanceHosts", "true").toBoolean
if (loadBalance) {
  params += "loadBalance=true"  // ✅ Always works for single region
  // Only add topologyKeys if explicitly configured (for multi-region)
  val topologyKeys = props.getProperty("yugabyte.topologyKeys", "")
  if (topologyKeys.nonEmpty) {
    params += s"topologyKeys=$topologyKeys"  // ✅ Optional, only for multi-region
  }
}
```

## Common Misconception

**❌ WRONG:** "loadBalanceHosts only works with topology keys"

**✅ CORRECT:** 
- `loadBalanceHosts=true` works **without** topology keys for single region
- Topology keys are **optional** and only needed for multi-region/stretch clusters
- For single region, load balancing distributes connections across all nodes automatically

## Verification

### Check JDBC URL in Logs

Look for this log message:
```
INFO YugabyteConnectionFactory: Created new connection for partition: ...
```

The connection URL will show:
- Single region: `jdbc:yugabytedb://...?loadBalance=true&...`
- Multi-region: `jdbc:yugabytedb://...?loadBalance=true&topologyKeys=...&...`

### Test Connection Distribution

With `loadBalanceHosts=true`, connections should be distributed across all YugabyteDB nodes. Check:
```sql
-- On YugabyteDB
SELECT datname, usename, client_addr, count(*) 
FROM pg_stat_activity 
WHERE datname = 'your_database'
GROUP BY datname, usename, client_addr;
```

You should see connections from your Spark executors distributed across nodes.

## Summary

| Scenario | loadBalanceHosts | topologyKeys | Works? |
|----------|------------------|--------------|--------|
| Single region | `true` | (empty) | ✅ Yes |
| Single region | `true` | (not set) | ✅ Yes |
| Multi-region | `true` | `region1.zone1,region2.zone1` | ✅ Yes |
| Multi-region | `true` | (empty) | ✅ Yes (but not optimal) |

**Bottom Line:** `loadBalanceHosts=true` works for single region without topology keys. Topology keys are only needed for multi-region optimization.

