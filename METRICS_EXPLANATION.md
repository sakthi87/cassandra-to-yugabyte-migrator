# Metrics.scala - Explanation and Prometheus Integration

## What is Metrics.scala?

`Metrics.scala` is a **metrics collection class** that tracks the performance and progress of the Cassandra to YugabyteDB migration job.

### Current Purpose

1. **Collects Migration Metrics**:
   - Rows read from Cassandra
   - Rows written to YugabyteDB
   - Rows skipped (due to errors or validation)
   - Partitions completed/failed
   - Elapsed time
   - Throughput (rows/second)

2. **Uses Spark Accumulators**:
   - Aggregates metrics from all Spark executors
   - Thread-safe and distributed-friendly
   - Automatically combines values from multiple partitions

3. **Displays Summary**:
   - Prints metrics at the end of migration
   - Shows in console/logs

### Current Implementation

```scala
class Metrics(sparkContext: SparkContext) {
  // Spark Accumulators for distributed aggregation
  private val rowsRead: LongAccumulator = ...
  private val rowsWritten: LongAccumulator = ...
  private val rowsSkipped: LongAccumulator = ...
  private val partitionsCompleted: LongAccumulator = ...
  private val partitionsFailed: LongAccumulator = ...
  
  // Methods to increment metrics
  def incrementRowsRead(count: Long = 1): Unit = ...
  def incrementRowsWritten(count: Long = 1): Unit = ...
  
  // Methods to get current values
  def getRowsRead: Long = rowsRead.value
  def getRowsWritten: Long = rowsWritten.value
  def getThroughputRowsPerSec: Double = ...
  
  // Generate summary string
  def getSummary: String = ...
}
```

### How It's Used

1. **During Migration**:
   ```scala
   // In PartitionExecutor.scala
   metrics.incrementRowsRead()      // When reading from Cassandra
   metrics.incrementRowsWritten(100) // When writing to YugabyteDB
   metrics.incrementPartitionsCompleted() // When partition finishes
   ```

2. **After Migration**:
   ```scala
   // In MainApp.scala
   logInfo(metrics.getSummary)  // Prints metrics to console
   ```

### Current Output

```
Migration Metrics:
  Rows Read: 100000
  Rows Written: 100000
  Rows Skipped: 0
  Partitions Completed: 34
  Partitions Failed: 0
  Elapsed Time: 15 seconds
  Throughput: 6666.67 rows/sec
```

---

## Can It Push to Prometheus?

**Yes!** The current `Metrics.scala` can be extended to push metrics to Prometheus. Here's how:

### Option 1: Prometheus Pushgateway (Recommended)

**Best for**: Batch jobs like migrations

```scala
import io.prometheus.client.{CollectorRegistry, Gauge, Counter}
import io.prometheus.client.exporter.PushGateway

class Metrics(sparkContext: SparkContext) extends Serializable {
  // ... existing accumulators ...
  
  // Prometheus metrics
  private val pushGateway = new PushGateway("prometheus-pushgateway:9091")
  private val registry = new CollectorRegistry()
  
  private val rowsReadGauge = Gauge.build()
    .name("migration_rows_read_total")
    .help("Total rows read from Cassandra")
    .register(registry)
  
  private val rowsWrittenGauge = Gauge.build()
    .name("migration_rows_written_total")
    .help("Total rows written to YugabyteDB")
    .register(registry)
  
  private val throughputGauge = Gauge.build()
    .name("migration_throughput_rows_per_sec")
    .help("Current migration throughput")
    .register(registry)
  
  def pushToPrometheus(jobId: String): Unit = {
    // Update Prometheus metrics
    rowsReadGauge.set(getRowsRead)
    rowsWrittenGauge.set(getRowsWritten)
    throughputGauge.set(getThroughputRowsPerSec)
    
    // Push to Prometheus Pushgateway
    pushGateway.pushAdd(registry, s"migration_job_$jobId")
  }
}
```

### Option 2: Prometheus HTTP Server (For Long-Running Jobs)

**Best for**: Continuous monitoring during migration

```scala
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.{CollectorRegistry, Gauge}

class Metrics(sparkContext: SparkContext) extends Serializable {
  // ... existing accumulators ...
  
  private val registry = new CollectorRegistry()
  private var httpServer: Option[HTTPServer] = None
  
  def startPrometheusServer(port: Int = 9090): Unit = {
    httpServer = Some(new HTTPServer(registry, port))
  }
  
  def updatePrometheusMetrics(): Unit = {
    // Metrics are automatically exposed at http://localhost:9090/metrics
    // Prometheus scrapes this endpoint
  }
}
```

### Option 3: Periodic Updates (Real-Time Monitoring)

**Best for**: Real-time dashboards

```scala
class Metrics(sparkContext: SparkContext) extends Serializable {
  // ... existing accumulators ...
  
  private val pushGateway = new PushGateway("prometheus-pushgateway:9091")
  
  // Update Prometheus every N seconds
  def startPeriodicUpdates(jobId: String, intervalSeconds: Int = 30): Unit = {
    val scheduler = Executors.newScheduledThreadPool(1)
    scheduler.scheduleAtFixedRate(
      () => pushToPrometheus(jobId),
      0,
      intervalSeconds,
      TimeUnit.SECONDS
    )
  }
}
```

---

## Implementation Steps for Prometheus Integration

### 1. Add Prometheus Dependency

```xml
<!-- pom.xml -->
<dependency>
  <groupId>io.prometheus</groupId>
  <artifactId>simpleclient</artifactId>
  <version>0.16.0</version>
</dependency>
<dependency>
  <groupId>io.prometheus</groupId>
  <artifactId>simpleclient_pushgateway</artifactId>
  <version>0.16.0</version>
</dependency>
```

### 2. Extend Metrics.scala

Add Prometheus push methods while keeping existing functionality.

### 3. Update MainApp.scala

```scala
// After migration completes
metrics.pushToPrometheus(jobId)

// Or for periodic updates during migration
metrics.startPeriodicUpdates(jobId, 30) // Update every 30 seconds
```

### 4. Configure Prometheus

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'migration'
    static_configs:
      - targets: ['prometheus-pushgateway:9091']
```

---

## Metrics Available for Prometheus

| Metric Name | Type | Description |
|------------|------|-------------|
| `migration_rows_read_total` | Gauge | Total rows read from Cassandra |
| `migration_rows_written_total` | Gauge | Total rows written to YugabyteDB |
| `migration_rows_skipped_total` | Gauge | Total rows skipped |
| `migration_partitions_completed` | Gauge | Number of partitions completed |
| `migration_partitions_failed` | Gauge | Number of partitions failed |
| `migration_throughput_rows_per_sec` | Gauge | Current throughput |
| `migration_elapsed_time_seconds` | Gauge | Elapsed time since start |
| `migration_duration_seconds` | Histogram | Migration duration |

---

## Benefits of Prometheus Integration

1. **Real-Time Monitoring**: See migration progress in Grafana dashboards
2. **Alerting**: Set up alerts for failures or low throughput
3. **Historical Data**: Track performance over time
4. **Multi-Job Tracking**: Monitor multiple migrations simultaneously
5. **Integration**: Works with existing Prometheus/Grafana infrastructure

---

## Example Prometheus Query

```promql
# Current throughput
migration_throughput_rows_per_sec{job="migration_job_123"}

# Total rows migrated
migration_rows_written_total{job="migration_job_123"}

# Success rate
rate(migration_partitions_completed[5m]) / 
  (rate(migration_partitions_completed[5m]) + rate(migration_partitions_failed[5m]))
```

---

## Summary

- **Current**: `Metrics.scala` collects and displays metrics in console/logs
- **Can Extend**: Yes, easily add Prometheus push functionality
- **Best Approach**: Use Prometheus Pushgateway for batch jobs
- **Benefits**: Real-time monitoring, alerting, historical tracking

The current implementation is **ready to be extended** for Prometheus integration without breaking existing functionality.

