# Migration Phase Analysis Report

**Log File:** `migration_test_split_size_complete.log`
**Generated:** 2025-12-24 22:26:12

## Phase Breakdown Table

| Spark Phase / Stage | Typical UI Indicator | Time Taken | Optimization | Notes / Impact |
| ------------------- | ------------------- | --------- | ------------ | -------------- |
| **Phase 0 – Initialization** | Job 0, Stage 0 (pre-job) | **~0.0 seconds** | – | SparkSession creation, config loading, driver initialization, YugabyteDB driver registration, checkpoint table setup. Minimal impact on migration time. |
| **Phase 1 – Planning / Token Range Calculation** | Stage 1 (implicit during DataFrame creation) | **~0.0 seconds** | Skip Row Count Estimation ✅ (Already implemented) | Token range calculation, partition creation. Current: 0.0 seconds. Optimized - no COUNT queries. |
| **Phase 2-4 – Read/Transform/COPY** | Stage 0 (ResultStage) - Task execution | **~0.0 seconds** | Token-aware partitioning ✅, Direct COPY streaming ✅, Parallel execution ✅ | Read from Cassandra, transform to CSV, COPY to YugabyteDB. N/A partitions processed concurrently. |
| **Phase 5 – Validation / Post-Processing** | Post-Stage (after Job 0) | **<0.1 seconds** | Metrics-based validation ✅ (no COUNT queries) | Row count validation using Spark Accumulators. Instant validation without database queries. |

## Detailed Timeline

| Event | Timestamp | Duration from Start |
|-------|-----------|---------------------|

## Key Observations

### Performance Metrics
