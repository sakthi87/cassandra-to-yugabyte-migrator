# Migration Phase Analysis Report

**Log File:** `migration_test_transaction_datastore.log`
**Generated:** 2025-12-24 21:47:01

## Summary

**Table:** `transaction_datastore.dda_pstd_fincl_txn_cnsmr_by_accntnbr`
**Total Records:** 100,000
**Throughput:** 5,882.35 rows/sec
**Total Time:** 17.00 seconds

## Phase Breakdown Table

| Spark Phase / Stage | Typical UI Indicator | Time Taken | Optimization | Notes / Impact |
| ------------------- | ------------------- | --------- | ------------ | -------------- |
| **Phase 0 – Initialization** | Job 0, Stage 0 (pre-job) | **~3.0 seconds** | – | SparkSession creation, config loading, driver initialization, YugabyteDB driver registration, checkpoint table setup. Minimal impact on migration time. |
| **Phase 1 – Planning / Token Range Calculation** | Stage 1 (implicit during DataFrame creation) | **~3.0 seconds** | Skip Row Count Estimation ✅ (Already implemented) | Token range calculation, partition creation. Current: 3.0 seconds. Optimized - no COUNT queries. |
| **Phase 2-4 – Read/Transform/COPY** | Stage 0 (ResultStage) - Task execution | **~14.3 seconds** | Token-aware partitioning ✅, Direct COPY streaming ✅, Parallel execution ✅ | Read from Cassandra, transform to CSV, COPY to YugabyteDB. 34 partitions processed concurrently. |
| **Phase 5 – Validation / Post-Processing** | Post-Stage (after Job 0) | **<0.1 seconds** | Metrics-based validation ✅ (no COUNT queries) | Row count validation using Spark Accumulators. Instant validation without database queries. |

## Detailed Timeline

| Event | Timestamp | Duration from Start |
|-------|-----------|---------------------|
| App Submitted | 21:30:03 | -1.0s |
| Spark Session | 21:30:04 | 0.0s |
| Migration Start | 21:30:04 | 0.0s |
| Reading Table | 21:30:04 | 0.0s |
| Table Read | 21:30:06 | 2.0s |
| Partitions Calculated | 21:30:07 | 3.0s |
| Job Started | 21:30:07 | 3.0s |
| First Task Started | 21:30:08 | 4.0s |
| First Copy Completed | 21:30:12 | 8.0s |
| First Partition Completed | 21:30:12 | 8.0s |
| Job Finished | 21:30:21 | 17.0s |
| Migration Complete | 21:30:21 | 17.0s |
| Validation Start | 21:30:21 | 17.0s |
| Validation Complete | 21:30:21 | 17.0s |
| Summary | 21:30:21 | 17.0s |

## Key Observations

### Performance Metrics
- **Total Migration Time:** 17.00 seconds
- **Job 0 Execution Time:** 14.28 seconds (actual data processing)
- **Planning Phase:** 3.00 seconds
- **Throughput:** 5,882.35 rows/sec

### Partition Distribution
- **Total Partitions:** 34 (determined by Cassandra token ranges)
- **Partitions Completed:** 34
- **Partitions Failed:** 0

### COPY Performance
- **First COPY Stream:** Completed in ~4.0 seconds
- **First COPY Rows:** 1,600 rows

### Validation Results
- **Rows Read:** 100,000
- **Rows Written:** 100,000
- **Rows Skipped:** 0
- **Validation:** ✅ PASSED
