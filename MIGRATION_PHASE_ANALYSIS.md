# Migration Phase Analysis - 100K Records Test Run

**Test Date:** December 24, 2024  
**Table:** `transaction_datastore.dda_pstd_fincl_txn_cnsmr_by_accntnbr`  
**Total Records:** 100,000  
**Total Time:** 17 seconds  
**Throughput:** 5,882 rows/sec

## Phase Breakdown Table

| Spark Phase / Stage                              | Typical UI Indicator                         | Time Taken | Optimization                                                                                                                                                                                                                                                            | Notes / Impact                                                                                                                                             |
| ------------------------------------------------ | -------------------------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Phase 0 – Initialization**                     | Job 0, Stage 0 (pre-job)                     | **~1 second** | –                                                                                                                                                                                                                                                                       | SparkSession creation, config loading, driver initialization, YugabyteDB driver registration, checkpoint table setup. Minimal impact on migration time.                                                                                   |
| **Phase 1 – Planning / Token Range Calculation** | Stage 1 (implicit during DataFrame creation) | **~3 seconds** | 1. Cache Token Range Information<br>2. Pre-calculate Splits<br>3. Reduce Metadata Queries<br>4. Parallel Metadata Reading<br>5. Use Smaller Initial Fetch for Planning<br>6. Skip Row Count Estimation ✅ (Already implemented)<br>7. Connection Pooling for Metadata<br>8. Incremental Planning | **Current:** 3 seconds for 100K rows (34 partitions calculated)<br>**Goal:** Reduce metadata queries, token calculation, and unnecessary scans.<br>**Effect:** For 25M rows, could be 30 min → 10–15 min with optimizations.                                   |
| **Phase 2 – Read from Cassandra**                | Stage 0 (ResultStage) - Task execution       | **~14 seconds** (combined with Phase 3 & 4) | 1. Token-aware partition reads ✅ (Already implemented)<br>2. Input split size tuning (`cassandra.inputSplitSizeMb=256`) ✅<br>3. Adjust `fetchSizeInRows=10000` for data ✅                                                                                                                                     | Reads happen in parallel per partition (34 partitions). First partition started at 21:30:08, first completed at 21:30:12. All 34 partitions completed by 21:30:21. Larger splits reduce number of partitions (less overhead), smaller splits improve parallelism if nodes are stable. |
| **Phase 3 – Transform / In-Memory Processing**   | Stage 0 (ResultStage) - `mapPartitions`       | **~14 seconds** (combined with Phase 2 & 4)   | 1. Row transformation (SchemaMapper, DataTypeConverter, RowTransformer) ✅<br>2. Pre-partition data (optional)                                                                                                                                                             | Converts Cassandra row types → CSV-friendly format. Minimal disk I/O. CPU & memory bound per executor. Happens inline during `foreachPartition` execution.                                                     |
| **Phase 4 – COPY Streaming to Yugabyte**         | Stage 0 (ResultStage) - JDBC COPY operations  | **~14 seconds** (Job 0 execution time)        | 1. COPY writer (direct streaming) ✅<br>2. Parallelism tuning (`spark.default.parallelism=16`) ✅<br>3. Executor cores/memory tuning ✅<br>4. 34 concurrent COPY streams (one per partition) ✅                                                                                                                                             | Writes bypass YSQL parsing. First COPY completed at 21:30:12 (1,600 rows). All 34 partitions completed by 21:30:21. **Job 0 finished in 14.282 seconds.** DocDB ops/sec visible in Yugabyte metrics.                                       |
| **Phase 5 – Validation / Post-Processing**       | Post-Stage (after Job 0)                     | **<1 second** | 1. RowCountValidator ✅ (Using migration metrics, no COUNT queries)<br>2. ChecksumValidator (optional) ✅                                                                                                                                                                                                                 | Not part of heavy migration. Ensures correctness. **100,000 rows read = 100,000 rows written, 0 skipped.** Validation completed instantly using Spark Accumulators (no database queries).                                                                                                          |

## Detailed Timeline

| Event | Timestamp | Duration from Start |
|-------|-----------|---------------------|
| Spark Context Submitted | 21:30:03 | 0s |
| Spark Session Created | 21:30:04 | 1s |
| Reading Table Started | 21:30:04 | 1s |
| Table Read Complete | 21:30:06 | 3s |
| Partitions Calculated (34) | 21:30:07 | 4s |
| Job 0 Started (foreachPartition) | 21:30:07 | 4s |
| First Tasks Started (4 concurrent) | 21:30:08 | 5s |
| First COPY Completed (1,600 rows) | 21:30:12 | 9s |
| First Partition Completed | 21:30:12 | 9s |
| Job 0 Finished | 21:30:21 | 18s |
| Migration Completed | 21:30:21 | 18s |
| Validation Completed | 21:30:21 | 18s |
| Spark Context Stopped | 21:30:21 | 18s |

## Key Observations

### Performance Metrics
- **Total Migration Time:** 17 seconds (from Spark session creation to migration completion)
- **Job 0 Execution Time:** 14.282 seconds (actual data processing)
- **Planning Phase:** 3 seconds (very fast for 100K rows)
- **Throughput:** 5,882 rows/sec (excellent for local environment)

### Partition Distribution
- **Total Partitions:** 34 (determined by Cassandra token ranges)
- **Concurrent Tasks:** 4 (limited by `spark.executor.cores=4`)
- **Partition Sizes:** Varied (800 to 5,200 rows per partition)
- **First Partition Completed:** 4 seconds after task start
- **Last Partition Completed:** 13 seconds after task start

### COPY Performance
- **First COPY Stream:** Completed in ~4 seconds (1,600 rows)
- **Average COPY Time per Partition:** ~1-2 seconds (varies by partition size)
- **Total COPY Streams:** 34 concurrent streams
- **All COPY Operations:** Completed successfully (0 failures)

### Optimization Status
✅ **Already Optimized:**
- No `df.count()` during planning (skipped row count estimation)
- Token-aware partitioning (34 partitions from Cassandra token ranges)
- Direct COPY streaming (no pipes, no intermediate storage)
- Metrics-based validation (no COUNT queries)
- Parallel execution (34 partitions processed concurrently)

🔧 **Potential Optimizations (for larger datasets):**
- Cache token range information (reduce planning time for 25M+ rows)
- Pre-calculate splits (if metadata queries become bottleneck)
- Increase parallelism (if network latency is a factor in remote environments)
- Connection pooling for metadata queries (if planning phase becomes slow)

## Comparison with Reference Table

| Metric | Reference (25M rows) | Current Test (100K rows) | Scaled Projection (25M rows) |
|--------|---------------------|--------------------------|------------------------------|
| Planning Time | 30 minutes | 3 seconds | ~12.5 minutes (if linear) |
| Migration Time | N/A | 14 seconds | ~58 minutes (if linear) |
| Total Time | N/A | 17 seconds | ~70 minutes (if linear) |
| Throughput | N/A | 5,882 rows/sec | Similar (if resources scale) |

**Note:** Scaling is not linear. Planning time may increase more than linearly due to metadata queries, but migration time should scale more linearly with proper parallelism.

