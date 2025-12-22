package com.company.migration.util

import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

/**
 * Metrics collection for migration tracking
 * 
 * CRITICAL: Uses Spark Accumulators to properly aggregate metrics from executors
 * 
 * Problem with LongAdder:
 * - Each executor gets a serialized copy of Metrics
 * - Updates happen on executor-side copies
 * - Driver never sees executor-side updates
 * 
 * Solution: Spark Accumulators
 * - Automatically aggregate values from all executors
 * - Driver sees the correct totals
 * - Thread-safe and efficient
 */
class Metrics(sparkContext: SparkContext) extends Serializable {
  // Use Spark Accumulators for proper aggregation across executors
  private val rowsRead: LongAccumulator = sparkContext.longAccumulator("migration.rowsRead")
  private val rowsWritten: LongAccumulator = sparkContext.longAccumulator("migration.rowsWritten")
  private val rowsSkipped: LongAccumulator = sparkContext.longAccumulator("migration.rowsSkipped")
  private val partitionsCompleted: LongAccumulator = sparkContext.longAccumulator("migration.partitionsCompleted")
  private val partitionsFailed: LongAccumulator = sparkContext.longAccumulator("migration.partitionsFailed")
  private val startTime = new AtomicLong(System.currentTimeMillis())
  
  def incrementRowsRead(count: Long = 1): Unit = rowsRead.add(count)
  def incrementRowsWritten(count: Long = 1): Unit = rowsWritten.add(count)
  def incrementRowsSkipped(count: Long = 1): Unit = rowsSkipped.add(count)
  def incrementPartitionsCompleted(): Unit = partitionsCompleted.add(1)
  def incrementPartitionsFailed(): Unit = partitionsFailed.add(1)
  
  def getRowsRead: Long = rowsRead.value
  def getRowsWritten: Long = rowsWritten.value
  def getRowsSkipped: Long = rowsSkipped.value
  def getPartitionsCompleted: Long = partitionsCompleted.value
  def getPartitionsFailed: Long = partitionsFailed.value
  
  def getElapsedTimeMs: Long = System.currentTimeMillis() - startTime.get()
  def getElapsedTimeSeconds: Long = getElapsedTimeMs / 1000
  
  def getThroughputRowsPerSec: Double = {
    val elapsed = getElapsedTimeSeconds
    if (elapsed > 0) getRowsWritten.toDouble / elapsed else 0.0
  }
  
  def getSummary: String = {
    val elapsed = getElapsedTimeSeconds
    val throughput = getThroughputRowsPerSec
    
    s"""
       |Migration Metrics:
       |  Rows Read: ${getRowsRead}
       |  Rows Written: ${getRowsWritten}
       |  Rows Skipped: ${getRowsSkipped}
       |  Partitions Completed: ${getPartitionsCompleted}
       |  Partitions Failed: ${getPartitionsFailed}
       |  Elapsed Time: ${elapsed} seconds
       |  Throughput: ${f"$throughput%.2f"} rows/sec
       |""".stripMargin
  }
}

