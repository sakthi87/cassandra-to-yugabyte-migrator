package com.company.migration.util

import java.util.concurrent.atomic.{AtomicLong, LongAdder}

/**
 * Metrics collection for migration tracking
 */
class Metrics extends Serializable {
  private val rowsRead = new LongAdder()
  private val rowsWritten = new LongAdder()
  private val rowsSkipped = new LongAdder()
  private val partitionsCompleted = new LongAdder()
  private val partitionsFailed = new LongAdder()
  private val startTime = new AtomicLong(System.currentTimeMillis())
  
  def incrementRowsRead(count: Long = 1): Unit = rowsRead.add(count)
  def incrementRowsWritten(count: Long = 1): Unit = rowsWritten.add(count)
  def incrementRowsSkipped(count: Long = 1): Unit = rowsSkipped.add(count)
  def incrementPartitionsCompleted(): Unit = partitionsCompleted.increment()
  def incrementPartitionsFailed(): Unit = partitionsFailed.increment()
  
  def getRowsRead: Long = rowsRead.sum()
  def getRowsWritten: Long = rowsWritten.sum()
  def getRowsSkipped: Long = rowsSkipped.sum()
  def getPartitionsCompleted: Long = partitionsCompleted.sum()
  def getPartitionsFailed: Long = partitionsFailed.sum()
  
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

