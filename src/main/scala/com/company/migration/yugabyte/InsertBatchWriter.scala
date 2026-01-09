package com.company.migration.yugabyte

import com.company.migration.util.Logging

import java.sql.{Connection, PreparedStatement}
import scala.collection.mutable.ListBuffer

/**
 * Batched INSERT writer for YugabyteDB using INSERT ... ON CONFLICT DO NOTHING
 * 
 * This provides idempotent inserts that handle duplicates gracefully,
 * making retries and resume logic safe from duplicate key violations.
 */
class InsertBatchWriter(
  conn: Connection,
  insertSql: String,
  batchSize: Int = 1000
) extends Logging {
  
  private var preparedStatement: Option[PreparedStatement] = None
  private var currentBatchSize = 0
  private var totalRowsWritten = 0L
  private var totalRowsSkipped = 0L  // Rows skipped due to duplicates (DO NOTHING)
  
  /**
   * Initialize the PreparedStatement
   */
  def start(): Unit = {
    logDebug(s"Initializing INSERT batch writer with batch size: $batchSize")
    logDebug(s"INSERT statement: $insertSql")
    preparedStatement = Some(conn.prepareStatement(insertSql))
    currentBatchSize = 0
    logDebug("INSERT batch writer initialized")
  }
  
  /**
   * Add a row to the batch using row values array
   * Flushes automatically when batch size is reached
   */
  def addRow(rowValues: Array[Any]): Unit = {
    val stmt = preparedStatement.get
    
    // Bind values to prepared statement
    rowValues.zipWithIndex.foreach { case (value, index) =>
      val paramIndex = index + 1  // JDBC parameters are 1-indexed
      if (value == null) {
        stmt.setObject(paramIndex, null)
      } else {
        stmt.setObject(paramIndex, value)
      }
    }
    
    stmt.addBatch()
    currentBatchSize += 1
    
    // Flush when batch size reached
    if (currentBatchSize >= batchSize) {
      flush()
    }
  }
  
  /**
   * Flush the current batch to the database
   */
  def flush(): Int = {
    if (currentBatchSize == 0) {
      return 0
    }
    
    val stmt = preparedStatement.get
    val batchCount = currentBatchSize
    
    try {
      val updateCounts = stmt.executeBatch()
      stmt.clearBatch()
      currentBatchSize = 0
      
      // Count actual inserts (updateCounts[i] = 1) vs skipped (updateCounts[i] = 0)
      val inserted = updateCounts.count(_ > 0)
      val skipped = batchCount - inserted
      
      totalRowsWritten += inserted
      totalRowsSkipped += skipped
      
      logDebug(s"Flushed batch: $batchCount rows (inserted: $inserted, skipped: $skipped)")
      inserted
    } catch {
      case e: Exception =>
        logError(s"Error flushing INSERT batch: ${e.getMessage}", e)
        stmt.clearBatch()
        currentBatchSize = 0
        throw new RuntimeException("Failed to flush INSERT batch", e)
    }
  }
  
  /**
   * Finalize and get total rows written
   */
  def endBatch(): Long = {
    // Flush any remaining rows
    if (currentBatchSize > 0) {
      flush()
    }
    
    preparedStatement.foreach { stmt =>
      try {
        stmt.close()
      } catch {
        case e: Exception =>
          logError(s"Error closing PreparedStatement: ${e.getMessage}", e)
      }
    }
    
    preparedStatement = None
    logInfo(s"INSERT batch writer completed. Total rows written: $totalRowsWritten, Total rows skipped (duplicates): $totalRowsSkipped")
    totalRowsWritten
  }
  
  /**
   * Cancel and cleanup
   */
  def cancel(): Unit = {
    preparedStatement.foreach { stmt =>
      try {
        stmt.clearBatch()
        stmt.close()
      } catch {
        case e: Exception =>
          logError(s"Error canceling PreparedStatement: ${e.getMessage}", e)
      }
    }
    preparedStatement = None
    currentBatchSize = 0
  }
  
  /**
   * Get total rows written so far
   */
  def getRowsWritten: Long = totalRowsWritten
  
  /**
   * Get total rows skipped (duplicates)
   */
  def getRowsSkipped: Long = totalRowsSkipped
  
  /**
   * Check if writer is active
   */
  def isActive: Boolean = preparedStatement.isDefined
}

