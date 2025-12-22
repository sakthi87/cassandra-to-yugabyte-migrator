package com.company.migration.yugabyte

import com.company.migration.util.Logging
// Use YugabyteDB's own CopyManager and BaseConnection
// YugabyteDB has its own implementation that works with com.yugabyte.jdbc.PgConnection
import com.yugabyte.copy.CopyManager
import com.yugabyte.core.BaseConnection

import java.nio.charset.StandardCharsets
import java.sql.Connection

/**
 * COPY writer that streams data directly to YugabyteDB
 * 
 * CRITICAL: This implementation uses direct writeToCopy() - NO PIPES!
 * This prevents "Pipe broken" errors and ensures production-grade reliability.
 * 
 * Uses YugabyteDB JDBC Driver (com.yugabyte.Driver) with YugabyteDB's own CopyManager
 * Reference: Production-grade implementation pattern
 */
class CopyWriter(
  conn: Connection,
  copySql: String,
  flushEvery: Int = 20000  // Increased for better throughput
) extends Logging {
  
  // Get YugabyteDB's BaseConnection (com.yugabyte.core.BaseConnection)
  // YugabyteDB's com.yugabyte.jdbc.PgConnection implements com.yugabyte.core.BaseConnection
  private val baseConn: BaseConnection = {
    try {
      // Try unwrapping to YugabyteDB's BaseConnection
      conn.unwrap(classOf[BaseConnection])
    } catch {
      case e: java.sql.SQLException =>
        // If unwrap fails, try direct cast
        try {
          conn.asInstanceOf[BaseConnection]
        } catch {
          case e2: ClassCastException =>
            logError(s"Failed to get YugabyteDB BaseConnection. Connection class: ${conn.getClass.getName}, URL: ${conn.getMetaData.getURL}", e2)
            throw new RuntimeException(
              s"Cannot get BaseConnection from YugabyteDB connection. " +
              s"Connection type: ${conn.getClass.getName}. " +
              s"URL: ${conn.getMetaData.getURL}. " +
              "Ensure you're using YugabyteDB JDBC driver (com.yugabyte.Driver).", e2)
        }
    }
  }

  // Use YugabyteDB's CopyManager with YugabyteDB's BaseConnection
  private val copyManager = new CopyManager(baseConn)
  
  // Buffer for batching rows (increased for higher throughput)
  private val buffer = new StringBuilder(4 * 1024 * 1024) // 4MB initial capacity
  private var rowCount = 0L
  private var totalRowsWritten = 0L
  private var copyIn: Option[com.yugabyte.copy.CopyIn] = None

  /**
   * Start the COPY operation
   */
  def start(): Unit = {
    logDebug(s"Starting COPY operation: $copySql")
    copyIn = Some(copyManager.copyIn(copySql))
    logDebug("COPY operation started")
  }

  /**
   * Write a CSV row to the buffer
   * Flushes automatically when buffer reaches flushEvery rows
   */
  def writeRow(csvRow: String): Unit = {
    buffer.append(csvRow).append('\n')
    rowCount += 1
    
    if (rowCount >= flushEvery) {
      flush()
    }
  }

  /**
   * Flush buffered rows to COPY stream
   * Uses direct writeToCopy() - NO PIPES!
   */
  def flush(): Unit = {
    if (buffer.nonEmpty && copyIn.isDefined) {
      val csvData = buffer.toString()
      val bytes = csvData.getBytes(StandardCharsets.UTF_8)
      
      try {
        copyIn.get.writeToCopy(bytes, 0, bytes.length)
        totalRowsWritten += rowCount
        logDebug(s"Flushed $rowCount rows (total: $totalRowsWritten)")
        
        buffer.clear()
        rowCount = 0
      } catch {
        case e: Exception =>
          logError(s"Error flushing COPY data: ${e.getMessage}", e)
          throw new RuntimeException("Failed to flush COPY data", e)
      }
    }
  }

  /**
   * End the COPY operation and wait for completion
   * This is critical - must be called before committing
   */
  def endCopy(): Long = {
    // Flush any remaining data
    if (buffer.nonEmpty) {
      flush()
    }
    
    copyIn match {
      case Some(copy) =>
        try {
          // End the COPY operation
          val rowsCopied = copy.endCopy()
          logInfo(s"COPY operation completed. Rows copied: $rowsCopied, Rows written: $totalRowsWritten")
          copyIn = None
          rowsCopied
        } catch {
          case e: Exception =>
            logError(s"Error ending COPY operation: ${e.getMessage}", e)
            try {
              copy.cancelCopy()
            } catch {
              case cancelEx: Exception =>
                logError(s"Error canceling COPY: ${cancelEx.getMessage}", cancelEx)
            }
            copyIn = None
            throw new RuntimeException("Failed to end COPY operation", e)
        }
      case None =>
        logWarn("COPY operation was not started")
        0L
    }
  }

  /**
   * Cancel the COPY operation
   */
  def cancelCopy(): Unit = {
    copyIn match {
      case Some(copy) =>
        try {
          copy.cancelCopy()
          logWarn("COPY operation canceled")
        } catch {
          case e: Exception =>
            logError(s"Error canceling COPY: ${e.getMessage}", e)
        }
        copyIn = None
      case None =>
        // Already ended or never started
    }
    buffer.clear()
    rowCount = 0
  }

  /**
   * Get total rows written to the writer
   */
  def getRowsWritten: Long = totalRowsWritten + rowCount

  /**
   * Check if COPY is active
   */
  def isActive: Boolean = copyIn.isDefined
}
