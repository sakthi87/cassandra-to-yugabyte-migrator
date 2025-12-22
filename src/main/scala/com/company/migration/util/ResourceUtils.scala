package com.company.migration.util

import java.sql.Connection
import org.slf4j.LoggerFactory

/**
 * Utility methods for resource management
 */
object ResourceUtils {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Safely close a connection
   */
  def closeConnection(conn: Connection): Unit = {
    if (conn != null && !conn.isClosed) {
      try {
        conn.close()
      } catch {
        case e: Exception =>
          logger.warn(s"Error closing connection: ${e.getMessage}", e)
      }
    }
  }
  
  /**
   * Execute a block with a connection, ensuring it's closed
   */
  def withConnection[T](conn: Connection)(block: Connection => T): T = {
    try {
      block(conn)
    } finally {
      closeConnection(conn)
    }
  }
  
  /**
   * Format bytes to human-readable string
   */
  def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"${bytes}B"
    else if (bytes < 1024 * 1024) s"${bytes / 1024}KB"
    else if (bytes < 1024 * 1024 * 1024) s"${bytes / (1024 * 1024)}MB"
    else s"${bytes / (1024 * 1024 * 1024)}GB"
  }
  
  /**
   * Format duration to human-readable string
   */
  def formatDuration(seconds: Long): String = {
    if (seconds < 60) s"${seconds}s"
    else if (seconds < 3600) s"${seconds / 60}m ${seconds % 60}s"
    else {
      val hours = seconds / 3600
      val minutes = (seconds % 3600) / 60
      val secs = seconds % 60
      s"${hours}h ${minutes}m ${secs}s"
    }
  }
}

