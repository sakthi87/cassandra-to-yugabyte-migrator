package com.company.migration.execution

import com.company.migration.util.Logging
import java.sql.SQLException
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
 * Handles retries for transient errors
 */
object RetryHandler extends Logging {
  
  /**
   * Retry a block with exponential backoff
   * @param maxRetries Maximum number of retries
   * @param initialDelayMs Initial delay in milliseconds
   * @param block Block to execute
   * @return Result of the block
   */
  def retryWithBackoff[T](
    maxRetries: Int = 3,
    initialDelayMs: Long = 100
  )(block: => T): T = {
    var lastException: Option[Throwable] = None
    var delay = initialDelayMs
    
    for (attempt <- 0 to maxRetries) {
      Try(block) match {
        case Success(result) => return result
        case Failure(e) if isRetryable(e) =>
          if (attempt < maxRetries) {
            lastException = Some(e)
            logWarn(s"Retryable error (attempt ${attempt + 1}/$maxRetries): ${e.getMessage}. Retrying in ${delay}ms...")
            Thread.sleep(delay)
            delay *= 2 // Exponential backoff
          } else {
            logError(s"Max retries ($maxRetries) exceeded. Last error: ${e.getMessage}", e)
            throw e
          }
        case Failure(e) =>
          // Non-retryable error
          logError(s"Non-retryable error: ${e.getMessage}", e)
          throw e
      }
    }
    
    throw lastException.getOrElse(new RuntimeException("Unknown error in retry handler"))
  }
  
  /**
   * Check if an exception is retryable
   */
  private def isRetryable(e: Throwable): Boolean = {
    e match {
      case sqlEx: SQLException =>
        val sqlState = sqlEx.getSQLState
        // Retry on serialization conflicts, deadlocks, connection errors
        sqlState != null && (
          sqlState.startsWith("40") || // Serialization failures
          sqlState.startsWith("08") || // Connection exceptions
          sqlState.startsWith("53")    // Insufficient resources
        )
      case _: java.net.SocketException => true
      case _: java.io.IOException => true
      case NonFatal(_) => true
      case _ => false
    }
  }
}

