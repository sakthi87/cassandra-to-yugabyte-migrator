package com.company.migration.util

import org.slf4j.{Logger, LoggerFactory}

/**
 * Unified logging utility
 */
trait Logging {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  
  protected def logInfo(msg: => String): Unit = {
    if (logger.isInfoEnabled) logger.info(msg)
  }
  
  protected def logError(msg: => String, throwable: Throwable = null): Unit = {
    if (logger.isErrorEnabled) {
      if (throwable != null) {
        logger.error(msg, throwable)
      } else {
        logger.error(msg)
      }
    }
  }
  
  protected def logWarn(msg: => String): Unit = {
    if (logger.isWarnEnabled) logger.warn(msg)
  }
  
  protected def logDebug(msg: => String): Unit = {
    if (logger.isDebugEnabled) logger.debug(msg)
  }
}

