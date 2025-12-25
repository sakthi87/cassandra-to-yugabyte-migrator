package com.company.migration.validation

import com.company.migration.config.TableConfig
import com.company.migration.util.{Logging, Metrics}

/**
 * Validates row counts using migration metrics (no COUNT queries)
 * 
 * CDM Pattern: Uses counters from migration instead of COUNT(*) queries
 * This avoids timeouts on distributed databases (Cassandra/YugabyteDB)
 * 
 * The migration already tracks:
 * - Rows Read from Cassandra (via Metrics.rowsRead)
 * - Rows Written to YugabyteDB (via Metrics.rowsWritten)
 * 
 * These counters are accumulated during migration, so no COUNT query needed.
 */
class RowCountValidator(
  metrics: Metrics
) extends Logging {
  
  /**
   * Validate row counts using migration metrics
   * No COUNT queries - uses counters accumulated during migration
   * 
   * @return (rowsRead, rowsWritten, match)
   */
  def validateRowCount(tableConfig: TableConfig): (Long, Long, Boolean) = {
    logWarn(s"Validating row counts for ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable} using migration metrics")
    
    // Get counts from migration metrics (already accumulated during migration)
    val rowsRead = metrics.getRowsRead
    val rowsWritten = metrics.getRowsWritten
    val rowsSkipped = metrics.getRowsSkipped
    
    // Validation: rows written should equal rows read minus skipped
    // (assuming no errors that weren't tracked)
    val expectedWritten = rowsRead - rowsSkipped
    val matchResult = rowsWritten == expectedWritten
    
    logWarn(s"Row count validation (from metrics): Read=$rowsRead, Written=$rowsWritten, Skipped=$rowsSkipped, Expected=$expectedWritten, Match=$matchResult")
    
    if (!matchResult) {
      val diff = rowsWritten - expectedWritten
      logWarn(s"Row count mismatch: Difference=$diff (Written - Expected)")
      if (diff > 0) {
        logWarn(s"  More rows written than expected - possible duplicates or retries")
      } else {
        logWarn(s"  Fewer rows written than expected - possible write failures")
      }
    }
    
    (rowsRead, rowsWritten, matchResult)
  }
}

