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
  
  /**
   * Alternative validation: Compare with actual COUNT (if needed, but may timeout)
   * This method is kept for optional use but is NOT recommended for large tables
   */
  def validateRowCountWithCountQuery(
    spark: org.apache.spark.sql.SparkSession,
    cassandraConfig: com.company.migration.config.CassandraConfig,
    yugabyteConfig: com.company.migration.config.YugabyteConfig,
    tableConfig: TableConfig
  ): (Long, Long, Boolean) = {
    logWarn("Using COUNT queries for validation (may timeout on large tables)")
    logWarn("Consider using validateRowCount() instead which uses migration metrics")
    
    // This will timeout on large distributed tables - use with caution
    val cassandraCount = getCassandraRowCount(spark, cassandraConfig, tableConfig)
    val yugabyteCount = getYugabyteRowCount(yugabyteConfig, tableConfig)
    val matchResult = cassandraCount == yugabyteCount
    
    logWarn(s"Row count validation (COUNT queries): Cassandra=$cassandraCount, Yugabyte=$yugabyteCount, Match=$matchResult")
    
    (cassandraCount, yugabyteCount, matchResult)
  }
  
  private def getCassandraRowCount(
    spark: org.apache.spark.sql.SparkSession,
    cassandraConfig: com.company.migration.config.CassandraConfig,
    tableConfig: TableConfig
  ): Long = {
    try {
      // Use Spark to count (more efficient than direct CQL COUNT)
      val df = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map(
          "keyspace" -> tableConfig.sourceKeyspace,
          "table" -> tableConfig.sourceTable
        ))
        .load()
      
      df.count()
    } catch {
      case e: Exception =>
        logWarn(s"Error getting Cassandra row count (may timeout on large tables): ${e.getMessage}")
        -1L // Return -1 to indicate failure
    }
  }
  
  private def getYugabyteRowCount(
    yugabyteConfig: com.company.migration.config.YugabyteConfig,
    tableConfig: TableConfig
  ): Long = {
    var conn: Option[java.sql.Connection] = None
    try {
      conn = Some(java.sql.DriverManager.getConnection(
        yugabyteConfig.jdbcUrl,
        yugabyteConfig.username,
        yugabyteConfig.password
      ))
      
      val query = s"SELECT COUNT(*) FROM ${tableConfig.targetSchema}.${tableConfig.targetTable}"
      val stmt = conn.get.createStatement()
      val rs = stmt.executeQuery(query)
      
      if (rs.next()) {
        rs.getLong(1)
      } else {
        0L
      }
    } catch {
      case e: Exception =>
        logWarn(s"Error getting Yugabyte row count (may timeout on large tables): ${e.getMessage}")
        -1L // Return -1 to indicate failure
    } finally {
      conn.foreach(_.close())
    }
  }
}

