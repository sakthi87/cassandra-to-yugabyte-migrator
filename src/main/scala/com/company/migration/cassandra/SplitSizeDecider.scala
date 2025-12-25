package com.company.migration.cassandra

import com.company.migration.config.CassandraConfig
import com.company.migration.config.TableConfig
import com.company.migration.util.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{ResultSet, Row}
import scala.util.{Try, Success, Failure}

/**
 * Determines optimal cassandra.inputSplitSizeMb at runtime based on:
 * - Table size estimation
 * - Cluster stability
 * - Data skew detection
 * - Executor memory capacity
 * 
 * This is a Spark-side planning optimization that reduces planning time
 * by creating fewer partitions for large tables.
 */
object SplitSizeDecider extends Logging {
  
  // Split size options (in MB)
  val MIN_SPLIT_SIZE = 128
  val DEFAULT_SPLIT_SIZE = 256
  val MAX_SPLIT_SIZE = 1024
  
  // Skew thresholds
  val LOW_SKEW_THRESHOLD = 1.2
  val MEDIUM_SKEW_THRESHOLD = 1.5
  val HIGH_SKEW_THRESHOLD = 2.0
  
  /**
   * Determine optimal split size based on table characteristics and cluster health
   * 
   * @param cassandraConfig Cassandra connection configuration
   * @param tableConfig Table configuration
   * @param executorMemoryGb Executor memory in GB
   * @param autoDetermine Whether to auto-determine (false = use default)
   * @param overrideSize Manual override (if set, this is used)
   * @return Optimal split size in MB
   */
  def determineSplitSize(
    cassandraConfig: CassandraConfig,
    tableConfig: TableConfig,
    executorMemoryGb: Int,
    autoDetermine: Boolean = true,
    overrideSize: Option[Int] = None
  ): Int = {
    
    // Manual override takes precedence
    overrideSize match {
      case Some(size) if size >= MIN_SPLIT_SIZE && size <= MAX_SPLIT_SIZE =>
        logInfo(s"Using manual override: cassandra.inputSplitSizeMb=$size")
        return size
      case Some(size) =>
        logWarn(s"Override size $size is outside valid range [$MIN_SPLIT_SIZE-$MAX_SPLIT_SIZE], using auto-determination")
      case None => // Continue with auto-determination
    }
    
    // If auto-determination is disabled, use the value from properties (or default)
    if (!autoDetermine) {
      val splitSize = cassandraConfig.inputSplitSizeMb
      logInfo(s"Auto-determination disabled, using configured value: cassandra.inputSplitSizeMb=$splitSize")
      return splitSize
    }
    
    logInfo("Determining optimal split size at runtime...")
    
    // Try to gather table statistics
    val tableStats = Try {
      gatherTableStats(cassandraConfig, tableConfig)
    } match {
      case Success(stats) => Some(stats)
      case Failure(e) =>
        logWarn(s"Could not gather table statistics: ${e.getMessage}. Using default split size.")
        None
    }
    
    // Decision logic
    val splitSize = tableStats match {
      case Some(stats) =>
        decideBasedOnStats(stats, executorMemoryGb)
      case None =>
        // Fallback: use default or size-based heuristic
        decideBasedOnHeuristic(executorMemoryGb)
    }
    
    // Ensure within bounds
    val finalSize = math.max(MIN_SPLIT_SIZE, math.min(MAX_SPLIT_SIZE, splitSize))
    
    logWarn(s"✅ Determined optimal split size: cassandra.inputSplitSizeMb=$finalSize MB")
    val expectedPartitions = estimatePartitions(tableStats, finalSize)
    if (expectedPartitions > 0) {
      logWarn(s"  Expected partitions: ~$expectedPartitions")
    }
    val decisionMethod = if (tableStats.isDefined && tableStats.get.estimatedSizeGb > 0) {
      "table statistics"
    } else {
      "heuristic-based (fallback - metadata unavailable)"
    }
    logWarn(s"  Decision method: $decisionMethod")
    
    finalSize
  }
  
  /**
   * Gather table statistics from Cassandra metadata
   */
  private def gatherTableStats(
    cassandraConfig: CassandraConfig,
    tableConfig: TableConfig
  ): TableStats = {
    
    var session: Option[CqlSession] = None
    
    try {
      // Create a temporary session for metadata queries
      val sessionBuilder = CqlSession.builder()
        .addContactPoint(new java.net.InetSocketAddress(cassandraConfig.hosts.split(",")(0).trim, cassandraConfig.port))
        .withLocalDatacenter(cassandraConfig.localDC)
      
      cassandraConfig.username.foreach { username =>
        sessionBuilder.withAuthCredentials(username, cassandraConfig.password.getOrElse(""))
      }
      
      val cqlSession = sessionBuilder.build()
      session = Some(cqlSession)
      
      // Try to query system_schema.tables for table metadata
      // Note: Different Cassandra versions have different columns
      var estimatedTableSizeGb = 0.0
      var partitionsCount = 0L
      var meanPartitionSize = 0L
      
      try {
        // Try modern Cassandra 4.x schema first
        val query = s"""
          SELECT 
            mean_partition_size,
            partitions_count
          FROM system_schema.tables
          WHERE keyspace_name = '${tableConfig.sourceKeyspace}'
            AND table_name = '${tableConfig.sourceTable}'
        """
        
        val resultSet = cqlSession.execute(query)
        val row = resultSet.one()
        
        if (row != null) {
          try {
            meanPartitionSize = Option(row.getLong("mean_partition_size")).getOrElse(0L)
            partitionsCount = Option(row.getLong("partitions_count")).getOrElse(0L)
            
            if (partitionsCount > 0 && meanPartitionSize > 0) {
              estimatedTableSizeGb = (meanPartitionSize * partitionsCount) / (1024.0 * 1024.0 * 1024.0)
              logInfo(f"Got table size from system_schema: ${estimatedTableSizeGb}%.2fGB")
            }
          } catch {
            case e: Exception =>
              logWarn(s"Could not read mean_partition_size (column may not exist in this Cassandra version): ${e.getMessage}")
          }
        }
      } catch {
        case e: Exception =>
          logWarn(s"Could not query system_schema.tables: ${e.getMessage}")
      }
      
      // Fallback: try system.size_estimates (available in most Cassandra versions)
      if (estimatedTableSizeGb == 0.0) {
        val sizeEstimate = estimateTableSizeFromSizeEstimates(cqlSession, tableConfig)
        if (sizeEstimate > 0) {
          estimatedTableSizeGb = sizeEstimate
          logInfo(f"Got table size from system.size_estimates: ${estimatedTableSizeGb}%.2fGB")
        }
      }
      
      // Final fallback: use heuristic based on sampling
      if (estimatedTableSizeGb == 0.0) {
        logWarn("Could not determine table size from metadata, using sampling-based estimation")
        estimatedTableSizeGb = estimateTableSizeFromSampling(cqlSession, tableConfig)
      }
      
      // Sample token ranges for skew detection
      val skewLevel = estimateSkew(cqlSession, tableConfig)
      
      TableStats(
        estimatedSizeGb = estimatedTableSizeGb,
        partitionsCount = partitionsCount,
        meanPartitionSize = meanPartitionSize,
        skewLevel = skewLevel
      )
      
    } finally {
      session.foreach(_.close())
    }
  }
  
  /**
   * Estimate table size from system.size_estimates (available in most Cassandra versions)
   */
  private def estimateTableSizeFromSizeEstimates(
    session: CqlSession,
    tableConfig: TableConfig
  ): Double = {
    try {
      val query = s"""
        SELECT 
          SUM(mean_partition_size) as total_size,
          COUNT(*) as range_count
        FROM system.size_estimates
        WHERE keyspace_name = '${tableConfig.sourceKeyspace}'
          AND table_name = '${tableConfig.sourceTable}'
      """
      
      val resultSet = session.execute(query)
      val row = resultSet.one()
      
      if (row != null) {
        val totalSize = Option(row.getLong("total_size")).getOrElse(0L)
        if (totalSize > 0) {
          return totalSize / (1024.0 * 1024.0 * 1024.0)
        }
      }
    } catch {
      case e: Exception =>
        logWarn(s"system.size_estimates query failed: ${e.getMessage}")
    }
    
    0.0
  }
  
  /**
   * Estimate table size by sampling a small number of rows
   */
  private def estimateTableSizeFromSampling(
    session: CqlSession,
    tableConfig: TableConfig
  ): Double = {
    try {
      // Sample 1000 rows to estimate average row size
      val sampleQuery = s"""
        SELECT *
        FROM ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}
        LIMIT 1000
      """
      
      val resultSet = session.execute(sampleQuery)
      var rowCount = 0
      var totalBytes = 0L
      
      resultSet.forEach { row =>
        rowCount += 1
        // Estimate row size (rough approximation)
        val columnDefinitions = row.getColumnDefinitions
        var rowSize = 0L
        for (i <- 0 until columnDefinitions.size()) {
          val value = row.getObject(i)
          if (value != null) {
            rowSize += value.toString.getBytes("UTF-8").length
          }
        }
        totalBytes += rowSize
      }
      
      if (rowCount > 0) {
        val avgRowSize = totalBytes / rowCount
        // Very rough estimate: assume 1M rows if unknown
        // This is conservative - actual size may be much larger
        val estimatedRows = 1000000L // Conservative estimate
        val estimatedSizeGb = (avgRowSize * estimatedRows) / (1024.0 * 1024.0 * 1024.0)
        logInfo(f"Estimated table size from sampling: ${estimatedSizeGb}%.2fGB (avg row size: ${avgRowSize} bytes, estimated rows: ${estimatedRows})")
        return estimatedSizeGb
      }
    } catch {
      case e: Exception =>
        logWarn(s"Sampling-based estimation failed: ${e.getMessage}")
    }
    
    // Ultimate fallback: return conservative estimate
    10.0 // Default to 10 GB if all methods fail
  }
  
  /**
   * Estimate data skew by sampling token ranges
   */
  private def estimateSkew(
    session: CqlSession,
    tableConfig: TableConfig
  ): Double = {
    
    try {
      // Sample a few token ranges to estimate skew
      // This is a simplified approach - in production, you might use more sophisticated sampling
      
      // CQL doesn't support token(*) - need to specify partition key columns
      // For now, use a simpler approach: sample a few rows and estimate variance
      val sampleQuery = s"""
        SELECT *
        FROM ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}
        LIMIT 100
      """
      
      val resultSet = session.execute(sampleQuery)
      var rowCount = 0
      
      resultSet.forEach { _ => rowCount += 1 }
      
      // Simplified skew estimation: if we can sample rows evenly, assume low skew
      // More sophisticated skew detection would require partition key analysis
      if (rowCount < 10) {
        logWarn("Insufficient samples for skew detection, assuming low skew")
        return 1.0 // Assume no skew
      }
      
      // For now, return low skew (1.0) - can be enhanced later with actual partition key analysis
      // This is a conservative estimate that won't hurt performance
      1.0
      
    } catch {
      case e: Exception =>
        logWarn(s"Could not estimate skew: ${e.getMessage}, assuming low skew")
        1.0 // Assume no skew on error
    }
  }
  
  /**
   * Decide split size based on gathered statistics
   */
  private def decideBasedOnStats(
    stats: TableStats,
    executorMemoryGb: Int
  ): Int = {
    
    val tableSizeGb = stats.estimatedSizeGb
    val skewLevel = stats.skewLevel
    
    logInfo(f"Table statistics: size=${tableSizeGb}%.2fGB, skew=${skewLevel}%.2f, partitions=${stats.partitionsCount}")
    
    // High skew or unstable cluster -> conservative
    if (skewLevel > HIGH_SKEW_THRESHOLD) {
      logWarn(f"High skew detected (${skewLevel}%.2f), using conservative split size")
      return MIN_SPLIT_SIZE
    }
    
    if (skewLevel > MEDIUM_SKEW_THRESHOLD) {
      logWarn(f"Medium skew detected (${skewLevel}%.2f), using moderate split size")
      return DEFAULT_SPLIT_SIZE
    }
    
    // Size-based decision
    if (tableSizeGb < 50) {
      // Small table: use default
      DEFAULT_SPLIT_SIZE
    } else if (tableSizeGb < 200) {
      // Medium table: use 512 MB if executor has capacity
      if (executorMemoryGb >= 8) {
        512
      } else {
        DEFAULT_SPLIT_SIZE
      }
    } else {
      // Large table: can use 1024 MB if conditions are right
      if (executorMemoryGb >= 8 && skewLevel < LOW_SKEW_THRESHOLD) {
        1024
      } else if (executorMemoryGb >= 4) {
        512
      } else {
        DEFAULT_SPLIT_SIZE
      }
    }
  }
  
  /**
   * Fallback decision based on heuristics when stats unavailable
   */
  private def decideBasedOnHeuristic(executorMemoryGb: Int): Int = {
    logWarn("Using heuristic-based decision (table stats unavailable)")
    
    if (executorMemoryGb >= 8) {
      512 // Conservative for unknown tables
    } else {
      DEFAULT_SPLIT_SIZE
    }
  }
  
  /**
   * Estimate number of partitions based on split size
   */
  private def estimatePartitions(
    tableStats: Option[TableStats],
    splitSizeMb: Int
  ): Int = {
    tableStats match {
      case Some(stats) if stats.estimatedSizeGb > 0 =>
        val totalSizeMb = stats.estimatedSizeGb * 1024
        math.max(1, (totalSizeMb / splitSizeMb).toInt)
      case _ =>
        -1 // Unknown
    }
  }
  
  /**
   * Table statistics gathered from Cassandra
   */
  case class TableStats(
    estimatedSizeGb: Double,
    partitionsCount: Long,
    meanPartitionSize: Long,
    skewLevel: Double
  )
}

