package com.company.migration.execution

import com.company.migration.config.{TableConfig, YugabyteConfig}
import com.company.migration.transform.RowTransformer
import com.company.migration.util.{Logging, Metrics, ResourceUtils}
import com.company.migration.yugabyte.{CopyStatementBuilder, CopyWriter, InsertBatchWriter, UpsertStatementBuilder, PrimaryKeyDiscovery, YugabyteConnectionFactory}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.TaskContext

/**
 * Executes COPY operation for a single Spark partition
 * This is where the actual data migration happens
 * Enhanced with robust checkpointing (run_info + run_details)
 */
class PartitionExecutor(
  tableConfig: TableConfig,
  yugabyteConfig: YugabyteConfig,
  connectionFactory: YugabyteConnectionFactory, // Not used - recreated per partition, kept for API compatibility
  targetColumns: List[String],
  sourceSchema: StructType,
  metrics: Metrics,
  checkpointManager: Option[CheckpointManager] = None,
  runId: Long,
  tableName: String,
  partitionId: Int,
  checkpointInterval: Int = 10000
) extends Logging with Serializable {
  
  // Mark non-serializable fields as transient - they'll be recreated per partition
  @transient private lazy val rowTransformer = new RowTransformer(tableConfig, targetColumns, sourceSchema)
  
  /**
   * Execute COPY for a partition
   * @param rows Iterator of rows from the partition
   * @return Number of rows written
   */
  def execute(rows: Iterator[Row]): Long = {
    var conn: Option[java.sql.Connection] = None
    var copyWriter: Option[CopyWriter] = None
    var insertWriter: Option[InsertBatchWriter] = None
    var rowsWritten = 0L
    var rowsSkipped = 0L
    var lastProcessedPk: Option[String] = None
    val insertMode = yugabyteConfig.insertMode.toUpperCase
    
    // Track sample primary keys for duplicate detection logging
    val samplePks = scala.collection.mutable.Set[String]()
    val maxSampleSize = 100  // Sample first 100 primary keys per partition
    
    // Get partition ID from Spark context (use provided partitionId as fallback)
    val actualPartitionId = try {
      TaskContext.getPartitionId()
    } catch {
      case _: Exception => partitionId
    }
    
    logInfo(s"Partition $actualPartitionId: Starting execution")
    
    // Token range tracking (simplified - Spark handles token ranges internally)
    // In production, you'd extract actual token ranges from Spark partition metadata
    // For now, we use partition_id as the identifier
    val tokenMin = actualPartitionId.toLong // Use partition ID as token identifier
    val tokenMax = actualPartitionId.toLong
    
    // Update checkpoint status to STARTED
    checkpointManager.foreach { cm =>
      cm.updateRun(
        tableName = tableName,
        runId = runId,
        tokenMin = tokenMin,
        partitionId = actualPartitionId,
        status = CheckpointManager.RunStatus.STARTED,
        runInfo = None
      )
    }
    
    try {
      // Recreate connection factory per partition (not serializable)
      val localConnectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
      // Get connection using partition ID for round-robin load balancing
      conn = Some(localConnectionFactory.getConnection(actualPartitionId))
      val connection = conn.get
      
      // Set YugabyteDB session parameters for performance optimization
      if (yugabyteConfig.disableTransactionalWrites) {
        val stmt = connection.createStatement()
        try {
          stmt.execute("SET yb_disable_transactional_writes = on;")
          logInfo(s"Partition $actualPartitionId: Enabled yb_disable_transactional_writes for performance")
        } finally {
          stmt.close()
        }
      }
      
      if (insertMode == "INSERT") {
        // INSERT mode: Use batched INSERT ... ON CONFLICT DO NOTHING
        // Discover primary key columns
        val primaryKeyColumns = PrimaryKeyDiscovery.getPrimaryKeyColumns(
          connection,
          tableConfig,
          targetColumns
        )
        
        // Build INSERT statement
        val insertSql = UpsertStatementBuilder.buildUpsertStatement(
          tableConfig,
          targetColumns,
          primaryKeyColumns
        )
        
        // Create INSERT batch writer
        insertWriter = Some(new InsertBatchWriter(
          connection,
          insertSql,
          yugabyteConfig.insertBatchSize
        ))
        val writer = insertWriter.get
        
        // Start INSERT batch writer
        writer.start()
        
        // Process rows
        rows.foreach { row =>
          rowTransformer.toValues(row) match {
            case Some(rowValues) =>
              writer.addRow(rowValues)
              rowsWritten += 1
              metrics.incrementRowsRead()
              
              // Extract primary key for logging and duplicate detection
              if (targetColumns.nonEmpty) {
                try {
                  // Build composite primary key string from all primary key columns
                  val pkStr = if (tableConfig.primaryKey.nonEmpty) {
                    // Use configured primary key columns
                    tableConfig.primaryKey.map { pkCol =>
                      try {
                        val idx = sourceSchema.fieldIndex(pkCol)
                        val value = if (row.isNullAt(idx)) "NULL" else row.get(idx).toString
                        s"$pkCol=$value"
                      } catch {
                        case _: Exception => ""
                      }
                    }.filter(_.nonEmpty).mkString("|")
                  } else {
                    // Fallback: use first column
                    val value = if (row.isNullAt(0)) "NULL" else row.get(0).toString
                    s"${targetColumns.head}=$value"
                  }
                  
                  if (lastProcessedPk.isEmpty) {
                    lastProcessedPk = Some(pkStr)
                  }
                  
                  // Sample primary keys for duplicate detection (first 100)
                  if (samplePks.size < maxSampleSize && pkStr.nonEmpty) {
                    samplePks += pkStr
                  }
                } catch {
                  case _: Exception => // Ignore if can't extract PK
                }
              }
              
              // Update checkpoint periodically
              if (checkpointManager.isDefined && rowsWritten % checkpointInterval == 0) {
                checkpointManager.foreach { cm =>
                  val runInfo = s"rows_written=$rowsWritten,rows_skipped=$rowsSkipped"
                  cm.updateRun(
                    tableName = tableName,
                    runId = runId,
                    tokenMin = tokenMin,
                    partitionId = actualPartitionId,
                    status = CheckpointManager.RunStatus.STARTED, // Still running
                    runInfo = Some(runInfo)
                  )
                }
              }
            case None =>
              rowsSkipped += 1
              metrics.incrementRowsSkipped()
          }
        }
        
        // End INSERT batch and commit
        val rowsInserted = writer.endBatch()
        rowsSkipped += writer.getRowsSkipped  // Add duplicate rows skipped
        connection.commit()
        
        logInfo(s"Partition $actualPartitionId completed (INSERT mode): $rowsWritten rows processed, $rowsInserted rows inserted, ${writer.getRowsSkipped} duplicates skipped")
        logInfo(s"Partition $actualPartitionId sample PKs (first ${math.min(samplePks.size, 10)}): ${samplePks.take(10).mkString(", ")}")
      } else {
        // COPY mode: Use COPY FROM STDIN (default)
        // Build COPY statement
        val copySql = CopyStatementBuilder.buildCopyStatement(
          tableConfig,
          targetColumns,
          yugabyteConfig
        )
        
        // Create COPY writer
        copyWriter = Some(new CopyWriter(
          connection,
          copySql,
          yugabyteConfig.copyFlushEvery
        ))
        val writer = copyWriter.get
        
        // Start COPY operation
        writer.start()
        
        // Process rows
        rows.foreach { row =>
          rowTransformer.toCsv(row) match {
            case Some(csvRow) =>
              writer.writeRow(csvRow)
              rowsWritten += 1
              metrics.incrementRowsRead()
              
              // Extract primary key for logging and duplicate detection
              if (targetColumns.nonEmpty) {
                try {
                  // Build composite primary key string from all primary key columns
                  val pkStr = if (tableConfig.primaryKey.nonEmpty) {
                    // Use configured primary key columns
                    tableConfig.primaryKey.map { pkCol =>
                      try {
                        val idx = sourceSchema.fieldIndex(pkCol)
                        val value = if (row.isNullAt(idx)) "NULL" else row.get(idx).toString
                        s"$pkCol=$value"
                      } catch {
                        case _: Exception => ""
                      }
                    }.filter(_.nonEmpty).mkString("|")
                  } else {
                    // Fallback: use first column
                    val value = if (row.isNullAt(0)) "NULL" else row.get(0).toString
                    s"${targetColumns.head}=$value"
                  }
                  
                  if (lastProcessedPk.isEmpty) {
                    lastProcessedPk = Some(pkStr)
                  }
                  
                  // Sample primary keys for duplicate detection (first 100)
                  if (samplePks.size < maxSampleSize && pkStr.nonEmpty) {
                    samplePks += pkStr
                  }
                } catch {
                  case _: Exception => // Ignore if can't extract PK
                }
              }
              
              // Update checkpoint periodically
              if (checkpointManager.isDefined && rowsWritten % checkpointInterval == 0) {
                checkpointManager.foreach { cm =>
                  val runInfo = s"rows_written=$rowsWritten,rows_skipped=$rowsSkipped"
                  cm.updateRun(
                    tableName = tableName,
                    runId = runId,
                    tokenMin = tokenMin,
                    partitionId = actualPartitionId,
                    status = CheckpointManager.RunStatus.STARTED, // Still running
                    runInfo = Some(runInfo)
                  )
                }
              }
            case None =>
              rowsSkipped += 1
              metrics.incrementRowsSkipped()
          }
        }
        
        // End COPY and commit
        val rowsCopied = writer.endCopy()
        connection.commit()
        
        logInfo(s"Partition $actualPartitionId completed (COPY mode): $rowsWritten rows written, $rowsSkipped rows skipped, $rowsCopied rows copied by COPY")
        logInfo(s"Partition $actualPartitionId sample PKs (first ${math.min(samplePks.size, 10)}): ${samplePks.take(10).mkString(", ")}")
      }
      
      // Mark checkpoint as PASS
      checkpointManager.foreach { cm =>
        val runInfo = s"rows_written=$rowsWritten,rows_skipped=$rowsSkipped,mode=$insertMode"
        cm.updateRun(
          tableName = tableName,
          runId = runId,
          tokenMin = tokenMin,
          partitionId = actualPartitionId,
          status = CheckpointManager.RunStatus.PASS,
          runInfo = Some(runInfo)
        )
      }
      
      metrics.incrementRowsWritten(rowsWritten)
      metrics.incrementPartitionsCompleted()
      
      rowsWritten
      
    } catch {
      case e: Exception =>
        logError(s"Error executing partition $actualPartitionId: ${e.getMessage}", e)
        
        // Rollback and cleanup
        conn.foreach { c =>
          try {
            c.rollback()
          } catch {
            case rollbackEx: Exception =>
              logError(s"Error during rollback: ${rollbackEx.getMessage}", rollbackEx)
          }
        }
        
        copyWriter.foreach(_.cancelCopy())
        insertWriter.foreach(_.cancel())
        
        // Mark checkpoint as FAIL
        checkpointManager.foreach { cm =>
          val runInfo = s"error=${e.getMessage},rows_written=$rowsWritten,rows_skipped=$rowsSkipped"
          cm.updateRun(
            tableName = tableName,
            runId = runId,
            tokenMin = tokenMin,
            partitionId = actualPartitionId,
            status = CheckpointManager.RunStatus.FAIL,
            runInfo = Some(runInfo)
          )
        }
        
        metrics.incrementPartitionsFailed()
        throw new RuntimeException(s"Partition execution failed: ${e.getMessage}", e)
        
    } finally {
      // Close resources
      copyWriter.foreach { writer =>
        if (writer.isActive) {
          writer.cancelCopy()
        }
      }
      insertWriter.foreach { writer =>
        if (writer.isActive) {
          writer.cancel()
        }
      }
      conn.foreach(ResourceUtils.closeConnection)
    }
  }
}
