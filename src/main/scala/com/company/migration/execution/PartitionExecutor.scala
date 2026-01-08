package com.company.migration.execution

import com.company.migration.config.{TableConfig, YugabyteConfig}
import com.company.migration.transform.RowTransformer
import com.company.migration.util.{Logging, Metrics, ResourceUtils}
import com.company.migration.yugabyte.{CopyStatementBuilder, CopyWriter, YugabyteConnectionFactory}
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
    var rowsWritten = 0L
    var rowsSkipped = 0L
    var lastProcessedPk: Option[String] = None
    
    // Get partition ID from Spark context (use provided partitionId as fallback)
    val actualPartitionId = try {
      TaskContext.getPartitionId()
    } catch {
      case _: Exception => partitionId
    }
    
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
            
            // Extract primary key for checkpointing (simplified - uses first column)
            if (targetColumns.nonEmpty && lastProcessedPk.isEmpty) {
              try {
                lastProcessedPk = Some(row.getAs[String](targetColumns.head))
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
      
      logInfo(s"Partition $actualPartitionId completed: $rowsWritten rows written, $rowsSkipped rows skipped, $rowsCopied rows copied by COPY")
      
      // Mark checkpoint as PASS
      checkpointManager.foreach { cm =>
        val runInfo = s"rows_written=$rowsWritten,rows_skipped=$rowsSkipped,rows_copied=$rowsCopied"
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
      conn.foreach(ResourceUtils.closeConnection)
    }
  }
}
