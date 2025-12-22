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
 * Includes checkpointing support based on CDM's TrackRun concept
 */
class PartitionExecutor(
  tableConfig: TableConfig,
  yugabyteConfig: YugabyteConfig,
  connectionFactory: YugabyteConnectionFactory, // Not used - recreated per partition, kept for API compatibility
  targetColumns: List[String],
  sourceSchema: StructType,
  metrics: Metrics,
  checkpointManager: Option[CheckpointManager] = None,
  jobId: Option[String] = None,
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
    
    // Get partition ID for checkpointing
    val partitionId = TaskContext.getPartitionId()
    val tokenStart = 0L // Spark handles token ranges internally
    val tokenEnd = 0L
    
    // Initialize checkpoint if enabled
    checkpointManager.foreach { cm =>
      jobId.foreach { jid =>
        cm.initCheckpoint(jid, partitionId, tokenStart, tokenEnd)
        cm.updateCheckpoint(jid, partitionId, "RUNNING", 0)
      }
    }
    
    try {
      // Recreate connection factory per partition (not serializable)
      val localConnectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
      // Get connection
      conn = Some(localConnectionFactory.getConnection())
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
            
            // Update checkpoint periodically
            if (checkpointManager.isDefined && rowsWritten % checkpointInterval == 0) {
              checkpointManager.foreach { cm =>
                jobId.foreach { jid =>
                  // Extract primary key (simplified - would need actual PK extraction)
                  val pkValue = if (targetColumns.nonEmpty) {
                    try {
                      Some(row.getAs[String](targetColumns.head))
                    } catch {
                      case _: Exception => None
                    }
                  } else None
                  
                  cm.updateCheckpoint(jid, partitionId, "RUNNING", rowsWritten, pkValue)
                }
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
      
      logInfo(s"Partition completed: $rowsWritten rows written, $rowsSkipped rows skipped, $rowsCopied rows copied by COPY")
      
      // Mark checkpoint as DONE
      checkpointManager.foreach { cm =>
        jobId.foreach { jid =>
          cm.updateCheckpoint(jid, partitionId, "DONE", rowsWritten, lastProcessedPk)
        }
      }
      
      metrics.incrementRowsWritten(rowsWritten)
      metrics.incrementPartitionsCompleted()
      
      rowsWritten
      
    } catch {
      case e: Exception =>
        logError(s"Error executing partition: ${e.getMessage}", e)
        
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
        
        // Mark checkpoint as FAILED
        checkpointManager.foreach { cm =>
          jobId.foreach { jid =>
            cm.updateCheckpoint(jid, partitionId, "FAILED", rowsWritten, lastProcessedPk)
          }
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

