package com.company.migration.execution

import com.company.migration.config.{CassandraConfig, SparkJobConfig, TableConfig, YugabyteConfig}
import com.company.migration.cassandra.{CassandraReader, CassandraTokenPartitioner}
import com.company.migration.transform.SchemaMapper
import com.company.migration.util.{Logging, Metrics}
import com.company.migration.yugabyte.YugabyteConnectionFactory
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Orchestrates the migration of a single table
 */
class TableMigrationJob(
  spark: SparkSession,
  cassandraConfig: CassandraConfig,
  yugabyteConfig: YugabyteConfig,
  sparkJobConfig: SparkJobConfig,
  tableConfig: TableConfig,
  connectionFactory: YugabyteConnectionFactory,
  metrics: Metrics,
  checkpointManager: Option[CheckpointManager] = None,
  jobId: Option[String] = None,
  checkpointInterval: Int = 10000
) extends Logging {
  
  /**
   * Execute the migration job
   */
  def execute(): Unit = {
    logInfo(s"Starting migration for table: ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable} -> ${tableConfig.targetSchema}.${tableConfig.targetTable}")
    
    try {
      // Step 1: Read from Cassandra
      val reader = new CassandraReader(spark, cassandraConfig)
      val df = reader.readTable(tableConfig)
      
      // Step 2: Get target columns
      val targetColumns = SchemaMapper.getTargetColumns(df.schema, tableConfig)
      logInfo(s"Target columns (${targetColumns.size}): ${targetColumns.mkString(", ")}")
      
      // Step 3: Validate source columns
      SchemaMapper.validateSourceColumns(df, tableConfig)
      
      // Step 4: Log partition info
      CassandraTokenPartitioner.logPartitionInfo(df)
      
      // Step 5: Execute COPY for each partition
      // Extract values needed for partition execution (avoid capturing entire TableMigrationJob)
      // CDM Pattern: Only pass serializable configs, recreate non-serializable objects per partition
      val localTableConfig = tableConfig
      val localYugabyteConfig = yugabyteConfig
      val localTargetColumns = targetColumns
      val localSourceSchema = df.schema
      val localMetrics = metrics
      val localCheckpointEnabled = checkpointManager.isDefined
      val localCheckpointTable = checkpointManager.map(_.checkpointTable).getOrElse("migration_checkpoint")
      val localJobId = jobId
      val localCheckpointInterval = checkpointInterval
      
      df.foreachPartition { (partition: Iterator[Row]) =>
        // Create new connection factory per partition (not serializable)
        val localConnectionFactory = new YugabyteConnectionFactory(localYugabyteConfig)
        // Recreate checkpoint manager per partition if enabled (CDM pattern)
        val localCheckpointManager = if (localCheckpointEnabled) {
          Some(new CheckpointManager(localYugabyteConfig, localCheckpointTable))
        } else {
          None
        }
        val partitionExecutor = new PartitionExecutor(
          localTableConfig,
          localYugabyteConfig,
          localConnectionFactory,
          localTargetColumns,
          localSourceSchema,
          localMetrics,
          localCheckpointManager,
          localJobId,
          localCheckpointInterval
        )
        val _ = partitionExecutor.execute(partition)
      }
      
      logInfo(s"Migration completed for table: ${tableConfig.targetSchema}.${tableConfig.targetTable}")
      
    } catch {
      case e: Exception =>
        logError(s"Migration failed for table ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}: ${e.getMessage}", e)
        throw e
    }
  }
}

